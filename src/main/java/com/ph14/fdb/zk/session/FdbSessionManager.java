package com.ph14.fdb.zk.session;

import java.io.PrintWriter;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.server.SessionTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class FdbSessionManager implements SessionTracker {

  private static final Logger LOG = LoggerFactory.getLogger(FdbSessionManager.class);

  private static final byte[] EMPTY_VALUE = new byte[0];

  static final List<String> SESSION_DIRECTORY = Collections.singletonList("fdb-zk-sessions-by-id");
  static final List<String> SESSION_TIMEOUT_DIRECTORY = Collections.singletonList("fdb-zk-sessions-by-timeout");

  private final Database fdb;
  private final byte[] incompleteSessionsByIdKey;
  private final DirectorySubspace sessionsById;
  private final DirectorySubspace sessionsByTimeout;
  private final Clock clock;
  private final long serverTickMillis;

  @Inject
  public FdbSessionManager(Database fdb,
                           Clock clock,
                           @Named("serverTickMillis") OptionalLong serverTickMillis) {
    this.fdb = fdb;
    this.sessionsById = fdb.run(tr -> DirectoryLayer.getDefault().createOrOpen(tr, SESSION_DIRECTORY).join());
    this.sessionsByTimeout = fdb.run(tr -> DirectoryLayer.getDefault().createOrOpen(tr, SESSION_TIMEOUT_DIRECTORY).join());
    this.incompleteSessionsByIdKey = sessionsById.packWithVersionstamp(Tuple.from(Versionstamp.incomplete()));
    this.clock = clock;
    this.serverTickMillis = serverTickMillis.orElse(500L);
  }

  @Override
  public long createSession(int sessionTimeout) {
    return fdb.run(tr -> {
      long sessionExpirationTimestamp = calculateSessionExpirationTimestamp(clock.millis(), sessionTimeout);

      // magic incantation for `FIRST_IN_BATCH` transaction option.
      // forces this transaction to get batch id == 0, so any competing
      // sessions are guaranteed to be unique by the transaction version
      // component of versionstamps alone
      tr.options().getOptionConsumer().setOption(710, null);

      // session id --> (next expiration, timeoutMs)
      tr.mutate(
          MutationType.SET_VERSIONSTAMPED_KEY,
          incompleteSessionsByIdKey,
          getSessionByIdValue(sessionExpirationTimestamp, sessionTimeout));

      // next expiration --> session ids
      tr.mutate(
          MutationType.SET_VERSIONSTAMPED_KEY,
          getIncompleteSessionByTimestampKey(sessionExpirationTimestamp),
          EMPTY_VALUE);

      return tr.getVersionstamp();
    })
        .thenApply(Versionstamp::complete)
        .thenApply(Versionstamp::getTransactionVersion)
        .thenApply(Longs::fromByteArray)
        .join();
  }

  @Override
  public void addSession(long sessionId, int sessionTimeout) {
    long sessionExpirationTimestamp = calculateSessionExpirationTimestamp(clock.millis(), sessionTimeout);

    fdb.run(tr -> {
      byte[] sessionByIdKey = getSessionByIdKey(sessionId);

      tr.get(sessionByIdKey).thenAccept(currentSession -> {
        if (currentSession == null) {
          // session id --> next expiration
          tr.set(
              sessionByIdKey,
              getSessionByIdValue(sessionExpirationTimestamp, sessionTimeout));

          // next expiration --> session ids
          tr.set(
              getSessionByTimestampKey(sessionId, sessionExpirationTimestamp),
              EMPTY_VALUE);
        }
      }).join();

      return null;
    });

    touchSession(sessionId, sessionTimeout);
  }

  @Override
  public boolean touchSession(long sessionId, int sessionTimeout) {
    byte[] value = fdb.read(rt -> rt.get(getSessionByIdKey(sessionId))).join();

    if (value == null) {
      return false;
    }

    Tuple tuple = Tuple.fromBytes(value);
    long persistedNextSessionExpirationTimestamp = tuple.getLong(0);

    long newlyComputedNextExpirationTimestamp = calculateSessionExpirationTimestamp(clock.millis(), sessionTimeout);

    if (persistedNextSessionExpirationTimestamp >= newlyComputedNextExpirationTimestamp) {
      // this session already got a higher expiration timestamp from a previous touch with a greater `sessionTimeout`
      return true;
    }

    return fdb.run(tr -> {
      // update existing session --> timestamp entry
      tr.set(
          getSessionByIdKey(sessionId),
          getSessionByIdValue(newlyComputedNextExpirationTimestamp, sessionTimeout)
      );

      // update timestamp --> session
      tr.clear(getSessionByTimestampKey(sessionId, persistedNextSessionExpirationTimestamp));
      tr.set(
          getSessionByTimestampKey(sessionId, newlyComputedNextExpirationTimestamp),
          EMPTY_VALUE
      );

      return true;
    });
  }

  @Override
  public void setSessionClosing(long sessionId) {
    removeSession(sessionId);
  }

  @Override
  public void removeSession(long sessionId) {
    fdb.run(tr -> removeSessionAsync(tr, sessionId)).join();
  }

  public CompletableFuture<Void> removeSessionAsync(Transaction transaction, long sessionId) {
    byte[] sessionByIdKey = getSessionByIdKey(sessionId);

    return transaction.get(sessionByIdKey).thenAccept(currentSession -> {
      if (currentSession != null) {
        transaction.clear(sessionByIdKey);
        long sessionExpirationTimestamp = Tuple.fromBytes(currentSession).getLong(0);

        // next expiration --> session ids
        transaction.clear(getSessionByTimestampKey(sessionId, sessionExpirationTimestamp));
      }
    });
  }

  @Override
  public void shutdown() {
    // N/A -- state stored in FDB, nothing to clean up here
  }

  @Override
  public void checkSession(long sessionId, Object owner) throws SessionExpiredException, SessionMovedException {
    byte[] value = fdb.read(rt -> rt.get(getSessionByIdKey(sessionId)).join());

    if (value == null) {
      throw new SessionExpiredException();
    }

    Tuple tuple = Tuple.fromBytes(value);
    long sessionExpirationTimestamp = tuple.getLong(0);

    if (sessionExpirationTimestamp <= clock.millis()) {
      throw new SessionExpiredException();
    }
  }

  @VisibleForTesting
  Tuple getSessionDataById(long sessionId) {
    return Tuple.fromBytes(fdb.read(rt -> rt.get(getSessionByIdKey(sessionId)).join()));
  }

  public List<Long> getExpiredSessionIds(long greatestExpiredSessionTimestamp) {
    List<KeyValue> keyValues = fdb.read(rt -> rt.getRange(
        new Range(
            getSessionByTimestampKey(0, 0),
            getSessionByTimestampKey(Long.MAX_VALUE, greatestExpiredSessionTimestamp)))
        .asList()
        .join()
    );

    return keyValues.stream()
        .map(kv -> Tuple.fromBytes(kv.getKey()).getVersionstamp(2).getTransactionVersion())
        .map(Longs::fromByteArray)
        .collect(Collectors.toList());
  }

  @Override
  public void setOwner(long id, Object owner) {
    // not needed, state is stored in fdb
  }

  @Override
  public void dumpSessions(PrintWriter pwriter) {
  }

  private byte[] getSessionByIdKey(long sessionId) {
    byte[] versionstampBytes = Bytes.concat(Longs.toByteArray(sessionId), Shorts.toByteArray((short) 0));
    return sessionsById.pack(Tuple.from(Versionstamp.complete(versionstampBytes)));
  }

  private byte[] getSessionByIdValue(long sessionExpirationTimestamp, long sessionTimeoutMillis) {
    return Tuple.from(sessionExpirationTimestamp, sessionTimeoutMillis).pack();
  }

  private byte[] getSessionByTimestampKey(long sessionId, long sessionExpirationTimestamp) {
    byte[] versionstampBytes = Bytes.concat(Longs.toByteArray(sessionId), Shorts.toByteArray((short) 0));
    return sessionsByTimeout.pack(Tuple.from(sessionExpirationTimestamp, Versionstamp.complete(versionstampBytes)));
  }

  private byte[] getIncompleteSessionByTimestampKey(long sessionExpirationTimestamp) {
    return sessionsByTimeout.packWithVersionstamp(Tuple.from(sessionExpirationTimestamp, Versionstamp.incomplete()));
  }

  private long calculateSessionExpirationTimestamp(long now, int timeoutMillis) {
    // We give a one interval grace period
    return ((now + timeoutMillis) / serverTickMillis + 1) * serverTickMillis;
  }

}
