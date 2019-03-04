package com.ph14.fdb.zk.session;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.server.SessionTracker;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

public class FdbSessionTracker implements SessionTracker {

  private static final Logger LOG = LoggerFactory.getLogger(FdbSessionTracker.class);

  private static final List<String> SESSION_DIRECTORY = Collections.singletonList("fdb-zk-sessions");

  private static final HashedWheelTimer hwt = new HashedWheelTimer();

  private final Database fdb;
  private final SessionExpirer sessionExpirer;
  private final byte[] versionstampKey;
  private final DirectorySubspace sessionDirectory;

  public FdbSessionTracker(Database fdb, SessionExpirer sessionExpirer) {
    this.fdb = fdb;
    this.sessionExpirer = sessionExpirer;
    this.sessionDirectory = fdb.run(tr -> DirectoryLayer.getDefault().createOrOpen(tr, SESSION_DIRECTORY).join());
    this.versionstampKey = sessionDirectory.packWithVersionstamp(Tuple.from(Versionstamp.incomplete()));
  }

  @Override
  public long createSession(int sessionTimeout) {
    return fdb.run(tr -> {
      tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, versionstampKey, Ints.toByteArray(sessionTimeout));
      return tr.getVersionstamp();
    })
        .thenApply(Versionstamp::complete)
        .thenApply(Versionstamp::getTransactionVersion)
        .thenApply(Longs::fromByteArray)
        .join();
  }

  @Override
  public void addSession(long id, int to) {
    fdb.run(tr -> {
      tr.set(sessionDirectory.pack(Longs.toByteArray(id)), Ints.toByteArray(to));
      return null;
    });
  }

  @Override
  public boolean touchSession(long sessionId, int sessionTimeout) {
    LOG.info("Asked to touch: {}", sessionId);
    byte[] value = fdb.read(tr -> tr.get(toVersionstampKey(sessionId))).join();

    LOG.info("Touch value: {}", value);
    return Ints.fromByteArray(value) > 0;
  }

  @Override
  public void setSessionClosing(long sessionId) {
    // this can be stored in a key or as part of a serialized value
  }

  @Override
  public void shutdown() {

  }

  @Override
  public void removeSession(long sessionId) {
    fdb.run(tr -> {
      tr.clear(toVersionstampKey(sessionId));
      return null;
    });
  }

  // do we need owners at all? guid for zkserver at start time?

  @Override
  public void checkSession(long sessionId, Object owner) throws SessionExpiredException, SessionMovedException {

  }

  @Override
  public void setOwner(long id, Object owner) throws SessionExpiredException {

  }

  @Override
  public void dumpSessions(PrintWriter pwriter) {

  }

  private byte[] toVersionstampKey(long sessionId) {
    byte[] versionstampBytes = Bytes.concat(Longs.toByteArray(sessionId), Shorts.toByteArray((short) 0));
    return sessionDirectory.pack(Tuple.from(Versionstamp.complete(versionstampBytes)));
  }

}
