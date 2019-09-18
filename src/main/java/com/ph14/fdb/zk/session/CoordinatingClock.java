package com.ph14.fdb.zk.session;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Coordinating clock to pick a client to execute some function when elected.
 *
 * Each clock participant reads the clock key, which is a timestamp
 * of the end of the current tick. Clients wait until their local time ~= this value,
 * and then try to write ts + tick_interval. If all of their transactions were
 * open simultaneously, then only one write should win and the rest get conflicts.
 * The winning write is the "leader" until the next tick interval.
 *
 * This doesn't guarantee single-leaders, but ought to cull most clients each tick.
 */
public class CoordinatingClock implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatingClock.class);

  // this should be configurable
  private static final long TICK_TIME_MILLIS = 1000;

  private final Database fdb;
  private final ExecutorService executorService;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final String clockName;
  private final byte[] clockKey;
  private final Runnable onElection;

  private CoordinatingClock(Database fdb, String clockName, Runnable onElection) {
    this.fdb = fdb;

    this.executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("fdb-clock-" + clockName)
        .setDaemon(true)
        .build());

    this.clockName = clockName;
    this.clockKey = clockName.getBytes(Charsets.UTF_8);
    this.onElection = onElection;

    executorService.submit(() -> {
      while (!isClosed.get()) {
        keepTime();
      }
    });
  }

  public static CoordinatingClock start(Database fdb, String clockName, Runnable onElection) {
    return new CoordinatingClock(fdb, clockName, onElection);
  }

  @Override
  public void close() {
    isClosed.set(true);
    executorService.shutdown();

    try {
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void keepTime() {
    try {
      Transaction transaction = fdb.createTransaction();

      byte[] currentTickToWaitFor = transaction.get(clockKey).join();

      long now = System.currentTimeMillis();
      long nowRounded = TICK_TIME_MILLIS * (now / TICK_TIME_MILLIS);

      if (currentTickToWaitFor == null) {
        transaction.set(clockKey, ByteArrayUtil.encodeInt(nowRounded + TICK_TIME_MILLIS));
        transaction.commit().join();
        return;
      }

      long nextPersistedTick = ByteArrayUtil.decodeInt(currentTickToWaitFor) + TICK_TIME_MILLIS;

      boolean mustRetryBeforeEligibleForLeader = false;
      // our clock is ahead of the next tick, we'll try to advance it further
      if (now > nextPersistedTick) {
        nextPersistedTick += TICK_TIME_MILLIS;
        mustRetryBeforeEligibleForLeader = true;
      } else {
        // we're behind, so wait until we think our real-time is the next tick
        Thread.sleep(nextPersistedTick - now);
      }

      transaction.set(clockKey, ByteArrayUtil.encodeInt(nextPersistedTick));
      transaction.commit().join();

      if (mustRetryBeforeEligibleForLeader) {
        return;
      }
    } catch (Exception e) {
      for (Throwable throwable : Throwables.getCausalChain(e)) {
        if (throwable instanceof FDBException) {
          FDBException fdbException = (FDBException) throwable;
          if (fdbException.getCode() == 1020) {
            // this is a conflict exception, which is expected for roughly all but one client per tick
            return;
          }
        }
      }

      // we hit a real exception. we might want to apply backpressure here.
      // it's risky to entirely drop the loop, since a transient error that
      // impacts all clients could stop all ticks
      LOG.error("Exception hit in leader clock", e);
      return;
    }

    // we won! by which we mean we didn't get a conflict
    LOG.info("elected leader of {} clock @ {}", clockName, System.currentTimeMillis());

    onElection.run();
  }

}
