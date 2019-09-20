package com.ph14.fdb.zk.session;

import java.io.Closeable;
import java.time.Clock;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
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

  static final String PREFIX = "fdb-zk-clock";
  static final byte[] KEY_PREFIX = Tuple.from(PREFIX).pack();

  static final long DEFAULT_TICK_MILLIS = 1000;

  private final Database fdb;
  private final ScheduledExecutorService executorService;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final String clockName;
  private final byte[] clockKey;
  private final Runnable onElection;
  private final Clock clock;
  private final long tickMillis;

  private CoordinatingClock(Database fdb, String clockName, Runnable onElection, Clock clock, OptionalLong tickMillis) {
    this.fdb = fdb;

    this.executorService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
        .setNameFormat("coordinating-clock-" + clockName)
        .setDaemon(true)
        .build());

    this.clockName = clockName;
    this.clockKey = Tuple.from(PREFIX, clockName.getBytes(Charsets.UTF_8)).pack();
    this.onElection = onElection;
    this.clock = clock;
    this.tickMillis = tickMillis.orElse(DEFAULT_TICK_MILLIS);
  }

  public static CoordinatingClock from(Database fdb, String clockName, Runnable onElection) {
    return new CoordinatingClock(fdb, clockName, onElection, Clock.systemUTC(), OptionalLong.empty());
  }

  public static CoordinatingClock from(Database fdb, String clockName, Runnable onElection, Clock clock, OptionalLong tickMillis) {
    return new CoordinatingClock(fdb, clockName, onElection, clock, tickMillis);
  }

  public CoordinatingClock start() {
    Preconditions.checkState(!isClosed.get(), "cannot start clock, already closed");

    executorService.scheduleAtFixedRate(() -> {
      if (!isClosed.get()) {
        keepTime();
      }
    }, 0, tickMillis, TimeUnit.MILLISECONDS);

    return this;
  }

  public OptionalLong getCurrentTick() {
    return fdb.read(rt -> {
      byte[] value = rt.get(clockKey).join();

      if (value != null) {
        return OptionalLong.of(ByteArrayUtil.decodeInt(value));
      } else {
        return OptionalLong.empty();
      }
    });
  }

  @VisibleForTesting
  void runOnce() {
    keepTime();
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
    final long winningTick;

    try (Transaction transaction = fdb.createTransaction()) {
      byte[] currentTickToWaitFor = transaction.get(clockKey).join();

      long now = clock.millis();

      if (currentTickToWaitFor == null) {
        transaction.set(clockKey, ByteArrayUtil.encodeInt(roundToTick(now + tickMillis)));
        transaction.commit().join();
        return;
      }

      long nextPersistedTick = roundToTick(ByteArrayUtil.decodeInt(currentTickToWaitFor) + tickMillis);

      boolean mustRetryBeforeEligibleForLeader = false;
      // our clock is ahead of the next tick, we'll try to advance it further
      if (now > nextPersistedTick) {
        nextPersistedTick = roundToTick(now + tickMillis);
        mustRetryBeforeEligibleForLeader = true;
      } else {
        // we're behind, so wait until we think our real-time is the next tick
        Thread.sleep(nextPersistedTick - now);
      }

      transaction.set(clockKey, ByteArrayUtil.encodeInt(nextPersistedTick));
      transaction.commit().join();

      winningTick = nextPersistedTick;

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
    LOG.info("elected leader of {} clock @ {}", clockName, winningTick);

    onElection.run();
  }

  private long roundToTick(long millis) {
    return tickMillis * (millis / tickMillis);
  }

}
