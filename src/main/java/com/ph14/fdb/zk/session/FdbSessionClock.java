package com.ph14.fdb.zk.session;

import java.io.Closeable;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;

public class FdbSessionClock implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(FdbSessionClock.class);

  private final Clock clock;
  private final FdbSessionManager fdbSessionManager;
  private final FdbSessionDataPurger fdbSessionDataPurger;
  private final CoordinatingClock coordinatingClock;

  public FdbSessionClock(Database fdb,
                         Clock clock,
                         OptionalLong serverTickMillis,
                         FdbSessionManager fdbSessionManager,
                         FdbSessionDataPurger fdbSessionDataPurger) {
    this.clock = clock;
    this.fdbSessionDataPurger = fdbSessionDataPurger;
    this.fdbSessionManager = fdbSessionManager;
    this.coordinatingClock = CoordinatingClock.from(
        fdb,
        "fdb-zk-session-cleanup",
        this::cleanExpiredSessions,
        clock,
        serverTickMillis);
  }

  public void run() {
    coordinatingClock.start();
  }

  public void runOnce() {
    coordinatingClock.runOnce();
  }

  @Override
  public void close() {
    coordinatingClock.close();
  }

  private void cleanExpiredSessions() {
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (long expiredSessionId : fdbSessionManager.getExpiredSessionIds(clock.millis())) {
      futures.add(fdbSessionDataPurger.removeAllSessionData(expiredSessionId));
    }

    futures.forEach(CompletableFuture::join);

    if (futures.size() > 0) {
      LOG.info("Cleared ephemeral nodes for {} sessions", futures.size());
    }
  }

}
