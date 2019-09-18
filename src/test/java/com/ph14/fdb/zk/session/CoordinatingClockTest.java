package com.ph14.fdb.zk.session;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

public class CoordinatingClockTest {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatingClockTest.class);

  @Test
  public void itDoes() throws InterruptedException {
    FDB fdb = FDB.selectAPIVersion(600);
    Database db = fdb.open();

    List<CoordinatingClock> coordinatingClocks = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      final int id = i;
      coordinatingClocks.add(CoordinatingClock.start(db, "session", () -> LOG.info("clock {} elected", id)));
    }

    Thread.sleep(10000);

    coordinatingClocks.forEach(CoordinatingClock::close);

    LOG.info("Shutdown");
  }

}
