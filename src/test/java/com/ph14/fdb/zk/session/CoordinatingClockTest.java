package com.ph14.fdb.zk.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

public class CoordinatingClockTest {

  private Database fdb;
  private AtomicBoolean wasLeader;

  @Before
  public void setUp() {
    this.fdb = FDB.selectAPIVersion(600).open();
    this.wasLeader = new AtomicBoolean(false);

    fdb.run(tr -> {
      tr.clear(Range.startsWith(CoordinatingClock.KEY_PREFIX));
      return null;
    });
  }

  @Test
  public void itStartsAClockWithoutAnInitialKey() {
    CoordinatingClock coordinatingClock = CoordinatingClock.from(
        fdb,
        "session",
        () -> wasLeader.set(true),
        Clock.fixed(Instant.ofEpochMilli(1050), ZoneId.systemDefault()),
        OptionalLong.of(500));

    assertThat(coordinatingClock.getCurrentTick()).isEmpty();

    coordinatingClock.runOnce();

    assertThat(coordinatingClock.getCurrentTick()).hasValue(1500);
    assertThat(wasLeader.get()).isFalse();
  }

  @Test
  public void itAdvancesAClockWhenKeyIsInThePast() {
    fdb.run(tr -> {
      tr.set(Tuple.from(CoordinatingClock.PREFIX, "session".getBytes(Charsets.UTF_8)).pack(), ByteArrayUtil.encodeInt(500));
      return null;
    });

    CoordinatingClock coordinatingClock = CoordinatingClock.from(
        fdb,
        "session",
        () -> wasLeader.set(true),
        Clock.fixed(Instant.ofEpochMilli(1234), ZoneId.systemDefault()),
        OptionalLong.of(500));

    assertThat(coordinatingClock.getCurrentTick()).hasValue(500);

    coordinatingClock.runOnce();

    assertThat(coordinatingClock.getCurrentTick()).hasValue(1500);
    assertThat(wasLeader.get()).isFalse();
  }

  @Test
  public void itWaitsAndAdvancesClockWhenKeyIsInTheFuture() {
    fdb.run(tr -> {
      tr.set(Tuple.from(CoordinatingClock.PREFIX, "session".getBytes(Charsets.UTF_8)).pack(), ByteArrayUtil.encodeInt(1234));
      return null;
    });

    CoordinatingClock coordinatingClock = CoordinatingClock.from(
        fdb,
        "session",
        () -> wasLeader.set(true),
        Clock.fixed(Instant.ofEpochMilli(1234), ZoneId.systemDefault()),
        OptionalLong.of(500));

    assertThat(coordinatingClock.getCurrentTick()).hasValue(1234);

    coordinatingClock.runOnce();

    assertThat(coordinatingClock.getCurrentTick()).hasValue(1500);
    assertThat(wasLeader.get()).isTrue();
  }

  @Test
  public void itAdvancesManyTimes() {
    CoordinatingClock coordinatingClock = CoordinatingClock.from(
        fdb,
        "session",
        () -> wasLeader.set(true),
        Clock.fixed(Instant.ofEpochMilli(1050), ZoneId.systemDefault()),
        OptionalLong.of(100));

    assertThat(coordinatingClock.getCurrentTick()).isEmpty();

    coordinatingClock.runOnce();
    assertThat(coordinatingClock.getCurrentTick()).hasValue(1100);
    assertThat(wasLeader.get()).isFalse();

    coordinatingClock.runOnce();
    assertThat(coordinatingClock.getCurrentTick()).hasValue(1200);
    assertThat(wasLeader.get()).isTrue();

    coordinatingClock.runOnce();
    assertThat(coordinatingClock.getCurrentTick()).hasValue(1300);
    assertThat(wasLeader.get()).isTrue();

    coordinatingClock.runOnce();
    assertThat(coordinatingClock.getCurrentTick()).hasValue(1400);
    assertThat(wasLeader.get()).isTrue();
  }

  @Test
  public void itOnlyLetsOneClockBecomeTheLeaderBestEffort() {
    fdb.run(tr -> {
      tr.set(Tuple.from(CoordinatingClock.PREFIX, "session".getBytes(Charsets.UTF_8)).pack(), ByteArrayUtil.encodeInt(1000));
      return null;
    });

    AtomicBoolean clockTwoWasLeader = new AtomicBoolean(false);
    AtomicBoolean clockThreeWasLeader = new AtomicBoolean(false);

    CoordinatingClock coordinatingClockOne = CoordinatingClock.from(
        fdb,
        "session",
        () -> wasLeader.set(true),
        Clock.fixed(Instant.ofEpochMilli(1000), ZoneId.systemDefault()),
        OptionalLong.of(500));

    CoordinatingClock coordinatingClockTwo = CoordinatingClock.from(
        fdb,
        "session",
        () -> clockTwoWasLeader.set(true),
        Clock.fixed(Instant.ofEpochMilli(1000), ZoneId.systemDefault()),
        OptionalLong.of(500));

    CoordinatingClock coordinatingClockThree = CoordinatingClock.from(
        fdb,
        "session",
        () -> clockThreeWasLeader.set(true),
        Clock.fixed(Instant.ofEpochMilli(1000), ZoneId.systemDefault()),
        OptionalLong.of(500));

    List<CoordinatingClock> clocks = ImmutableList.of(coordinatingClockOne, coordinatingClockTwo, coordinatingClockThree);

    clocks.stream()
        .map(clock -> CompletableFuture.runAsync(clock::runOnce))
        .collect(Collectors.toList())
        .forEach(CompletableFuture::join);

    // it's possible this could fail with an unfortunate scheduling of the futures, where they wind up running serially
    assertThat(wasLeader.get() ^ clockTwoWasLeader.get() ^ clockThreeWasLeader.get()).isTrue();
    assertThat(coordinatingClockOne.getCurrentTick()).hasValue(1500);

    wasLeader.set(false);
    clockTwoWasLeader.set(false);
    clockThreeWasLeader.set(false);

    clocks.stream()
        .map(clock -> CompletableFuture.runAsync(clock::runOnce))
        .collect(Collectors.toList())
        .forEach(CompletableFuture::join);

    assertThat(wasLeader.get() ^ clockTwoWasLeader.get() ^ clockThreeWasLeader.get()).isTrue();
    assertThat(coordinatingClockOne.getCurrentTick()).hasValue(2000);
  }

}
