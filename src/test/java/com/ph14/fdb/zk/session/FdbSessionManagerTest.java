package com.ph14.fdb.zk.session;

import static com.ph14.fdb.zk.session.FdbSessionManager.SESSION_DIRECTORY;
import static com.ph14.fdb.zk.session.FdbSessionManager.SESSION_TIMEOUT_DIRECTORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.OptionalLong;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbSessionManagerTest extends FdbBaseTest {

  @Override
  @Before
  public void setUp() {
    super.setUp();
    fdb.run(tr -> DirectoryLayer.getDefault().remove(tr, SESSION_DIRECTORY).join());
    fdb.run(tr -> DirectoryLayer.getDefault().remove(tr, SESSION_TIMEOUT_DIRECTORY).join());
  }

  @Test
  public void itCreatesAndReadsNewSession() {
    FdbSessionManager fdbSessionManager = new FdbSessionManager(
        fdb,
        Clock.fixed(Instant.ofEpochMilli(1234), ZoneId.systemDefault()),
        OptionalLong.of(200));

    long session = fdbSessionManager.createSession(1000);
    long sessionTwo = fdbSessionManager.createSession(1201);

    assertThat(sessionTwo).isGreaterThan(session);

    long expiresAt = fdbSessionManager.getSessionDataById(session).getLong(0);
    long sessionTimeout = fdbSessionManager.getSessionDataById(session).getLong(1);
    assertThat(expiresAt).isEqualTo(2400); // timeout is 1000, current time 1234 --> 2234, rounded up to next tick of 200 --> 2400
    assertThat(sessionTimeout).isEqualTo(1000);

    expiresAt = fdbSessionManager.getSessionDataById(sessionTwo).getLong(0);
    sessionTimeout = fdbSessionManager.getSessionDataById(sessionTwo).getLong(1);
    assertThat(expiresAt).isEqualTo(2600);
    assertThat(sessionTimeout).isEqualTo(1201);
  }

  @Test
  public void itAddsKnownSession() {
    FdbSessionManager fdbSessionManager = new FdbSessionManager(
        fdb,
        Clock.fixed(Instant.ofEpochMilli(1234), ZoneId.systemDefault()),
        OptionalLong.of(200));

    long sessionId = System.currentTimeMillis();

    fdbSessionManager.addSession(sessionId, 1001);

    long expiresAt = fdbSessionManager.getSessionDataById(sessionId).getLong(0);
    long sessionTimeout = fdbSessionManager.getSessionDataById(sessionId).getLong(1);

    assertThat(expiresAt).isEqualTo(2400);
    assertThat(sessionTimeout).isEqualTo(1001);
    assertThat(fdbSessionManager.getExpiredSessionIds(2399)).isEmpty();
    assertThat(fdbSessionManager.getExpiredSessionIds(2400)).containsExactly(sessionId);
  }

  @Test
  public void itCanUpdateExpirationWithHeartbeatAndClockAdvancement() {
    FdbSessionManager fdbSessionManager = new FdbSessionManager(
        fdb,
        Clock.fixed(Instant.ofEpochMilli(1234), ZoneId.systemDefault()),
        OptionalLong.of(200));

    long session = fdbSessionManager.createSession(1000);
    assertThat(fdbSessionManager.getExpiredSessionIds(2401)).containsExactly(session);

    // advance our clock
    fdbSessionManager = new FdbSessionManager(
        fdb,
        Clock.fixed(Instant.ofEpochMilli(12345), ZoneId.systemDefault()),
        OptionalLong.of(200));

    fdbSessionManager.touchSession(session, 500);

    long expiresAt = fdbSessionManager.getSessionDataById(session).getLong(0);
    long sessionTimeout = fdbSessionManager.getSessionDataById(session).getLong(1);

    assertThat(expiresAt).isEqualTo(13000);
    assertThat(sessionTimeout).isEqualTo(500);

    assertThat(fdbSessionManager.getExpiredSessionIds(2401)).isEmpty();
    assertThat(fdbSessionManager.getExpiredSessionIds(13001)).containsExactly(session);
  }

  @Test
  public void itCanUpdateExpirationWithHeartbeatAndTimeoutAdvancement() {
    FdbSessionManager fdbSessionManager = new FdbSessionManager(
        fdb,
        Clock.fixed(Instant.ofEpochMilli(1234), ZoneId.systemDefault()),
        OptionalLong.of(200));

    long session = fdbSessionManager.createSession(1000);
    assertThat(fdbSessionManager.getExpiredSessionIds(2401)).containsExactly(session);

    // embiggen the timeout
    fdbSessionManager.touchSession(session, 1500);

    long expiresAt = fdbSessionManager.getSessionDataById(session).getLong(0);
    long sessionTimeout = fdbSessionManager.getSessionDataById(session).getLong(1);

    assertThat(expiresAt).isEqualTo(2800);
    assertThat(sessionTimeout).isEqualTo(1500);

    assertThat(fdbSessionManager.getExpiredSessionIds(2401)).isEmpty();
    assertThat(fdbSessionManager.getExpiredSessionIds(2800)).containsExactly(session);
  }

  @Test
  public void itDoesNothingWhenHeartbeatingWithSmallerExpiration() {
    FdbSessionManager fdbSessionManager = new FdbSessionManager(
        fdb,
        Clock.fixed(Instant.ofEpochMilli(1234), ZoneId.systemDefault()),
        OptionalLong.of(200));

    long session = fdbSessionManager.createSession(1000);
    assertThat(fdbSessionManager.getExpiredSessionIds(2401)).containsExactly(session);

    fdbSessionManager.touchSession(session, 0);

    long expiresAt = fdbSessionManager.getSessionDataById(session).getLong(0);
    long sessionTimeout = fdbSessionManager.getSessionDataById(session).getLong(1);

    assertThat(expiresAt).isEqualTo(2400);
    assertThat(sessionTimeout).isEqualTo(1000);

    assertThat(fdbSessionManager.getExpiredSessionIds(2399)).isEmpty();
    assertThat(fdbSessionManager.getExpiredSessionIds(2400)).containsExactly(session);
  }

  @Test
  public void itFindsExpiredNodes() {
    FdbSessionManager fdbSessionManager = new FdbSessionManager(
        fdb,
        Clock.fixed(Instant.ofEpochMilli(1234), ZoneId.systemDefault()),
        OptionalLong.of(200));

    long session = fdbSessionManager.createSession(1000);
    assertThat(fdbSessionManager.getExpiredSessionIds(2399)).isEmpty();
    assertThat(fdbSessionManager.getExpiredSessionIds(2400)).containsExactly(session);
    assertThat(fdbSessionManager.getExpiredSessionIds(2401)).containsExactly(session);
    assertThat(fdbSessionManager.getExpiredSessionIds(2402)).containsExactly(session);

    long sessionTwo = fdbSessionManager.createSession(1201);
    assertThat(fdbSessionManager.getExpiredSessionIds(2399)).isEmpty();
    assertThat(fdbSessionManager.getExpiredSessionIds(2400)).containsExactly(session);
    assertThat(fdbSessionManager.getExpiredSessionIds(2401)).containsExactly(session);
    assertThat(fdbSessionManager.getExpiredSessionIds(2601)).containsExactly(session, sessionTwo);
  }

  @Test
  public void itThrowsWhenCheckingExpiredSession() throws SessionExpiredException, SessionMovedException {
    FdbSessionManager fdbSessionManager = new FdbSessionManager(
        fdb,
        Clock.fixed(Instant.ofEpochMilli(1234), ZoneId.systemDefault()),
        OptionalLong.of(200));

    long session = fdbSessionManager.createSession(1000);

    fdbSessionManager.checkSession(session, null);

    // advance the clock
    fdbSessionManager = new FdbSessionManager(
        fdb,
        Clock.fixed(Instant.ofEpochMilli(2400), ZoneId.systemDefault()),
        OptionalLong.of(200));

    FdbSessionManager finalFdbSessionManager = fdbSessionManager;
    assertThatThrownBy(() -> finalFdbSessionManager.checkSession(session, null))
        .isInstanceOf(SessionExpiredException.class);
  }

  @Test
  public void itRemovesSessions() {
    FdbSessionManager fdbSessionManager = new FdbSessionManager(
        fdb,
        Clock.fixed(Instant.ofEpochMilli(1234), ZoneId.systemDefault()),
        OptionalLong.of(200));

    long session = fdbSessionManager.createSession(1000);
    long sessionTwo = fdbSessionManager.createSession(1000);

    assertThat(fdbSessionManager.getExpiredSessionIds(2401)).containsExactly(session, sessionTwo);
    assertThat(fdbSessionManager.getSessionDataById(session).getLong(0)).isEqualTo(2400);

    fdbSessionManager.removeSession(session);

    assertThat(fdbSessionManager.getExpiredSessionIds(2401)).containsExactly(sessionTwo);
    assertThatThrownBy(() -> fdbSessionManager.getSessionDataById(session)).isInstanceOf(NullPointerException.class);
  }

}
