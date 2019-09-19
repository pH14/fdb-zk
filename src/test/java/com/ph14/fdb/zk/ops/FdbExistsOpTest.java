package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.junit.Test;

import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbExistsOpTest extends FdbBaseTest {

  @Test
  public void itFindsStatOfExistingNode() {
    byte[] data = "some string thing".getBytes();
    long timeBeforeExecution = System.currentTimeMillis();

    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, data, Collections.emptyList(), 0))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<ExistsResponse, KeeperException> exists = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, false))).join();

    assertThat(exists.unwrapOrElseThrow().getStat()).isNotNull();

    Stat stat = exists.unwrapOrElseThrow().getStat();

    assertThat(stat.getCzxid()).isGreaterThan(0L);
    assertThat(stat.getMzxid()).isGreaterThan(0L);
    assertThat(stat.getCtime()).isGreaterThanOrEqualTo(timeBeforeExecution);
    assertThat(stat.getMtime()).isGreaterThanOrEqualTo(timeBeforeExecution);
    assertThat(stat.getVersion()).isEqualTo(0);
    assertThat(stat.getCversion()).isEqualTo(0);
    assertThat(stat.getDataLength()).isEqualTo(data.length);
  }

  @Test
  public void itReturnsErrorIfNodeDoesNotExist() {
    Result<ExistsResponse, KeeperException> exists = fdbExistsOp.execute(REQUEST, transaction, new ExistsRequest(BASE_PATH, false)).join();
    assertThat(exists.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itSetsWatchForDataUpdateIfNodeExists() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0))).join();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<ExistsResponse, KeeperException> result2 = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, true))).join();

    fdb.run(tr -> {
      fdbWatchManager.triggerNodeUpdatedWatch(tr, BASE_PATH);
      return null;
    });

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeDataChanged);
    assertThat(event.getPath()).isEqualTo(BASE_PATH);
  }

  @Test
  public void itSetsWatchForNodeCreationIfNodeDoesNotExist() throws InterruptedException {
    Result<ExistsResponse, KeeperException> result2 = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, true))).join();
    assertThat(result2.isOk()).isFalse();

    fdb.run(tr -> {
      fdbWatchManager.triggerNodeCreatedWatch(tr, BASE_PATH);
      return null;
    });

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeCreated);
    assertThat(event.getPath()).isEqualTo(BASE_PATH);
  }

  @Test
  public void itSetsWatchForNodeDeletionIfNodeExists() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0))).join();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<ExistsResponse, KeeperException> result2 = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, true))).join();
    assertThat(result2.isOk()).isTrue();

    fdb.run(tr -> {
      fdbWatchManager.triggerNodeDeletedWatch(tr, BASE_PATH);
      return null;
    });

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeDeleted);
    assertThat(event.getPath()).isEqualTo(BASE_PATH);
  }

  @Test
  public void itPlaysPendingWatchesBeforeReturning() throws InterruptedException {
    fdb.run(tr -> {
      CompletableFuture<Void> fdbWatch = watchEventChangefeed.setZKChangefeedWatch(tr, SERVER_CNXN, REQUEST.sessionId, EventType.NodeCreated, "abc");
      fdbWatch.cancel(true);
      watchEventChangefeed.appendToChangefeed(tr, EventType.NodeCreated, "abc").join();
      return null;
    });

    Result<ExistsResponse, KeeperException> result2 = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, true))).join();
    assertThat(result2.isOk()).isFalse();

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeCreated);
    assertThat(event.getPath()).isEqualTo("abc");
  }

}
