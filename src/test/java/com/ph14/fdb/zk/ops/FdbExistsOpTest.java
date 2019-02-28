package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
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

    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH, data, Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<ExistsResponse, KeeperException> exists = fdbExistsOp.execute(REQUEST, transaction, new ExistsRequest(BASE_PATH, false));
    assertThat(exists.isOk()).isTrue();
    assertThat(exists.unwrapOrElseThrow().getStat()).isNotNull();

    Stat stat = exists.unwrapOrElseThrow().getStat();

    assertThat(stat.getCzxid()).isGreaterThan(0L);
    assertThat(stat.getMzxid()).isGreaterThan(0L);
    assertThat(stat.getCtime()).isGreaterThanOrEqualTo(timeBeforeExecution);
    assertThat(stat.getMtime()).isGreaterThanOrEqualTo(timeBeforeExecution);
    assertThat(stat.getVersion()).isEqualTo(1);
    assertThat(stat.getCversion()).isEqualTo(1);
    assertThat(stat.getDataLength()).isEqualTo(data.length);
  }

  @Test
  public void itReturnsErrorIfNodeDoesNotExist() {
    Result<ExistsResponse, KeeperException> exists = fdbExistsOp.execute(REQUEST, transaction, new ExistsRequest(BASE_PATH, false));
    assertThat(exists.isOk()).isFalse();
    assertThat(exists.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itSetsWatchForDataUpdateIfNodeExists() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<ExistsResponse, KeeperException> result2 = fdbExistsOp.execute(REQUEST, transaction, new ExistsRequest(BASE_PATH, true));
    assertThat(result2.isOk()).isTrue();

    transaction.commit().join();

    transaction = fdb.createTransaction();
    fdbWatchManager.triggerNodeUpdatedWatch(transaction, BASE_PATH);
    transaction.commit().join();

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeDataChanged);
    assertThat(event.getPath()).isEqualTo(BASE_PATH);
  }

  @Test
  public void itSetsWatchForNodeCreationIfNodeDoesNotExist() throws InterruptedException {
    Result<ExistsResponse, KeeperException> result2 = fdbExistsOp.execute(REQUEST, transaction, new ExistsRequest(BASE_PATH, true));
    assertThat(result2.isOk()).isFalse();

    transaction.commit().join();

    transaction = fdb.createTransaction();
    fdbWatchManager.triggerNodeCreatedWatch(transaction, BASE_PATH);
    transaction.commit().join();

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeCreated);
    assertThat(event.getPath()).isEqualTo(BASE_PATH);
  }

  @Test
  public void itSetsWatchForNodeDeletionIfNodeExists() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<ExistsResponse, KeeperException> result2 = fdbExistsOp.execute(REQUEST, transaction, new ExistsRequest(BASE_PATH, true));
    assertThat(result2.isOk()).isTrue();

    transaction.commit().join();

    transaction = fdb.createTransaction();
    fdbWatchManager.triggerNodeDeletedWatch(transaction, BASE_PATH);
    transaction.commit().join();

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeDeleted);
    assertThat(event.getPath()).isEqualTo(BASE_PATH);
  }

}
