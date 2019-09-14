package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbGetDataOpTest extends FdbBaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(FdbGetDataOpTest.class);

  @Test
  public void itGetsDataFromExistingNode() {
    long timeBeforeCreation = System.currentTimeMillis();
    String data = Strings.repeat("this is the song that never ends ", 10000);

    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  data.getBytes(), Collections.emptyList(), 0))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<GetDataResponse, KeeperException> result2 = fdb.run(tr -> fdbGetDataOp.execute(REQUEST, tr, new GetDataRequest(BASE_PATH, false))).join();

    GetDataResponse getDataResponse = result2.unwrapOrElseThrow();

    assertThat(getDataResponse.getData()).isEqualTo(data.getBytes());
    assertThat(getDataResponse.getStat().getMzxid()).isGreaterThan(0L);
    assertThat(getDataResponse.getStat().getMzxid()).isEqualTo(getDataResponse.getStat().getCzxid());
    assertThat(getDataResponse.getStat().getVersion()).isEqualTo(0);
    assertThat(getDataResponse.getStat().getCtime()).isGreaterThanOrEqualTo(timeBeforeCreation);
    assertThat(getDataResponse.getStat().getCtime()).isEqualTo(getDataResponse.getStat().getMtime());
    assertThat(getDataResponse.getStat().getAversion()).isEqualTo(0);
    assertThat(getDataResponse.getStat().getNumChildren()).isEqualTo(0);
    assertThat(getDataResponse.getStat().getEphemeralOwner()).isEqualTo(0);
    assertThat(getDataResponse.getStat().getDataLength()).isEqualTo(data.getBytes().length);
  }

  @Test
  public void itReturnsErrorIfNodeDoesNotExist() {
    Result<GetDataResponse, KeeperException> exists = fdbGetDataOp.execute(REQUEST, transaction, new GetDataRequest(BASE_PATH, false)).join();
    assertThat(exists.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itSetsWatchForDataUpdate() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<GetDataResponse, KeeperException> result2 = fdb.run(tr -> fdbGetDataOp.execute(REQUEST, tr, new GetDataRequest(BASE_PATH, true))).join();
    assertThat(result2.isOk()).isTrue();

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
  public void itSetsWatchForNodeDeletion() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<GetDataResponse, KeeperException> result2 = fdb.run(tr -> fdbGetDataOp.execute(REQUEST, tr, new GetDataRequest(BASE_PATH, true))).join();
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
  public void itDoesntTriggerTwoWatchesForUpdateAndDelete() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<GetDataResponse, KeeperException> result2 = fdb.run(tr -> fdbGetDataOp.execute(REQUEST, tr, new GetDataRequest(BASE_PATH, true))).join();
    assertThat(result2.isOk()).isTrue();

    fdb.run(tr -> {
      fdbWatchManager.triggerNodeUpdatedWatch(tr, BASE_PATH);
      fdbWatchManager.triggerNodeDeletedWatch(tr, BASE_PATH);
      return null;
    });

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();

    event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNull();
  }

  @Test
  public void itPlaysPendingWatchesBeforeReturning() throws InterruptedException {
    fdb.run(tr -> {
      CompletableFuture<Void> fdbWatch = watchEventChangefeed.setZKChangefeedWatch(tr, SERVER_CNXN, REQUEST.sessionId, EventType.NodeCreated, "abc");
      fdbWatch.cancel(true);
      watchEventChangefeed.appendToChangefeed(tr, EventType.NodeCreated, "abc");
      return null;
    });

    Result<GetDataResponse, KeeperException> result2 = fdb.run(
        tr -> fdbGetDataOp.execute(REQUEST, tr, new GetDataRequest(BASE_PATH, true))).join();
    assertThat(result2.isOk()).isFalse();

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeCreated);
    assertThat(event.getPath()).isEqualTo("abc");
  }

}
