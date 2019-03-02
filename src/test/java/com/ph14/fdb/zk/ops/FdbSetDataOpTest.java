package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;
import org.junit.Test;

import com.google.common.base.Strings;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbSetDataOpTest extends FdbBaseTest {

  @Test
  public void itSetsDataForExistingNode() {
    final String data = Strings.repeat("this is the song that never ends ", 10000);

    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  data.getBytes(), Collections.emptyList(), 0))).join();
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    long now = System.currentTimeMillis();

    final String data2 = "this is something else";
    Result<SetDataResponse, KeeperException> result2 = fdb.run(
        tr -> fdbSetDataOp.execute(REQUEST, tr, new SetDataRequest(BASE_PATH, data2.getBytes(), 1))).join();
    assertThat(result2.isOk()).isTrue();

    SetDataResponse setDataResponse = result2.unwrapOrElseThrow();
    assertThat(setDataResponse.getStat().getMtime()).isGreaterThanOrEqualTo(now);
    assertThat(setDataResponse.getStat().getCtime()).isLessThan(setDataResponse.getStat().getMtime());
    assertThat(setDataResponse.getStat().getVersion()).isEqualTo(2);
    assertThat(setDataResponse.getStat().getDataLength()).isEqualTo(data2.getBytes().length);
  }

  @Test
  public void itReturnsErrorIfVersionIsWrong() {
    final String data = Strings.repeat("this is the song that never ends ", 10000);
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  data.getBytes(), Collections.emptyList(), 0))).join();
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    final String data2 = "this is something else";
    Result<SetDataResponse, KeeperException> result2 = fdb.run(
        tr -> fdbSetDataOp.execute(REQUEST, tr, new SetDataRequest(BASE_PATH, data2.getBytes(), 2))).join();
    assertThat(result2.isOk()).isFalse();
    assertThat(result2.unwrapErrOrElseThrow().code()).isEqualTo(Code.BADVERSION);
  }

  @Test
  public void itReturnsErrorIfNodeDoesNotExist() {
    Result<SetDataResponse, KeeperException> exists = fdbSetDataOp.execute(REQUEST, transaction, new SetDataRequest(BASE_PATH, "hello".getBytes(), 1)).join();
    assertThat(exists.isOk()).isFalse();
    assertThat(exists.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itReturnsErrorIfDataIsTooLarge() {
    final String data = Strings.repeat("this is the song that never ends ", 10000);
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  data.getBytes(), Collections.emptyList(), 0))).join();
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    final byte[] bigData = new byte[1024 * 1024];
    Result<SetDataResponse, KeeperException> result2 = fdb.run(
        tr -> fdbSetDataOp.execute(REQUEST, tr, new SetDataRequest(BASE_PATH, bigData, 1))).join();
    assertThat(result2.isOk()).isTrue();

    assertThatThrownBy(() -> {
      final byte[] webscaleDataTM = new byte[1024 * 1024 + 1];
      fdbSetDataOp.execute(REQUEST, transaction, new SetDataRequest(BASE_PATH, webscaleDataTM, 2)).join();
    }).hasCauseInstanceOf(IOException.class);
  }

  @Test
  public void itTriggersWatchForDataChange() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdb.run(tr ->
        fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0))).join();
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    CountDownLatch countDownLatch = new CountDownLatch(1);
    Watcher watcher = event -> {
      assertThat(event.getType()).isEqualTo(EventType.NodeDataChanged);
      assertThat(event.getPath()).isEqualTo(BASE_PATH);
      countDownLatch.countDown();
    };

    fdb.run(tr -> {
      fdbWatchManager.addNodeDataUpdatedWatch(tr, BASE_PATH, watcher);
      return null;
    });

    Result<SetDataResponse, KeeperException> exists = fdb.run(
        tr -> fdbSetDataOp.execute(REQUEST, tr, new SetDataRequest(BASE_PATH, "hello!".getBytes(), 1))).join();

    assertThat(exists.isOk()).isTrue();
    assertThat(SERVER_CNXN.getWatchedEvents().peek()).isNull();
    assertThat(countDownLatch.await(1, TimeUnit.SECONDS)).isTrue();
  }

}
