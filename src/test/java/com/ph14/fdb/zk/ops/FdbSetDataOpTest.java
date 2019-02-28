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
    String data = Strings.repeat("this is the song that never ends ", 10000);

    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH,  data.getBytes(), Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    long now = System.currentTimeMillis();

    data = "this is something else";
    Result<SetDataResponse, KeeperException> result2 = fdbSetDataOp.execute(REQUEST, transaction, new SetDataRequest(BASE_PATH, data.getBytes(), 1));
    assertThat(result2.isOk()).isTrue();

    SetDataResponse setDataResponse = result2.unwrapOrElseThrow();
    assertThat(setDataResponse.getStat().getMtime()).isGreaterThanOrEqualTo(now);
    assertThat(setDataResponse.getStat().getCtime()).isLessThan(setDataResponse.getStat().getMtime());
    assertThat(setDataResponse.getStat().getVersion()).isEqualTo(2);
    assertThat(setDataResponse.getStat().getDataLength()).isEqualTo(data.getBytes().length);
  }

  @Test
  public void itReturnsErrorIfVersionIsWrong() {
    String data = Strings.repeat("this is the song that never ends ", 10000);
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH,  data.getBytes(), Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    data = "this is something else";
    Result<SetDataResponse, KeeperException> result2 = fdbSetDataOp.execute(REQUEST, transaction, new SetDataRequest(BASE_PATH, data.getBytes(), 2));
    assertThat(result2.isOk()).isFalse();
    assertThat(result2.unwrapErrOrElseThrow().code()).isEqualTo(Code.BADVERSION);
  }

  @Test
  public void itReturnsErrorIfNodeDoesNotExist() {
    Result<SetDataResponse, KeeperException> exists = fdbSetDataOp.execute(REQUEST, transaction, new SetDataRequest(BASE_PATH, "hello".getBytes(), 1));
    assertThat(exists.isOk()).isFalse();
    assertThat(exists.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itReturnsErrorIfDataIsTooLarge() {
    String data = Strings.repeat("this is the song that never ends ", 10000);
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH,  data.getBytes(), Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    byte[] bigData = new byte[1024 * 1024];
    Result<SetDataResponse, KeeperException> result2 = fdbSetDataOp.execute(REQUEST, transaction, new SetDataRequest(BASE_PATH, bigData, 1));
    assertThat(result2.isOk()).isTrue();

    assertThatThrownBy(() -> {
      byte[] webscaleDataTM = new byte[1024 * 1024 + 1];
      fdbSetDataOp.execute(REQUEST, transaction, new SetDataRequest(BASE_PATH, webscaleDataTM, 2));
    }).hasCauseInstanceOf(IOException.class);
  }

  @Test
  public void itTriggersWatchForDataChange() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    CountDownLatch countDownLatch = new CountDownLatch(1);
    Watcher watcher = event -> {
      assertThat(event.getType()).isEqualTo(EventType.NodeDataChanged);
      assertThat(event.getPath()).isEqualTo(BASE_PATH);
      countDownLatch.countDown();
    };

    fdbWatchManager.addNodeDataUpdatedWatch(transaction, BASE_PATH, watcher);
    transaction.commit().join();

    transaction = fdb.createTransaction();

    Result<SetDataResponse, KeeperException> exists = fdbSetDataOp.execute(REQUEST, transaction, new SetDataRequest(BASE_PATH, "hello!".getBytes(), 1));
    assertThat(exists.isOk()).isTrue();
    assertThat(SERVER_CNXN.getWatchedEvents().peek()).isNull();

    transaction.commit().join();
    assertThat(countDownLatch.await(1, TimeUnit.SECONDS)).isTrue();
  }

}
