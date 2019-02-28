package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
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

    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH,  data.getBytes(), Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    transaction.commit().join();
    transaction = fdb.createTransaction();

    Result<GetDataResponse, KeeperException> result2 = fdbGetDataOp.execute(REQUEST, transaction, new GetDataRequest(BASE_PATH, false));

    transaction.commit().join();

    assertThat(result2.isOk()).isTrue();
    GetDataResponse getDataResponse = result2.unwrapOrElseThrow();

    assertThat(getDataResponse.getData()).isEqualTo(data.getBytes());
    assertThat(getDataResponse.getStat().getMzxid()).isGreaterThan(0L);
    assertThat(getDataResponse.getStat().getMzxid()).isEqualTo(getDataResponse.getStat().getCzxid());
    assertThat(getDataResponse.getStat().getVersion()).isEqualTo(1);
    assertThat(getDataResponse.getStat().getCtime()).isGreaterThanOrEqualTo(timeBeforeCreation);
    assertThat(getDataResponse.getStat().getCtime()).isEqualTo(getDataResponse.getStat().getMtime());
    assertThat(getDataResponse.getStat().getAversion()).isEqualTo(1);
    assertThat(getDataResponse.getStat().getNumChildren()).isEqualTo(0);
    assertThat(getDataResponse.getStat().getEphemeralOwner()).isEqualTo(0);
    assertThat(getDataResponse.getStat().getDataLength()).isEqualTo(data.getBytes().length);
  }

  @Test
  public void itReturnsErrorIfNodeDoesNotExist() {
    Result<GetDataResponse, KeeperException> exists = fdbGetDataOp.execute(REQUEST, transaction, new GetDataRequest(BASE_PATH, false));
    assertThat(exists.isOk()).isFalse();
    assertThat(exists.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itSetsWatchForDataUpdate() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<GetDataResponse, KeeperException> result2 = fdbGetDataOp.execute(REQUEST, transaction, new GetDataRequest(BASE_PATH, true));
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

}
