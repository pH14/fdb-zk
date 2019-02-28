package com.ph14.fdb.zk.watches;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;
import org.junit.Test;

import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbWatchLocalIntegrationTests extends FdbBaseTest {

  @Test
  public void itSetsAndFiresWatchForGetDataUpdates() throws InterruptedException {
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<GetDataResponse, KeeperException> result2 = fdbGetDataOp.execute(REQUEST, transaction, new GetDataRequest(BASE_PATH, true));
    assertThat(result2.isOk()).isTrue();

    transaction.commit().join();

    transaction = fdb.createTransaction();

    Result<SetDataResponse, KeeperException> exists = fdbSetDataOp.execute(REQUEST, transaction, new SetDataRequest(BASE_PATH, "hello!".getBytes(), 1));
    assertThat(exists.isOk()).isTrue();
    assertThat(SERVER_CNXN.getWatchedEvents().peek()).isNull();

    transaction.commit().join();

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeDataChanged);
    assertThat(event.getPath()).isEqualTo(BASE_PATH);
  }

}
