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
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  "hello".getBytes(), Collections.emptyList(), 0))).join();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<GetDataResponse, KeeperException> result2 = fdb.run(
        tr -> fdbGetDataOp.execute(REQUEST, tr, new GetDataRequest(BASE_PATH, true))).join();
    assertThat(result2.isOk()).isTrue();

    Result<SetDataResponse, KeeperException> exists = fdb.run(
        tr -> fdbSetDataOp.execute(REQUEST, tr, new SetDataRequest(BASE_PATH, "hello!".getBytes(), 1))).join();
    assertThat(exists.isOk()).isTrue();

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeDataChanged);
    assertThat(event.getPath()).isEqualTo(BASE_PATH);
  }

}
