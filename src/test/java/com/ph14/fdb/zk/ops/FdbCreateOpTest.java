package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.junit.Test;

import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbCreateOpTest extends FdbBaseTest {

  @Test
  public void itCreatesADirectory() {
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH,  new byte[0], Collections.emptyList(), 0))).join();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));
  }

  @Test
  public void itDoesNotCreateTheSameDirectoryTwice() {
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();
    assertThat(result.unwrapErrOrElseThrow().code()).isEqualTo(Code.NODEEXISTS);
  }

  @Test
  public void itDoesNotCreateDirectoryWithoutParent() {
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(SUBPATH, new byte[0], Collections.emptyList(), 0))).join();
    assertThat(result.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itProgressivelyCreatesNodes() {
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(SUBPATH, new byte[0], Collections.emptyList(), 0))).join();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(SUBPATH));
  }

  @Test
  public void itUpdatesParentNodeVersionAndChildrenCount() {
    Result<ExistsResponse, KeeperException> parent = fdb.run(tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest("/", false))).join();
    assertThat(parent.unwrapOrElseThrow().getStat().getCversion()).isEqualTo(0);
    assertThat(parent.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(0);

    long initialPzxid = parent.unwrapOrElseThrow().getStat().getPzxid();

    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    parent = fdb.run(tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest("/", false))).join();
    assertThat(parent.unwrapOrElseThrow().getStat().getPzxid()).isGreaterThan(initialPzxid);
    assertThat(parent.unwrapOrElseThrow().getStat().getCversion()).isEqualTo(1);
    assertThat(parent.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(1);
  }

  @Test
  public void itCreatesSequentialNodes() {
    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(
            REQUEST, tr,
            new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), CreateMode.PERSISTENT_SEQUENTIAL.toFlag())).join());

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH + "0000000000"));

    result = fdb.run(
        tr -> fdbCreateOp.execute(
            REQUEST, tr,
            new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), CreateMode.PERSISTENT_SEQUENTIAL.toFlag())).join());

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH + "0000000001"));

    Result<ExistsResponse, KeeperException> exists = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, false))).join();
    assertThat(exists.isOk()).isFalse();

    exists = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH + "0000000002", false))).join();
    assertThat(exists.isOk()).isFalse();

    exists = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH + "0000000001", false))).join();
    assertThat(exists.isOk()).isTrue();
  }

  @Test
  public void itTriggersWatchForNodeCreation() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Watcher watcher = event -> {
      assertThat(event.getType()).isEqualTo(EventType.NodeCreated);
      assertThat(event.getPath()).isEqualTo(BASE_PATH);
      countDownLatch.countDown();
    };

    fdb.run(tr -> {
      fdbWatchManager.addNodeCreatedWatch(tr, BASE_PATH, watcher, REQUEST.sessionId);
      return null;
    });

    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));
    assertThat(SERVER_CNXN.getWatchedEvents().peek()).isNull();

    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void itTriggersWatchesOnParentNode() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);

    Watcher watcher = event -> {
      assertThat(event.getType()).isEqualTo(EventType.NodeChildrenChanged);
      assertThat(event.getPath()).isEqualTo("/");
      countDownLatch.countDown();
    };

    fdb.run(tr -> {
      fdbWatchManager.addNodeChildrenWatch(tr, "/", watcher, REQUEST.sessionId);
      return null;
    });

    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));
    assertThat(SERVER_CNXN.getWatchedEvents().peek()).isNull();

    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
  }

}
