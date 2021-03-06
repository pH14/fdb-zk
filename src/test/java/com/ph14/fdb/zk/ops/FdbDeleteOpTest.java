package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.OpResult.DeleteResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.SetDataRequest;
import org.junit.Test;

import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;
import com.ph14.fdb.zk.layer.changefeed.WatchEventChangefeed;

public class FdbDeleteOpTest extends FdbBaseTest {

  @Test
  public void itReturnsErrorIfNodeDoesntExist() {
    Result<DeleteResult, KeeperException> result = fdb.run(
        tr -> fdbDeleteOp.execute(REQUEST, tr, new DeleteRequest(BASE_PATH, 0))).join();
    assertThat(result.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itReturnsErrorIfVersionDoesntMatch() {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();
    fdb.run(tr -> fdbSetDataOp.execute(REQUEST, tr, new SetDataRequest(BASE_PATH, "a".getBytes(), 1))).join();
    fdb.run(tr -> fdbSetDataOp.execute(REQUEST, tr, new SetDataRequest(BASE_PATH, "b".getBytes(), 2))).join();

    Result<DeleteResult, KeeperException> result = fdb.run(
        tr -> fdbDeleteOp.execute(REQUEST, tr, new DeleteRequest(BASE_PATH, 2))).join();

    assertThat(result.unwrapErrOrElseThrow().code()).isEqualTo(Code.BADVERSION);
  }

  @Test
  public void itReturnsErrorIfNodeHasChildren() {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(SUBPATH, new byte[0], Collections.emptyList(), 0))).join();

    Result<DeleteResult, KeeperException> result = fdb.run(
        tr -> fdbDeleteOp.execute(REQUEST, tr, new DeleteRequest(BASE_PATH, 1))).join();

    assertThat(result.unwrapErrOrElseThrow().code()).isEqualTo(Code.NOTEMPTY);
  }

  @Test
  public void itDeletesIfVersionMatchesExactly() {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();
    fdb.run(tr -> fdbSetDataOp.execute(REQUEST, tr, new SetDataRequest(BASE_PATH, "a".getBytes(), 0))).join();
    fdb.run(tr -> fdbSetDataOp.execute(REQUEST, tr, new SetDataRequest(BASE_PATH, "b".getBytes(), 1))).join();

    Result<DeleteResult, KeeperException> result = fdb.run(
        tr -> fdbDeleteOp.execute(REQUEST, tr, new DeleteRequest(BASE_PATH, 2))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new DeleteResult());
  }

  @Test
  public void itDeletesIfVersionIsAllVersionsFlag() {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();

    Result<DeleteResult, KeeperException> result = fdb.run(
        tr -> fdbDeleteOp.execute(REQUEST, tr, new DeleteRequest(BASE_PATH, -1))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new DeleteResult());
  }

  @Test
  public void itUpdatesParentStatAfterSuccessfulDeletion() {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(SUBPATH, new byte[0], Collections.emptyList(), 0))).join();

    Result<ExistsResponse, KeeperException> exists = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, false))).join();
    assertThat(exists.unwrapOrElseThrow().getStat().getCversion()).isEqualTo(1);
    assertThat(exists.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(1);
    long initialPzxid = exists.unwrapOrElseThrow().getStat().getPzxid();

    fdb.run(tr -> fdbDeleteOp.execute(REQUEST, tr, new DeleteRequest(SUBPATH, 0))).join();

    exists = fdb.run(tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, false))).join();
    assertThat(exists.unwrapOrElseThrow().getStat().getCversion()).isEqualTo(2);
    assertThat(exists.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(0);
    assertThat(exists.unwrapOrElseThrow().getStat().getPzxid()).isGreaterThan(initialPzxid);
  }

  @Test
  public void itDoesntPerformWritesIfExceptionIsThrown() {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(SUBPATH, new byte[0], Collections.emptyList(), 0))).join();

    Result<ExistsResponse, KeeperException> exists = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, false))).join();
    assertThat(exists.unwrapOrElseThrow().getStat().getCversion()).isEqualTo(1);
    assertThat(exists.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(1);
    long initialPzxid = exists.unwrapOrElseThrow().getStat().getPzxid();

    FdbDeleteOp throwingFdbDeleteOp = new FdbDeleteOp(fdbNodeReader, fdbNodeWriter, new ThrowingWatchManager(new WatchEventChangefeed(fdb)), fdbEphemeralNodeManager);
    assertThatThrownBy(() -> fdb.run(tr -> throwingFdbDeleteOp.execute(REQUEST, tr, new DeleteRequest(SUBPATH, 0))))
        .hasCauseInstanceOf(RuntimeException.class);

    exists = fdb.run(tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, false))).join();
    assertThat(exists.unwrapOrElseThrow().getStat().getCversion()).isEqualTo(1);
    assertThat(exists.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(1);
    assertThat(exists.unwrapOrElseThrow().getStat().getPzxid()).isEqualTo(initialPzxid);
  }

  @Test
  public void itRemovesEphemeralNodesFromManager() {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), CreateMode.PERSISTENT.toFlag()))).join();
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(SUBPATH, new byte[0], Collections.emptyList(), CreateMode.EPHEMERAL.toFlag()))).join();

    Result<GetChildrenResponse, KeeperException> children = fdb.run(tr -> fdbGetChildrenOp.execute(REQUEST, tr, new GetChildrenRequest(BASE_PATH, false)).join());
    assertThat(children.isOk()).isTrue();
    assertThat(children.unwrapOrElseThrow().getChildren()).containsExactly("bar");

    fdb.run(tr -> fdbDeleteOp.execute(REQUEST, tr, new DeleteRequest(SUBPATH, -1))).join();

    children = fdb.run(tr -> fdbGetChildrenOp.execute(REQUEST, tr, new GetChildrenRequest(BASE_PATH, false)).join());
    assertThat(children.isOk()).isTrue();
    assertThat(children.unwrapOrElseThrow().getChildren()).isEmpty();

    Iterable<String> ephemeralNodePaths = fdb.run(tr -> fdbEphemeralNodeManager.getEphemeralNodeZkPaths(tr, REQUEST.sessionId)).join();
    assertThat(ephemeralNodePaths).isEmpty();
  }

  @Test
  public void itTriggersWatchForNodeDeletion() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Watcher watcher = event -> {
      assertThat(event.getType()).isEqualTo(EventType.NodeDeleted);
      assertThat(event.getPath()).isEqualTo(BASE_PATH);
      countDownLatch.countDown();
    };

    fdb.run(tr -> {
      fdbWatchManager.addNodeDeletedWatch(tr, BASE_PATH, watcher, REQUEST.sessionId);
      return null;
    });

    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));
    assertThat(SERVER_CNXN.getWatchedEvents().peek()).isNull();

    fdb.run(tr -> fdbDeleteOp.execute(REQUEST, tr, new DeleteRequest(BASE_PATH, -1))).join();
    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void itTriggersWatchesOnParentNodeDeletion() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);

    Watcher watcher = event -> {
      assertThat(event.getType()).isEqualTo(EventType.NodeChildrenChanged);
      assertThat(event.getPath()).isEqualTo("/");
      countDownLatch.countDown();
    };

    Result<CreateResponse, KeeperException> result = fdb.run(
        tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0))).join();

    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));
    assertThat(SERVER_CNXN.getWatchedEvents().peek()).isNull();

    fdb.run(tr -> {
      fdbWatchManager.addNodeChildrenWatch(tr, "/", watcher, REQUEST.sessionId);
      return null;
    });

    fdb.run(tr -> fdbDeleteOp.execute(REQUEST, tr, new DeleteRequest(BASE_PATH, -1))).join();
    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
  }

}
