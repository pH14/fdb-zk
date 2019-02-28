package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.junit.Test;

import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbCreateOpTest extends FdbBaseTest {

  @Test
  public void itCreatesADirectory() {
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH,  new byte[0], Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));
  }

  @Test
  public void itDoesNotCreateTheSameDirectoryTwice() {
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0));
    assertThat(result.isOk()).isFalse();
    assertThat(result.unwrapErrOrElseThrow().code()).isEqualTo(Code.NODEEXISTS);
  }

  @Test
  public void itDoesNotCreateDirectoryWithoutParent() {
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(SUBPATH, new byte[0], Collections.emptyList(), 0));
    assertThat(result.isErr()).isTrue();
    assertThat(result.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itProgressivelyCreatesNodes() {
    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(SUBPATH, new byte[0], Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(SUBPATH));
  }

  @Test
  public void itTriggersWatchForNodeCreation() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Watcher watcher = event -> {
      assertThat(event.getType()).isEqualTo(EventType.NodeCreated);
      assertThat(event.getPath()).isEqualTo(BASE_PATH);
      countDownLatch.countDown();
    };

    fdbWatchManager.addNodeCreatedWatch(transaction, BASE_PATH, watcher);
    transaction.commit().join();

    transaction = fdb.createTransaction();

    Result<CreateResponse, KeeperException> result = fdbCreateOp.execute(REQUEST, transaction, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));
    assertThat(SERVER_CNXN.getWatchedEvents().peek()).isNull();

    transaction.commit().join();

    assertThat(countDownLatch.await(2, TimeUnit.SECONDS)).isTrue();
  }

}
