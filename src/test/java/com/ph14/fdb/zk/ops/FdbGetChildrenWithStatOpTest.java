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
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.junit.Test;

import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbGetChildrenWithStatOpTest extends FdbBaseTest {

  @Test
  public void itListsNoChildrenWhenThereAreNoChildren() {
    Result<GetChildren2Response, KeeperException> result = fdb.run(
        tr -> fdbGetChildrenWithStatOp.execute(REQUEST, tr, new GetChildren2Request("/", false))).join();
    assertThat(result.unwrapOrElseThrow().getChildren()).isEmpty();
  }

  @Test
  public void itReturnsErrorIfNodeDoesntExist() {
    Result<GetChildren2Response, KeeperException> result = fdb.run(
        tr -> fdbGetChildrenWithStatOp.execute(REQUEST, tr, new GetChildren2Request(BASE_PATH, false))).join();
    assertThat(result.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itReturnsChildrenOfRoot() {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest("/pwh", new byte[0], Collections.emptyList(), 0))).join();

    Result<GetChildren2Response, KeeperException> result = fdb.run(
        tr -> fdbGetChildrenWithStatOp.execute(REQUEST, tr, new GetChildren2Request("/", false))).join();

    assertThat(result.unwrapOrElseThrow().getChildren()).containsExactly("pwh");
    assertThat(result.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(1);
  }

  @Test
  public void itReturnsChildren() {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest("/abc", new byte[0], Collections.emptyList(), 0))).join();
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest("/abc/def", new byte[0], Collections.emptyList(), 0))).join();
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest("/abc/ghi", new byte[0], Collections.emptyList(), 0))).join();
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest("/abc/ghi/xyz", new byte[0], Collections.emptyList(), 0))).join();

    Result<GetChildren2Response, KeeperException> result = fdb.run(
        tr -> fdbGetChildrenWithStatOp.execute(REQUEST, tr, new GetChildren2Request("/", false))).join();
    assertThat(result.unwrapOrElseThrow().getChildren()).containsExactly("abc");
    assertThat(result.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(1);

    result = fdb.run(tr -> fdbGetChildrenWithStatOp.execute(REQUEST, tr, new GetChildren2Request("/abc", false))).join();
    assertThat(result.unwrapOrElseThrow().getChildren()).containsExactly("def", "ghi");
    assertThat(result.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(2);

    result = fdb.run(tr -> fdbGetChildrenWithStatOp.execute(REQUEST, tr, new GetChildren2Request("/abc/def", false))).join();
    assertThat(result.unwrapOrElseThrow().getChildren()).isEmpty();
    assertThat(result.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(0);

    result = fdb.run(tr -> fdbGetChildrenWithStatOp.execute(REQUEST, tr, new GetChildren2Request("/abc/ghi", false))).join();
    assertThat(result.unwrapOrElseThrow().getChildren()).containsExactly("xyz");
    assertThat(result.unwrapOrElseThrow().getStat().getNumChildren()).isEqualTo(1);
  }

  @Test
  public void itSetsWatchForParentDeletion() throws InterruptedException {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest("/abc",  "hello".getBytes(), Collections.emptyList(), 0))).join();
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest("/abc/def",  "hello".getBytes(), Collections.emptyList(), 0))).join();

    fdb.run(tr -> fdbGetChildrenWithStatOp.execute(REQUEST, tr, new GetChildren2Request("/abc", true))).join();

    fdb.run(tr -> {
      fdbWatchManager.triggerNodeDeletedWatch(tr, "/abc");
      return null;
    });

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeDeleted);
    assertThat(event.getPath()).isEqualTo("/abc");
  }

  @Test
  public void itSetsChildWatch() throws InterruptedException {
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest("/abc",  "hello".getBytes(), Collections.emptyList(), 0))).join();
    fdb.run(tr -> fdbCreateOp.execute(REQUEST, tr, new CreateRequest("/abc/def",  "hello".getBytes(), Collections.emptyList(), 0))).join();

    fdb.run(tr -> fdbGetChildrenWithStatOp.execute(REQUEST, tr, new GetChildren2Request("/abc", true))).join();

    fdb.run(tr -> {
      fdbWatchManager.triggerNodeChildrenWatch(tr, "/abc");
      return null;
    });

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeChildrenChanged);
    assertThat(event.getPath()).isEqualTo("/abc");
  }

  @Test
  public void itPlaysPendingWatchesBeforeReturning() throws InterruptedException {
    fdb.run(tr -> {
      CompletableFuture<Void> fdbWatch = watchEventChangefeed.setZKChangefeedWatch(tr, SERVER_CNXN, REQUEST.sessionId, EventType.NodeCreated, "abc");
      fdbWatch.cancel(true);
      watchEventChangefeed.appendToChangefeed(tr, EventType.NodeCreated, "abc");
      return null;
    });

    Result<GetChildren2Response, KeeperException> result2 = fdb.run(
        tr -> fdbGetChildrenWithStatOp.execute(REQUEST, tr, new GetChildren2Request(BASE_PATH, true))).join();
    assertThat(result2.isOk()).isFalse();

    WatchedEvent event = SERVER_CNXN.getWatchedEvents().poll(1, TimeUnit.SECONDS);
    assertThat(event).isNotNull();
    assertThat(event.getType()).isEqualTo(EventType.NodeCreated);
    assertThat(event.getPath()).isEqualTo("abc");
  }

}
