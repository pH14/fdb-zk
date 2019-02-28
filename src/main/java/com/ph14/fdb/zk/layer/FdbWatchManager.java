package com.ph14.fdb.zk.layer;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.inject.Inject;

/**
 * GetData -- sets watch for data changes or node deletion
 * Exists -- if doesn't exist, set watch for node creation. if does exist, set watch for deletion or getData
 *
 * For these we'll want to register multiple watches (and for creation, we want the path directly w/o directory layer
 * since we don't know which prefix we'll get yet), and when the first one fires, removes all of the client's watches
 * for that particular path
 */
public class FdbWatchManager {

  private static final Logger LOG = LoggerFactory.getLogger(FdbWatchManager.class);

  private static final String WATCH_DIRECTORY = "fdb-zk-watch";

  private static final byte[] CREATED_NODE_PATH = new byte[] { 1 };
  private static final byte[] DELETED_NODE_PATH = new byte[] { 2 };
  private static final byte[] UPDATED_NODE_PATH = new byte[] { 3 };

  private final byte[] versionstampValue;
  private final DirectorySubspace directorySubspace;

  @Inject
  public FdbWatchManager(Database database) {
    this.directorySubspace = database.run(tr ->
        DirectoryLayer.getDefault().createOrOpen(tr, Collections.singletonList(WATCH_DIRECTORY)).join());
    this.versionstampValue = Tuple.from(Versionstamp.incomplete()).packWithVersionstamp();
  }

  public void triggerNodeCreatedWatch(Transaction transaction, String zkPath) {
    transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, getCreatedWatchPath(zkPath), versionstampValue);
  }

  public void triggerNodeUpdatedWatch(Transaction transaction, String zkPath) {
    transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, getUpdatedWatchPath(zkPath), versionstampValue);
  }

  public void triggerNodeDeletedWatch(Transaction transaction, String zkPath) {
    transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, getDeletedWatchPath(zkPath), versionstampValue);
  }

  public void addNodeCreatedWatch(Transaction transaction, String zkPath, Watcher watcher) {
    LOG.info("Adding creation watch Path is : {}", getCreatedWatchPath(zkPath));
    CompletableFuture<Void> watch = transaction.watch(getCreatedWatchPath(zkPath));

    watch.whenComplete((v, e) -> {
      if (e != null) {
        throw new RuntimeException(e);
      }

      watcher.process(new WatchedEvent(EventType.NodeCreated, KeeperState.SyncConnected, zkPath));
    });
  }

  public void addNodeDataUpdatedWatch(Transaction transaction, String zkPath, Watcher watcher) {
    CompletableFuture<Void> watch = transaction.watch(getUpdatedWatchPath(zkPath));

    watch.whenComplete((v, e) -> {
      if (e != null) {
        throw new RuntimeException(e);
      }

      watcher.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, zkPath));
    });
  }

  public void addNodeDeletedWatch(Transaction transaction, String zkPath, Watcher watcher) {
    CompletableFuture<Void> watch = transaction.watch(getDeletedWatchPath(zkPath));

    watch.whenComplete((v, e) -> {
      if (e != null) {
        throw new RuntimeException(e);
      }

      watcher.process(new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, zkPath));
    });
  }

  private byte[] getCreatedWatchPath(String zkPath) {
    return directorySubspace.pack(Tuple.from(CREATED_NODE_PATH, zkPath));
  }

  private byte[] getDeletedWatchPath(String zkPath) {
    return directorySubspace.pack(Tuple.from(DELETED_NODE_PATH, zkPath));
  }

  private byte[] getUpdatedWatchPath(String zkPath) {
    return directorySubspace.pack(Tuple.from(UPDATED_NODE_PATH, zkPath));
  }

}
