package com.ph14.fdb.zk.layer;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.inject.Inject;

/**
 * Uses the DirectoryLayer to allocate a small keyspace for
 * creation / update / deletes watches. When these events
 * occur, the keys are written with a versionstamp mutation
 * that trigger watches
 */
public class FdbWatchManager {

  private static final Logger LOG = LoggerFactory.getLogger(FdbWatchManager.class);

  private static final String WATCH_DIRECTORY = "fdb-zk-watch";

  private static final byte[] CREATED_NODE_PATH = new byte[] { 1 };
  private static final byte[] DELETED_NODE_PATH = new byte[] { 2 };
  private static final byte[] UPDATED_NODE_PATH = new byte[] { 3 };
  private static final byte[] CHILDREN_NODE_PATH = new byte[] { 4 };

  private final ListMultimap<Watcher, CompletableFuture<Void>> watchesByWatcher = ArrayListMultimap.create();

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

  public void triggerNodeChildrenWatch(Transaction transaction, String zkPath) {
    transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, getChildrenWatchPath(zkPath), versionstampValue);
  }

  public void addNodeCreatedWatch(Transaction transaction, String zkPath, Watcher watcher) {
    synchronized (watchesByWatcher) {
      CompletableFuture<Void> watch = transaction.watch(getCreatedWatchPath(zkPath));
      watch.whenComplete(createWatchCallback(watcher, zkPath, EventType.NodeCreated));

      watchesByWatcher.put(watcher, watch);
    }
  }

  public void addNodeDataUpdatedWatch(Transaction transaction, String zkPath, Watcher watcher) {
    synchronized (watchesByWatcher) {
      CompletableFuture<Void> watch = transaction.watch(getUpdatedWatchPath(zkPath));
      watch.whenComplete(createWatchCallback(watcher, zkPath, EventType.NodeDataChanged));

      watchesByWatcher.put(watcher, watch);
    }
  }

  public void addNodeDeletedWatch(Transaction transaction, String zkPath, Watcher watcher) {
    synchronized (watchesByWatcher) {
      CompletableFuture<Void> watch = transaction.watch(getDeletedWatchPath(zkPath));
      watch.whenComplete(createWatchCallback(watcher, zkPath, EventType.NodeDeleted));

      watchesByWatcher.put(watcher, watch);
    }
  }

  public void addNodeChildrenWatch(Transaction transaction, String zkPath, Watcher watcher) {
    synchronized (watchesByWatcher) {
      CompletableFuture<Void> watch = transaction.watch(getChildrenWatchPath(zkPath));
      watch.whenComplete(createWatchCallback(watcher, zkPath, EventType.NodeChildrenChanged));

      watchesByWatcher.put(watcher, watch);
    }
  }

  private BiConsumer<Void, Throwable> createWatchCallback(Watcher watcher, String zkPath, EventType eventType) {
    return (v, e) -> {
      if (e != null) {
        throw new RuntimeException(e);
      }

      synchronized (watchesByWatcher) {
        if (watchesByWatcher.get(watcher).isEmpty()) {
          return;
        }

        watcher.process(new WatchedEvent(eventType, KeeperState.SyncConnected, zkPath));

        watchesByWatcher.get(watcher).clear();
      }
    };
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

  private byte[] getChildrenWatchPath(String zkPath) {
    return directorySubspace.pack(Tuple.from(CHILDREN_NODE_PATH, zkPath));
  }

}
