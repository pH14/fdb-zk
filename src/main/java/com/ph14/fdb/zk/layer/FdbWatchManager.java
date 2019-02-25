package com.ph14.fdb.zk.layer;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.ph14.fdb.zk.FdbSchemaConstants;

/**
 * GetData -- sets watch for data changes or node deletion
 * Exists -- if doesn't exist, set watch for node creation. if does exist, set watch for deletion or getData
 *
 * For these we'll want to register multiple watches (and for creation, we want the path directly w/o directory layer
 * since we don't know which prefix we'll get yet), and when the first one fires, removes all of the client's watches
 * for that particular path
 */
public class FdbWatchManager {

  public void addNodeCreatedWatch(Transaction transaction, DirectorySubspace nodeSubspace, Watcher watcher) {
    CompletableFuture<Void> watch = transaction.watch(nodeSubspace.get(FdbSchemaConstants.NODE_CREATED_WATCH_KEY).pack());

    String path = nodeSubspace.getPath().stream().collect(Collectors.joining());

    watch.whenComplete((v, e) -> {
      if (e != null) {
        throw new RuntimeException(e);
      }

      watcher.process(new WatchedEvent(EventType.NodeCreated, KeeperState.SyncConnected, path));
    });
  }

  public void addNodeDataUpdatedWatch(Transaction transaction, DirectorySubspace nodeSubspace, Watcher watcher) {
    CompletableFuture<Void> watch = transaction.watch(nodeSubspace.get(FdbSchemaConstants.NODE_DATA_UPDATED_KEY).pack());

    String path = nodeSubspace.getPath().stream().collect(Collectors.joining("/"));

    watch.whenComplete((v, e) -> {
      if (e != null) {
        throw new RuntimeException(e);
      }

      watcher.process(new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path));
    });
  }

}
