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
