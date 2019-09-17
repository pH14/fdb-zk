package com.ph14.fdb.zk.ops;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.layer.StatKey;

public class FdbSetWatchesOp implements FdbOp<SetWatches, Void> {

  private final FdbNodeReader fdbNodeReader;
  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbSetWatchesOp(FdbNodeReader fdbNodeReader,
                         FdbWatchManager fdbWatchManager) {
    this.fdbNodeReader = fdbNodeReader;
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public CompletableFuture<Result<Void, KeeperException>> execute(Request zkRequest, Transaction transaction, SetWatches setWatches) {
    Watcher watcher = zkRequest.cnxn;

    List<CompletableFuture<Void>> backfilledWatches = new ArrayList<>(
        setWatches.getDataWatches().size()
            + setWatches.getExistWatches().size()
            + setWatches.getChildWatches().size());

    for (String path : setWatches.getDataWatches()) {
      backfilledWatches.add(
          readNodeStat(transaction, path, StatKey.MZXID)
              .thenAccept(stat -> {
                if (stat.getMzxid() == 0) {
                  watcher.process(new WatchedEvent(EventType.NodeDeleted,
                      KeeperState.SyncConnected, path));
                } else if (stat.getMzxid() > setWatches.getRelativeZxid()) {
                  watcher.process(new WatchedEvent(EventType.NodeDataChanged,
                      KeeperState.SyncConnected, path));
                } else {
                  fdbWatchManager.addNodeDataUpdatedWatch(transaction, path, watcher, zkRequest.sessionId);
                }
              })
      );
    }

    for (String path : setWatches.getExistWatches()) {
      backfilledWatches.add(
          readNodeStat(transaction, path, StatKey.CZXID)
              .thenAccept(stat -> {
                if (stat.getCzxid() != 0) {
                  watcher.process(new WatchedEvent(EventType.NodeCreated,
                      KeeperState.SyncConnected, path));
                } else {
                  fdbWatchManager.addNodeCreatedWatch(transaction, path, watcher, zkRequest.sessionId);
                }
              })
      );
    }

    for (String path : setWatches.getChildWatches()) {
      backfilledWatches.add(
          readNodeStat(transaction, path, StatKey.PZXID)
              .thenAccept(stat -> {

                if (stat.getPzxid() == 0) {
                  watcher.process(new WatchedEvent(EventType.NodeDeleted,
                      KeeperState.SyncConnected, path));
                } else if (stat.getPzxid() > setWatches.getRelativeZxid()) {
                  watcher.process(new WatchedEvent(EventType.NodeChildrenChanged,
                      KeeperState.SyncConnected, path));
                } else {
                  fdbWatchManager.addNodeChildrenWatch(transaction, path, watcher, zkRequest.sessionId);
                }
              })
      );
    }

    return AsyncUtil.whenAll(backfilledWatches).thenApply(v -> Result.ok(null));
  }

  private CompletableFuture<Stat> readNodeStat(Transaction transaction, String zkPath, StatKey ... statKeys) {
    List<String> fdbPath = FdbPath.toFdbPath(zkPath);

    return DirectoryLayer.getDefault().open(transaction, fdbPath)
        .thenCompose(nodeSubspace -> fdbNodeReader.getNodeStat(nodeSubspace, transaction, statKeys));
  }

}
