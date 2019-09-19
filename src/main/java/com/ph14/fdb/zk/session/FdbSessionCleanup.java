package com.ph14.fdb.zk.session;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.async.AsyncUtil;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.changefeed.WatchEventChangefeed;
import com.ph14.fdb.zk.layer.ephemeral.FdbEphemeralNodeManager;

@Singleton
public class FdbSessionCleanup {

  private final Database fdb;
  private final WatchEventChangefeed watchEventChangefeed;
  private final FdbNodeWriter fdbNodeWriter;
  private final FdbNodeReader fdbNodeReader;
  private final FdbEphemeralNodeManager fdbEphemeralNodeManager;

  @Inject
  public FdbSessionCleanup(Database fdb,
                           WatchEventChangefeed watchEventChangefeed,
                           FdbNodeWriter fdbNodeWriter,
                           FdbNodeReader fdbNodeReader,
                           FdbEphemeralNodeManager fdbEphemeralNodeManager) {
    this.fdb = fdb;
    this.watchEventChangefeed = watchEventChangefeed;
    this.fdbNodeWriter = fdbNodeWriter;
    this.fdbNodeReader = fdbNodeReader;
    this.fdbEphemeralNodeManager = fdbEphemeralNodeManager;
  }

  public CompletableFuture<Void> removeAllSessionEntries(long sessionId) {
    return fdb.run(tr -> {
      List<CompletableFuture<Void>> deletions = new ArrayList<>();

      deletions.add(watchEventChangefeed.clearAllWatchesForSession(tr, sessionId));

      for (String zkPath : fdbEphemeralNodeManager.getEphemeralNodeZkPaths(tr, sessionId).join()) {
        deletions.add(
            fdbNodeReader.getNodeDirectory(tr, zkPath)
                .thenCompose(nodeDirectory -> fdbNodeWriter.deleteNodeAsync(tr, nodeDirectory)));
      }

      fdbEphemeralNodeManager.clearEphemeralNodesForSession(tr, sessionId);

      // TODO: clean up session information once it's finished

      return AsyncUtil.whenAll(deletions);
    });
  }

}
