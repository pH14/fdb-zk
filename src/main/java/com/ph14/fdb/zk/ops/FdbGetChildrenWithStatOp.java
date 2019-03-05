package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.FdbWatchManager;

public class FdbGetChildrenWithStatOp implements FdbOp<GetChildren2Request, GetChildren2Response> {

  private final FdbNodeReader fdbNodeReader;
  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbGetChildrenWithStatOp(FdbNodeReader fdbNodeReader,
                                  FdbWatchManager fdbWatchManager) {
    this.fdbNodeReader = fdbNodeReader;
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public CompletableFuture<Result<GetChildren2Response, KeeperException>> execute(Request zkRequest, Transaction transaction, GetChildren2Request request) {
    List<String> path = FdbPath.toFdbPath(request.getPath());

    try {
      Stat stat = fdbNodeReader.getNodeStat(DirectoryLayer.getDefault().open(transaction, path).join(), transaction).join();
      List<String> childrenDirectoryNames = DirectoryLayer.getDefault().list(transaction, path).join();

      if (request.getWatch()) {
        fdbWatchManager.addNodeChildrenWatch(transaction, request.getPath(), zkRequest.cnxn);
        fdbWatchManager.addNodeDeletedWatch(transaction, request.getPath(), zkRequest.cnxn);
      }

      return CompletableFuture.completedFuture(Result.ok(new GetChildren2Response(childrenDirectoryNames, stat)));
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return CompletableFuture.completedFuture(Result.err(new NoNodeException(request.getPath())));
      } else {
        throw new RuntimeException(e);
      }
    }
  }

}
