package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbSchemaConstants;
import com.ph14.fdb.zk.layer.FdbNode;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbWatchManager;

public class FdbExistsOp implements BaseFdbOp<ExistsRequest, ExistsResponse> {

  private final FdbNodeReader fdbNodeReader;
  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbExistsOp(FdbNodeReader fdbNodeReader,
                     FdbWatchManager fdbWatchManager) {
    this.fdbNodeReader = fdbNodeReader;
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public CompletableFuture<Result<ExistsResponse, KeeperException>> execute(Request zkRequest, Transaction transaction, ExistsRequest request) {
    List<String> path = ImmutableList.copyOf(request.getPath().split("/"));

    final DirectorySubspace subspace;
    try {
      subspace = DirectoryLayer.getDefault().open(transaction, path).join();

      Range statKeyRange = subspace.get(FdbSchemaConstants.STAT_KEY).range();

      FdbNode fdbNode = fdbNodeReader.getNode(subspace, transaction.getRange(statKeyRange).asList().join());

      if (request.getWatch()) {
        fdbWatchManager.addNodeDataUpdatedWatch(transaction, request.getPath(), zkRequest.cnxn);
        fdbWatchManager.addNodeDeletedWatch(transaction, request.getPath(), zkRequest.cnxn);
      }

      return CompletableFuture.completedFuture(Result.ok(new ExistsResponse(fdbNode.getStat())));
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        if (request.getWatch()) {
          fdbWatchManager.addNodeCreatedWatch(transaction, request.getPath(), zkRequest.cnxn);
        }

        return CompletableFuture.completedFuture(Result.err(new NoNodeException(request.getPath())));
      } else {
        throw new RuntimeException(e);
      }
    }
  }

}
