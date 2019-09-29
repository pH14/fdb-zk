package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.APIErrorException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.hubspot.algebra.Results;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbPath;

public class FdbCheckVersionOp implements FdbOp<CheckVersionRequest, Void> {

  private final FdbNodeReader fdbNodeReader;

  @Inject
  public FdbCheckVersionOp(FdbNodeReader fdbNodeReader) {
    this.fdbNodeReader = fdbNodeReader;
  }

  @Override
  public CompletableFuture<Result<Void, KeeperException>> execute(Request zkRequest, Transaction transaction, CheckVersionRequest request) {
    List<String> path = FdbPath.toFdbPath(request.getPath());

    final DirectorySubspace subspace;
    final Stat stat;
    try {
      subspace = DirectoryLayer.getDefault().open(transaction, path).join();
      stat = fdbNodeReader.getNodeStat(subspace, transaction).join();

      if (request.getVersion() != -1 && stat.getVersion() != request.getVersion()) {
        return CompletableFuture.completedFuture(Result.err(new BadVersionException(request.getPath())));
      }
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return CompletableFuture.completedFuture(Results.err(new NoNodeException(request.getPath())));
      } else {
        return CompletableFuture.completedFuture(Results.err(new APIErrorException()));
      }
    }

     return CompletableFuture.completedFuture(Result.ok(null));
  }

}
