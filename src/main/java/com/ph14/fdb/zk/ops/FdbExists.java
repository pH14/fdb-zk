package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
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

public class FdbExists implements BaseFdbOp<ExistsRequest, ExistsResponse> {

  private final FdbNodeReader fdbNodeReader;

  @Inject
  public FdbExists(FdbNodeReader fdbNodeReader) {
    this.fdbNodeReader = fdbNodeReader;
  }

  @Override
  public Result<ExistsResponse, KeeperException> execute(Request zkRequest, Transaction transaction, ExistsRequest request) {
    List<String> path = ImmutableList.copyOf(request.getPath().split("/"));

    final DirectorySubspace subspace;
    try {
      subspace = DirectoryLayer.getDefault().open(transaction, path).join();

      Range statKeyRange = subspace.get(FdbSchemaConstants.STAT_KEY).range();

      FdbNode fdbNode = fdbNodeReader.deserialize(subspace, transaction.getRange(statKeyRange).asList().join());

      return Result.ok(new ExistsResponse(fdbNode.getStat()));
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return Result.err(new NoNodeException(request.getPath()));
      } else {
        throw new RuntimeException(e);
      }
    }
  }

}
