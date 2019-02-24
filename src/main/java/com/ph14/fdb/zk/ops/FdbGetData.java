package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.collect.ImmutableList;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbWatchManager;

public class FdbGetData extends BaseFdbOp<GetDataRequest, GetDataResponse> {

  public FdbGetData(Request rawRequest, Transaction transaction, GetDataRequest request) {
    super(rawRequest, transaction, request);
  }

  @Override
  public Result<GetDataResponse, KeeperException> execute() {
    List<String> path = ImmutableList.copyOf(request.getPath().split("/"));

    final DirectorySubspace subspace;
    try {
      subspace = DirectoryLayer.getDefault().open(transaction, path).join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return Result.err(new NoNodeException(request.getPath()));
      } else {
        throw new RuntimeException(e);
      }
    }

    byte[] data = new FdbNodeReader(subspace).deserialize(transaction).getData();

    if (request.getWatch()) {
      new FdbWatchManager().addNodeDataUpdatedWatch(transaction, subspace, rawRequest.cnxn);
    }

    return Result.ok(new GetDataResponse(data, new Stat()));
  }

}
