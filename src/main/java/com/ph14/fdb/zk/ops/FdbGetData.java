package com.ph14.fdb.zk.ops;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.collect.ImmutableList;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbNodeReader;

public class FdbGetData extends BaseFdbOp<GetDataRequest, GetDataResponse> {

  public FdbGetData(Transaction transaction, GetDataRequest request) {
    super(transaction, request);
  }

  @Override
  public Result<GetDataResponse, KeeperException> execute() {
    List<String> path = ImmutableList.copyOf(request.getPath().split("/"));

    final DirectorySubspace subspace;
    try {
      subspace = DirectoryLayer.getDefault().open(transaction, path).join();
    } catch (NoSuchDirectoryException e) {
      return Result.err(new NoNodeException(request.getPath()));
    }

    byte[] data = new FdbNodeReader(subspace).deserialize(transaction).getData();

    return Result.ok(new GetDataResponse(data, new Stat()));
  }

}
