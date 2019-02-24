package com.ph14.fdb.zk.ops;

import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbNode;
import com.ph14.fdb.zk.layer.FdbNodeWriter;

public class FdbCreate extends BaseFdbOp<CreateRequest, CreateResponse> {

  // TODO: Write a Stat into the value

  public FdbCreate(Request rawRequest, Transaction transaction, CreateRequest request) {
    super(rawRequest, transaction, request);
  }

  @Override
  public Result<CreateResponse, KeeperException> execute() {
    FdbNode fdbNode = new FdbNode(request.getPath(), new StatPersisted(), request.getData(), request.getAcl());

    if (fdbNode.getSplitPath().size() > 1) {
      boolean parentExists = DirectoryLayer.getDefault().exists(transaction, fdbNode.getSplitPath().subList(0, fdbNode.getSplitPath().size() - 1)).join();

      if (!parentExists) {
        return Result.err(new NoNodeException(request.getPath()));
      }
    }

    try {
      DirectorySubspace subspace = DirectoryLayer.getDefault().create(transaction, fdbNode.getSplitPath()).join();

      new FdbNodeWriter(subspace).serialize(fdbNode)
          .forEach(kv -> transaction.set(kv.getKey(), kv.getValue()));
    } catch (CompletionException e) {
      if (e.getCause() instanceof DirectoryAlreadyExistsException) {
        return Result.err(new NodeExistsException(request.getPath()));
      } else {
        throw new RuntimeException(e);
      }
    }

    return Result.ok(new CreateResponse(request.getPath()));
  }

}
