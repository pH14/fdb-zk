package com.ph14.fdb.zk.ops;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbNode;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbWatchManager;

public class FdbCreateOp implements BaseFdbOp<CreateRequest, CreateResponse> {

  private final FdbNodeWriter fdbNodeWriter;
  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbCreateOp(FdbNodeWriter fdbNodeWriter,
                     FdbWatchManager fdbWatchManager) {
    this.fdbNodeWriter = fdbNodeWriter;
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public CompletableFuture<Result<CreateResponse, KeeperException>> execute(Request zkRequest, Transaction transaction, CreateRequest request) {
    FdbNode fdbNode = new FdbNode(request.getPath(), null, request.getData(), request.getAcl());

    if (fdbNode.getSplitPath().size() > 1) {
      boolean parentExists = DirectoryLayer.getDefault().exists(transaction, fdbNode.getSplitPath().subList(0, fdbNode.getSplitPath().size() - 1)).join();

      if (!parentExists) {
        return CompletableFuture.completedFuture(Result.err(new NoNodeException(request.getPath())));
      }
    }

    try {
      DirectorySubspace subspace = DirectoryLayer.getDefault().create(transaction, fdbNode.getSplitPath()).join();

      fdbNodeWriter.createNewNode(transaction, subspace, fdbNode);

      fdbWatchManager.triggerNodeCreatedWatch(transaction, request.getPath());
    } catch (CompletionException e) {
      if (e.getCause() instanceof DirectoryAlreadyExistsException) {
        return CompletableFuture.completedFuture(Result.err(new NodeExistsException(request.getPath())));
      } else {
        throw new RuntimeException(e);
      }
    }

    return CompletableFuture.completedFuture(Result.ok(new CreateResponse(request.getPath())));
  }

}
