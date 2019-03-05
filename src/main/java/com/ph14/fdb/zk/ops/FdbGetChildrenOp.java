package com.ph14.fdb.zk.ops;

import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;

public class FdbGetChildrenOp implements FdbOp<GetChildrenRequest, GetChildrenResponse> {

  private final FdbGetChildrenWithStatOp fdbGetChildrenWithStatOp;

  @Inject
  public FdbGetChildrenOp(FdbGetChildrenWithStatOp fdbGetChildrenWithStatOp) {
    this.fdbGetChildrenWithStatOp = fdbGetChildrenWithStatOp;
  }

  @Override
  public CompletableFuture<Result<GetChildrenResponse, KeeperException>> execute(Request zkRequest, Transaction transaction, GetChildrenRequest request) {
    return fdbGetChildrenWithStatOp.execute(zkRequest, transaction, new GetChildren2Request(request.getPath(), request.getWatch()))
        .thenApply(result -> result.mapOk(getChildren2Response -> new GetChildrenResponse(getChildren2Response.getChildren())));
  }

}
