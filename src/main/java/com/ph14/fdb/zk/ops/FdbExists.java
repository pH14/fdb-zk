package com.ph14.fdb.zk.ops;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;

import com.apple.foundationdb.Transaction;
import com.hubspot.algebra.Result;

public class FdbExists extends BaseFdbOp<ExistsRequest, ExistsResponse> {

  public FdbExists(Transaction transaction, ExistsRequest request) {
    super(transaction, request);
  }

  @Override
  public Result<ExistsResponse, KeeperException> execute() {
    return null;
  }

}
