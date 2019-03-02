package com.ph14.fdb.zk.ops;

import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.hubspot.algebra.Result;

public interface BaseFdbOp<REQ, RESP> {

  /**
   * Returned future must be joined after Transaction has committed and closed
   */
  CompletableFuture<Result<RESP, KeeperException>> execute(Request zkRequest, Transaction transaction, REQ request);

}
