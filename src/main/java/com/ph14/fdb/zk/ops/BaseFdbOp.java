package com.ph14.fdb.zk.ops;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.hubspot.algebra.Result;

public abstract class BaseFdbOp<REQ, RESP> {

  protected final Request rawRequest;
  protected final Transaction transaction;
  protected final REQ request;

  public BaseFdbOp(Request rawRequest, Transaction transaction, REQ request) {
    this.rawRequest = rawRequest;
    this.transaction = transaction;
    this.request = request;
  }

  public abstract Result<RESP, KeeperException> execute();

}
