package com.ph14.fdb.zk.ops;

import org.apache.zookeeper.KeeperException;

import com.apple.foundationdb.Transaction;
import com.hubspot.algebra.Result;

public abstract class BaseFdbOp<REQ, RESP> {

  protected final Transaction transaction;
  protected final REQ request;

  public BaseFdbOp(Transaction transaction, REQ request) {
    this.transaction = transaction;
    this.request = request;
  }

  public abstract Result<RESP, KeeperException> execute();

}
