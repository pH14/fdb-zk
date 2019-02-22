package com.ph14.fdb.zk.ops;

import org.apache.zookeeper.KeeperException;

import com.apple.foundationdb.Transaction;
import com.hubspot.algebra.Result;

public interface FdbOp<REQ, RESP> {
  Result<RESP, KeeperException> execute(Transaction tr, REQ request);
}
