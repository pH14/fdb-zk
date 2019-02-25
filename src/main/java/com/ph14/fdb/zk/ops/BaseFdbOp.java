package com.ph14.fdb.zk.ops;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.hubspot.algebra.Result;

public interface BaseFdbOp<REQ, RESP> {

  Result<RESP, KeeperException> execute(Request zkRequest, Transaction transaction, REQ request);

}
