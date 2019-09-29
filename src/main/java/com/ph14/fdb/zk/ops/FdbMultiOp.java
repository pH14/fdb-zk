package com.ph14.fdb.zk.ops;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.CheckResult;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.OpResult.DeleteResult;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;

/**
 * Not functional yet... many of these ops need atomic mutations
 * which aren't read-your-writes friendly.
 *
 * Tracking: https://github.com/pH14/fdb-zk/issues/15
 */
public class FdbMultiOp {

  private static final Logger LOG = LoggerFactory.getLogger(FdbMultiOp.class);

  private final Database fdb;
  private final FdbCreateOp fdbCreateOp;
  private final FdbDeleteOp fdbDeleteOp;
  private final FdbSetDataOp fdbSetDataOp;
  private final FdbCheckVersionOp fdbCheckVersionOp;

  @Inject
  public FdbMultiOp(Database fdb,
                    FdbCreateOp fdbCreateOp,
                    FdbDeleteOp fdbDeleteOp,
                    FdbSetDataOp fdbSetDataOp,
                    FdbCheckVersionOp fdbCheckVersionOp) {
    this.fdb = fdb;
    this.fdbCreateOp = fdbCreateOp;
    this.fdbDeleteOp = fdbDeleteOp;
    this.fdbSetDataOp = fdbSetDataOp;
    this.fdbCheckVersionOp = fdbCheckVersionOp;
  }

  public MultiResponse execute(Request zkRequest, MultiTransactionRecord multiTransactionRecord) {
    List<CompletableFuture<Result<OpResult, KeeperException>>> results = new ArrayList<>();

    try {
      fdb.run(transaction -> {
        Iterator<Op> ops = multiTransactionRecord.iterator();

        while (ops.hasNext()) {
          Op op = ops.next();

          switch (op.getType()) {
            case OpCode.create:
              results.add(
                  fdbCreateOp.execute(zkRequest, transaction, (CreateRequest) op.toRequestRecord())
                      .thenApply(r -> r.mapOk(response -> new CreateResult(response.getPath()))));
              break;
            case OpCode.delete:
              results.add(
                  fdbDeleteOp.execute(zkRequest, transaction, (DeleteRequest) op.toRequestRecord())
                      .thenApply(r -> r.mapOk(response -> new DeleteResult())));
              break;
            case OpCode.setData:
              results.add(
                  fdbSetDataOp.execute(zkRequest, transaction, (SetDataRequest) op.toRequestRecord())
                      .thenApply(r -> r.mapOk(response -> new SetDataResult(response.getStat()))));
              break;
            case OpCode.check:
              results.add(
                  fdbCheckVersionOp.execute(zkRequest, transaction, (CheckVersionRequest) op.toRequestRecord())
                      .thenApply(r -> r.mapOk(response -> new CheckResult())));
              break;
          }
        }

        for (CompletableFuture<Result<OpResult, KeeperException>> result : results) {
          Result<OpResult, KeeperException> individualResult = result.join();
          if (individualResult.isErr()) {
            throw new AbortTransactionException();
          }
        }

        return null;
      });
    } catch (Exception e) {
      if (e.getCause() instanceof AbortTransactionException) {
        LOG.debug("Aborting transaction");
      } else {
        throw e;
      }
    }

    MultiResponse multiResponse = new MultiResponse();
    for (CompletableFuture<Result<OpResult, KeeperException>> result : results) {
      Result<OpResult, KeeperException> individualResult = result.join();

      if (individualResult.isErr()) {
        multiResponse.add(new ErrorResult(individualResult.unwrapErrOrElseThrow().code().intValue()));
      } else {
        multiResponse.add(individualResult.unwrapOrElseThrow());
      }
    }

    return multiResponse;
  }


  private static class AbortTransactionException extends RuntimeException {
    public AbortTransactionException() {
    }
  }

}
