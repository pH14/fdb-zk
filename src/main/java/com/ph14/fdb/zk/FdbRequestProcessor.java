package com.ph14.fdb.zk;

import java.io.IOException;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.hubspot.algebra.Result;

public class FdbRequestProcessor implements RequestProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FdbRequestProcessor.class);

  private final FdbZooKeeperImpl fdbZooKeeper;
  private final ZooKeeperServer zooKeeperServer;
  private final RequestProcessor defaultRequestProcessor;

  public FdbRequestProcessor(RequestProcessor defaultRequestProcessor,
                             ZooKeeperServer zooKeeperServer,
                             FdbZooKeeperImpl fdbZooKeeper) {
    this.defaultRequestProcessor = defaultRequestProcessor;
    this.zooKeeperServer = zooKeeperServer;
    this.fdbZooKeeper = fdbZooKeeper;
  }

  @Override
  public void processRequest(Request request) {
    LOG.info("Received request in Fdb Request Processor: {}", request);

    Preconditions.checkState(
        fdbZooKeeper.handlesRequest(request),
        String.format("given unprocessable request type %s", request.type));

    try {
      sendResponse(request, fdbZooKeeper.handle(request));
    } catch (IOException e) {
      LOG.error("Failed to process request {}", request, e);
    }
  }

  private void sendResponse(Request request,
                            Result<?, KeeperException> fdbResult) {
    if (request.type == OpCode.createSession) {
      zooKeeperServer.finishSessionInit(request.cnxn, true);
      return;
    }

    Record result = null;
    int errorCode = Code.OK.intValue();

    if (fdbResult.isOk()) {
      Object o = fdbResult.unwrapOrElseThrow();

      if (o instanceof Record) {
        result = (Record) o;
      }

      // note: it's OK for result to be null in the case of some operations like setWatches

      errorCode = Code.OK.intValue();
    } else if (fdbResult.isErr()) {
      LOG.error("Error: ", fdbResult.unwrapErrOrElseThrow());
      result = null;
      errorCode = fdbResult.unwrapErrOrElseThrow().code().intValue();
    } else {
      LOG.error("What is going on send help. Request: {}", request);
    }

    ReplyHeader hdr = new ReplyHeader(request.cxid, System.currentTimeMillis(), errorCode);

    try {
      LOG.debug("Returning: {}", result);
      request.cnxn.sendResponse(hdr, result, "response");
    } catch (IOException e) {
      LOG.error("FIXMSG",e);
    }
  }

  public void shutdown() {

  }
}
