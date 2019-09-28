package com.ph14.fdb.zk;

import java.io.IOException;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SessionTracker;
import org.apache.zookeeper.server.ZKDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hubspot.algebra.Result;

public class FdbRequestProcessor implements RequestProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FdbRequestProcessor.class);

  private final FdbZooKeeperImpl fdbZooKeeper;
  private final SessionTracker sessionTracker;
  private final ZKDatabase zkDatabase;
  private final RequestProcessor defaultRequestProcessor;

  public FdbRequestProcessor(SessionTracker sessionTracker,
                             RequestProcessor defaultRequestProcessor,
                             ZKDatabase zkDatabase,
                             FdbZooKeeperImpl fdbZooKeeper) {
    this.sessionTracker = sessionTracker;
    this.defaultRequestProcessor = defaultRequestProcessor;
    this.zkDatabase = zkDatabase;
    this.fdbZooKeeper = fdbZooKeeper;
  }

  @Override
  public void processRequest(Request request) throws RequestProcessorException {
    LOG.info("Received request in Fdb Request Processor: {}", request);

    if (fdbZooKeeper.handlesRequest(request)) {
      try {
        sendResponse(request, fdbZooKeeper.handle(request));
      } catch (IOException e) {
        LOG.error("Failed to process request {}", request, e);
      }
    } else {
      // we don't need to intercept everything
      defaultRequestProcessor.processRequest(request);
    }
  }

  private void sendResponse(Request request,
                            Result<?, KeeperException> fdbResult) {
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
      LOG.error("Error: {}", fdbResult.unwrapErrOrElseThrow());
      result = null;
      errorCode = fdbResult.unwrapErrOrElseThrow().code().intValue();
    } else {
      LOG.error("What is going on send help. Request: {}", request);
    }

    ReplyHeader hdr = new ReplyHeader(request.cxid, System.currentTimeMillis(), errorCode);

    // need a real solution here, something that replicates zkDatabase all the way down
    zkDatabase.setlastProcessedZxid(Long.MAX_VALUE);

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
