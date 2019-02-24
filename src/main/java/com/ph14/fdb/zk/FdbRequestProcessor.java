package com.ph14.fdb.zk;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SessionTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.FDB;
import com.google.common.collect.ImmutableSet;
import com.hubspot.algebra.Result;

public class FdbRequestProcessor implements RequestProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FdbRequestProcessor.class);

  private static final Set<Integer> SHIMMED_OPCODES = ImmutableSet.<Integer>builder()
      .addAll(
          Arrays.asList(
              OpCode.create,
              OpCode.delete,
              OpCode.setData,
              OpCode.setACL,
              OpCode.check,
              OpCode.multi,
              OpCode.sync,
              OpCode.exists,
              OpCode.getData,
              OpCode.getACL,
              OpCode.getChildren,
              OpCode.getChildren2,
              OpCode.setWatches)
      )
      .build();

  private final FdbZooKeeperLayer fdbZooKeeper;
  private final SessionTracker sessionTracker;
  private final RequestProcessor defaultRequestProcessor;

  public FdbRequestProcessor(SessionTracker sessionTracker,
                             RequestProcessor defaultRequestProcessor) {
    this.sessionTracker = sessionTracker;
    this.defaultRequestProcessor = defaultRequestProcessor;

    // TODO: Inject
    this.fdbZooKeeper = new FdbZooKeeperImpl(FDB.selectAPIVersion(600).open());
  }

  @Override
  public void processRequest(Request request) throws RequestProcessorException {
    LOG.info("Received request in Fdb Request Processor: {}", request);


    if (SHIMMED_OPCODES.contains(request.type)) {
      LOG.info("OpCode is shimmed, will not pass onto other processors");
    }

    Result<? extends Record, KeeperException> result = null;

    try {
      switch (request.type) {
        case OpCode.create:
          CreateRequest create2Request = new CreateRequest();
          ByteBufferInputStream.byteBuffer2Record(request.request, create2Request);
          request.request.clear();

          result = fdbZooKeeper.create(request, create2Request);

          LOG.debug("Handling create request: {}", create2Request);
          break;
        case OpCode.delete:
          DeleteRequest deleteRequest = new DeleteRequest();
          ByteBufferInputStream.byteBuffer2Record(request.request, deleteRequest);
          LOG.debug("Handling delete request: {}", deleteRequest);
          break;
        case OpCode.setData:
          SetDataRequest setDataRequest = new SetDataRequest();
          ByteBufferInputStream.byteBuffer2Record(request.request, setDataRequest);
          LOG.debug("Handling set data request: {}", setDataRequest);
          break;
        case OpCode.setACL:
          SetACLRequest setAclRequest = new SetACLRequest();
          ByteBufferInputStream.byteBuffer2Record(request.request, setAclRequest);
          break;
        case OpCode.check:
          CheckVersionRequest checkRequest = new CheckVersionRequest();
          ByteBufferInputStream.byteBuffer2Record(request.request, checkRequest);
          break;
        case OpCode.multi:
          MultiTransactionRecord multiRequest = new MultiTransactionRecord();
          ByteBufferInputStream.byteBuffer2Record(request.request, multiRequest);

          break;

          //create/close session don't require request record
        case OpCode.createSession:
        case OpCode.closeSession:
          break;

        //All the rest don't need to create a Txn - just verify session
        case OpCode.sync:
        case OpCode.exists:
          LOG.debug("Handling exists request: {}", request);
//          sendResponse(request);
//          return;
        case OpCode.getData:
          GetDataRequest getDataRequest = new GetDataRequest();
          ByteBufferInputStream.byteBuffer2Record(request.request, getDataRequest);

          result = fdbZooKeeper.getData(request, getDataRequest);
          break;
        case OpCode.getACL:
        case OpCode.getChildren:
        case OpCode.getChildren2:
        case OpCode.ping:
        case OpCode.setWatches:
          sessionTracker.checkSession(request.sessionId, request.getOwner());
          break;
        default:
          LOG.warn("unknown type " + request.type);
          break;
      }
    } catch (KeeperException e) {
      LOG.error("Failed to process with keeper exception" + request, e);
      request.setException(e);
    } catch (Exception e) {
      // log at error level as we are returning a marshalling
      // error to the user
      LOG.error("Failed to process " + request, e);
    }

    request.request.clear();

    if (result != null) {
      sendResponse(request, result);
    } else {
      // we don't need to intercept everything
      defaultRequestProcessor.processRequest(request);
    }
  }

  private void sendResponse(Request request,
                            Result<? extends Record, KeeperException> fdbResult) {
    Record result = null;
    int errorCode = Code.OK.intValue();

    if (fdbResult.isOk()) {
      result = fdbResult.unwrapOrElseThrow();
      errorCode = Code.OK.intValue();
    } else if (fdbResult.isErr()) {
      result = null;
      errorCode = fdbResult.unwrapErrOrElseThrow().code().intValue();
    } else {
      LOG.error("What is going on send help. Request: {}", request);
    }

    ReplyHeader hdr = new ReplyHeader(request.cxid, System.currentTimeMillis(), errorCode);

    try {
      request.cnxn.sendResponse(hdr, result, "response");
    } catch (IOException e) {
      LOG.error("FIXMSG",e);
    }
  }

  public void shutdown() {

  }
}
