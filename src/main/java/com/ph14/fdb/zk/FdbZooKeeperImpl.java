package com.ph14.fdb.zk;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.txn.CreateSessionTxn;

import com.apple.foundationdb.Database;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.ops.FdbCheckVersionOp;
import com.ph14.fdb.zk.ops.FdbCreateOp;
import com.ph14.fdb.zk.ops.FdbDeleteOp;
import com.ph14.fdb.zk.ops.FdbExistsOp;
import com.ph14.fdb.zk.ops.FdbGetChildrenOp;
import com.ph14.fdb.zk.ops.FdbGetChildrenWithStatOp;
import com.ph14.fdb.zk.ops.FdbGetDataOp;
import com.ph14.fdb.zk.ops.FdbMultiOp;
import com.ph14.fdb.zk.ops.FdbSetDataOp;
import com.ph14.fdb.zk.ops.FdbSetWatchesOp;
import com.ph14.fdb.zk.session.FdbSessionManager;

public class FdbZooKeeperImpl {

  private static final Set<Integer> OPS_WITHOUT_SESSION_CHECK = ImmutableSet.<Integer>builder()
      .add(OpCode.createSession)
      .add(OpCode.closeSession)
      .build();

  private static final Set<Integer> FDB_SUPPORTED_OPCODES = ImmutableSet.<Integer>builder()
      .addAll(
          Arrays.asList(
              OpCode.create,
              OpCode.delete,
              OpCode.setData,
              OpCode.setACL,
              OpCode.multi,
              OpCode.exists,
              OpCode.getData,
              OpCode.getACL,
              OpCode.getChildren,
              OpCode.getChildren2, // includes stat of node
              OpCode.setWatches,
              OpCode.multi,
              OpCode.check,

              OpCode.sync,
              OpCode.ping,

              OpCode.createSession,
              OpCode.closeSession
          )
      )
      .build();

  private final Database fdb;
  private final FdbCreateOp fdbCreateOp;
  private final FdbCheckVersionOp fdbCheckVersionOp;
  private final FdbExistsOp fdbExistsOp;
  private final FdbGetDataOp fdbGetDataOp;
  private final FdbSetDataOp fdbSetDataOp;
  private final FdbGetChildrenOp fdbGetChildrenOp;
  private final FdbGetChildrenWithStatOp fdbGetChildrenWithStatOp;
  private final FdbDeleteOp fdbDeleteOp;
  private final FdbSetWatchesOp fdbSetWatchesOp;
  private final FdbMultiOp fdbMultiOp;

  private final FdbSessionManager fdbSessionManager;

  @Inject
  public FdbZooKeeperImpl(Database fdb,
                          FdbSessionManager fdbSessionManager,
                          FdbCreateOp fdbCreateOp,
                          FdbCheckVersionOp fdbCheckVersionOp,
                          FdbExistsOp fdbExistsOp,
                          FdbGetDataOp fdbGetDataOp,
                          FdbSetDataOp fdbSetDataOp,
                          FdbGetChildrenOp fdbGetChildrenOp,
                          FdbGetChildrenWithStatOp fdbGetChildrenWithStatOp,
                          FdbDeleteOp fdbDeleteOp,
                          FdbSetWatchesOp fdbSetWatchesOp,
                          FdbMultiOp fdbMultiOp) {
    this.fdb = fdb;
    this.fdbSessionManager = fdbSessionManager;
    this.fdbCreateOp = fdbCreateOp;
    this.fdbCheckVersionOp = fdbCheckVersionOp;
    this.fdbExistsOp = fdbExistsOp;
    this.fdbGetDataOp = fdbGetDataOp;
    this.fdbSetDataOp = fdbSetDataOp;
    this.fdbGetChildrenOp = fdbGetChildrenOp;
    this.fdbGetChildrenWithStatOp = fdbGetChildrenWithStatOp;
    this.fdbDeleteOp = fdbDeleteOp;
    this.fdbSetWatchesOp = fdbSetWatchesOp;
    this.fdbMultiOp = fdbMultiOp;
  }

  public boolean handlesRequest(Request request) {
    return FDB_SUPPORTED_OPCODES.contains(request.type);
  }

  public Result<?, KeeperException> handle(Request request) throws IOException {
    Preconditions.checkArgument(handlesRequest(request), "does not handle request: " + request);

    if (!OPS_WITHOUT_SESSION_CHECK.contains(request.type)) {
      try {
        fdbSessionManager.checkSession(request.sessionId, null);
      } catch (SessionExpiredException | SessionMovedException e) {
        return Result.err(e);
      }
    }

    switch (request.type) {
      case OpCode.create:
        CreateRequest create2Request = new CreateRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, create2Request);
        return fdb.run(tr -> fdbCreateOp.execute(request, tr, create2Request)).join();

      case OpCode.exists:
        ExistsRequest existsRequest = new ExistsRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, existsRequest);
        return fdb.run(tr -> fdbExistsOp.execute(request, tr, existsRequest)).join();

      case OpCode.delete:
        DeleteRequest deleteRequest = new DeleteRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, deleteRequest);
        return fdb.run(tr -> fdbDeleteOp.execute(request, tr, deleteRequest))
            .thenApply(r -> r.mapOk(success -> deleteRequest))
            .join();

      case OpCode.getData:
        GetDataRequest getDataRequest = new GetDataRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, getDataRequest);
        return fdb.run(tr -> fdbGetDataOp.execute(request, tr, getDataRequest)).join();

      case OpCode.getChildren:
        GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, getChildrenRequest);
        return fdb.run(tr -> fdbGetChildrenOp.execute(request, tr, getChildrenRequest)).join();

      case OpCode.getChildren2:
        GetChildren2Request getChildren2Request = new GetChildren2Request();
        ByteBufferInputStream.byteBuffer2Record(request.request, getChildren2Request);
        return fdb.run(tr -> fdbGetChildrenWithStatOp.execute(request, tr, getChildren2Request)).join();

      case OpCode.setData:
        SetDataRequest setDataRequest = new SetDataRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, setDataRequest);
        return fdb.run(tr -> fdbSetDataOp.execute(request, tr, setDataRequest)).join();

      case OpCode.check:
        CheckVersionRequest checkVersionRequest = new CheckVersionRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, checkVersionRequest);
        return fdb.run(tr -> fdbCheckVersionOp.execute(request, tr, checkVersionRequest)).join();

      case OpCode.setACL:
        SetACLRequest setACLRequest = new SetACLRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, setACLRequest);
        throw new UnsupportedOperationException("not there yet");

      case OpCode.setWatches:
        SetWatches setWatches = new SetWatches();
        ByteBufferInputStream.byteBuffer2Record(request.request, setWatches);
        return fdb.run(tr -> fdbSetWatchesOp.execute(request, tr, setWatches)).join();

      case OpCode.createSession:
        request.request.rewind();
        int to = request.request.getInt();
        request.txn = new CreateSessionTxn(to);
        fdbSessionManager.addSession(request.sessionId, to);
        return Result.ok(true);

      case OpCode.closeSession:
        fdbSessionManager.setSessionClosing(request.sessionId);
        return Result.ok(true);

      case OpCode.multi:
        MultiTransactionRecord multiTransactionRecord = new MultiTransactionRecord();
        ByteBufferInputStream.byteBuffer2Record(request.request, multiTransactionRecord);
        return Result.ok(fdbMultiOp.execute(request, multiTransactionRecord));

      case OpCode.sync: // no-op, fdb won't return stale reads
        SyncRequest syncRequest = new SyncRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, syncRequest);
        return Result.ok(new SyncResponse(syncRequest.getPath()));

      case OpCode.ping:
        return Result.ok(true);
    }

    return Result.err(new KeeperException.BadArgumentsException());
  }

}
