package com.ph14.fdb.zk;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Database;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.ops.FdbCreateOp;
import com.ph14.fdb.zk.ops.FdbExistsOp;
import com.ph14.fdb.zk.ops.FdbGetDataOp;
import com.ph14.fdb.zk.ops.FdbSetDataOp;

public class FdbZooKeeperImpl implements ZooKeeperLayer {

  private static final Set<Integer> FDB_SUPPORTED_OPCODES = ImmutableSet.<Integer>builder()
      .addAll(
          Arrays.asList(
              OpCode.create,
              OpCode.delete,
              OpCode.setData,
              OpCode.setACL,
//              OpCode.check, // does the client every pass this in?
              OpCode.multi,
              OpCode.exists,
              OpCode.getData,
              OpCode.getACL,
              OpCode.getChildren,
              OpCode.getChildren2, // includes stat of node
              OpCode.setWatches)
      )
      .build();

  private final Database fdb;
  private final FdbCreateOp fdbCreateOp;
  private final FdbExistsOp fdbExistsOp;
  private final FdbGetDataOp fdbGetDataOp;
  private final FdbSetDataOp fdbSetDataOp;

  @Inject
  public FdbZooKeeperImpl(Database fdb,
                          FdbCreateOp fdbCreateOp,
                          FdbExistsOp fdbExistsOp,
                          FdbGetDataOp fdbGetDataOp,
                          FdbSetDataOp fdbSetDataOp) {
    this.fdb = fdb;
    this.fdbCreateOp = fdbCreateOp;
    this.fdbExistsOp = fdbExistsOp;
    this.fdbGetDataOp = fdbGetDataOp;
    this.fdbSetDataOp = fdbSetDataOp;
  }

  public boolean handlesRequest(Request request) {
    return FDB_SUPPORTED_OPCODES.contains(request.type);
  }

  public Result<? extends Record, KeeperException> handle(Request request) throws IOException {
    Preconditions.checkArgument(handlesRequest(request), "does not handle request: " + request);

    switch (request.type) {
      case OpCode.create:
        CreateRequest create2Request = new CreateRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, create2Request);
        return create(request, create2Request);

      case OpCode.exists:
        ExistsRequest existsRequest = new ExistsRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, existsRequest);
        return exists(request, existsRequest);

      case OpCode.delete:
        DeleteRequest deleteRequest = new DeleteRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, deleteRequest);
        return delete(request, deleteRequest);

      case OpCode.getData:
        GetDataRequest getDataRequest = new GetDataRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, getDataRequest);
        return getData(request, getDataRequest);

      case OpCode.getChildren:
        GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, getChildrenRequest);
        return getChildren(request, getChildrenRequest);

      case OpCode.getChildren2:
        GetChildren2Request getChildren2Request = new GetChildren2Request();
        ByteBufferInputStream.byteBuffer2Record(request.request, getChildren2Request);
        return getChildrenAndStat(request, getChildren2Request);

      case OpCode.setData:
        SetDataRequest setDataRequest = new SetDataRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, setDataRequest);
        return setData(request, setDataRequest);

      case OpCode.setACL:
        SetACLRequest setACLRequest = new SetACLRequest();
        ByteBufferInputStream.byteBuffer2Record(request.request, setACLRequest);
        return setAcl(request, setACLRequest);

      case OpCode.multi:
        MultiTransactionRecord multiTransactionRecord = new MultiTransactionRecord();
        ByteBufferInputStream.byteBuffer2Record(request.request, multiTransactionRecord);
        return multi(request, multiTransactionRecord);
    }

    return Result.err(new KeeperException.BadArgumentsException());
  }

  @Override
  public Result<ExistsResponse, KeeperException> exists(Request zkRequest, ExistsRequest existsRequest) {
    return fdb.run(tr -> fdbExistsOp.execute(zkRequest, tr, existsRequest)).join();
  }

  @Override
  public Result<CreateResponse, KeeperException> create(Request zkRequest, CreateRequest createRequest) {
    return fdb.run(tr -> fdbCreateOp.execute(zkRequest, tr, createRequest)).join();
  }

  @Override
  public Result<GetDataResponse, KeeperException> getData(Request zkRequest, GetDataRequest getDataRequest) {
    return fdb.run(tr -> fdbGetDataOp.execute(zkRequest, tr, getDataRequest)).join();
  }

  @Override
  public Result<SetDataResponse, KeeperException> setData(Request zkRequest, SetDataRequest setDataRequest) {
    return fdb.run(tr -> fdbSetDataOp.execute(zkRequest, tr, setDataRequest)).join();
  }

  @Override
  public Result<DeleteRequest, KeeperException> delete(Request zkRequest, DeleteRequest deleteRequest) {
    return null;
  }

  @Override
  public Result<GetChildrenResponse, KeeperException> getChildren(Request zkRequest, GetChildrenRequest getChildrenRequest) {
    return null;
  }

  @Override
  public Result<GetChildren2Response, KeeperException> getChildrenAndStat(Request zkRequest, GetChildren2Request getChildrenRequest) {
    return null;
  }

  @Override
  public Result<SetACLResponse, KeeperException> setAcl(Request zkRequest, SetACLRequest setACLRequest) {
    return null;
  }

  @Override
  public Result<MultiResponse, KeeperException> multi(Request zkRequest, MultiTransactionRecord multiTransactionRecord) {
    return null;
  }

}
