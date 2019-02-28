package com.ph14.fdb.zk;

import java.util.Arrays;
import java.util.Set;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
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

public class FdbZooKeeperImpl implements FdbZooKeeperLayer {

  private static final Set<Integer> FDB_SUPPORTED_OPCODES = ImmutableSet.<Integer>builder()
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

  public Result<? extends Record, KeeperException> handle(Request request) {
    Preconditions.checkArgument(handlesRequest(request), "does not handle request: " + request);

    if (request.type == OpCode.create) {

    }

    return Result.ok(null);
  }

  // do we want something like ResponseWithWatch, RequestWithRawRequest as the inputs?

  @Override
  public Result<ExistsResponse, KeeperException> exists(Request zkRequest, ExistsRequest existsRequest) {
    return fdb.run(tr -> fdbExistsOp.execute(zkRequest, tr, existsRequest));
  }

  @Override
  public Result<CreateResponse, KeeperException> create(Request zkRequest, CreateRequest createRequest) {
    return fdb.run(tr -> fdbCreateOp.execute(zkRequest, tr, createRequest));
  }

  @Override
  public Result<GetDataResponse, KeeperException> getData(Request zkRequest, GetDataRequest getDataRequest) {
    return fdb.run(tr -> fdbGetDataOp.execute(zkRequest, tr, getDataRequest));
  }

}
