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
import com.ph14.fdb.zk.ops.FdbCreate;
import com.ph14.fdb.zk.ops.FdbExists;
import com.ph14.fdb.zk.ops.FdbGetData;
import com.ph14.fdb.zk.ops.FdbSetData;

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
  private final FdbCreate fdbCreate;
  private final FdbExists fdbExists;
  private final FdbGetData fdbGetData;
  private final FdbSetData fdbSetData;

  @Inject
  public FdbZooKeeperImpl(Database fdb,
                          FdbCreate fdbCreate,
                          FdbExists fdbExists,
                          FdbGetData fdbGetData,
                          FdbSetData fdbSetData) {
    this.fdb = fdb;
    this.fdbCreate = fdbCreate;
    this.fdbExists = fdbExists;
    this.fdbGetData = fdbGetData;
    this.fdbSetData = fdbSetData;
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
    return fdb.run(tr -> fdbExists.execute(zkRequest, tr, existsRequest));
  }

  @Override
  public Result<CreateResponse, KeeperException> create(Request zkRequest, CreateRequest createRequest) {
    return fdb.run(tr -> fdbCreate.execute(zkRequest, tr, createRequest));
  }

  @Override
  public Result<GetDataResponse, KeeperException> getData(Request zkRequest, GetDataRequest getDataRequest) {
    return fdb.run(tr -> fdbGetData.execute(zkRequest, tr, getDataRequest));
  }

}
