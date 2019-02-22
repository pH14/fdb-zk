package com.ph14.fdb.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.server.WatchManager;

import com.apple.foundationdb.Database;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.ops.FdbCreate;
import com.ph14.fdb.zk.ops.FdbExists;
import com.ph14.fdb.zk.ops.FdbGetData;

public class FdbZooKeeperImpl implements FdbZooKeeperLayer {

  private final Database fdb;
  private final WatchManager watchManager;

  @Inject
  public FdbZooKeeperImpl(Database fdb) {
    this.fdb = fdb;
    this.watchManager = new WatchManager();
  }

  @Override
  public Result<ExistsResponse, KeeperException> exists(ExistsRequest existsRequest) {
    return fdb.run(tr -> new FdbExists(tr, existsRequest).execute());
  }

  @Override
  public Result<CreateResponse, KeeperException> create(CreateRequest createRequest) {
    return fdb.run(tr -> new FdbCreate(tr, createRequest).execute());
  }

  @Override
  public Result<GetDataResponse, KeeperException> getData(GetDataRequest getDataRequest) {
    return fdb.run(tr -> new FdbGetData(tr, getDataRequest).execute());
  }

}
