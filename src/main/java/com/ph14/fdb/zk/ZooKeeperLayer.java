package com.ph14.fdb.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.MultiTransactionRecord;
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
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.server.Request;

import com.hubspot.algebra.Result;

public interface ZooKeeperLayer {

  Result<CreateResponse, KeeperException> create(Request zkRequest, CreateRequest createRequest);

  Result<ExistsResponse, KeeperException> exists(Request zkRequest, ExistsRequest existsRequest);

  Result<DeleteRequest, KeeperException> delete(Request zkRequest, DeleteRequest deleteRequest);

  Result<GetDataResponse, KeeperException> getData(Request zkRequest, GetDataRequest getDataRequest);

  Result<SetDataResponse, KeeperException> setData(Request zkRequest, SetDataRequest setDataRequest);

  Result<GetChildrenResponse, KeeperException> getChildren(Request zkRequest, GetChildrenRequest getChildrenRequest);

  Result<GetChildren2Response, KeeperException> getChildrenAndStat(Request zkRequest, GetChildren2Request getChildrenRequest);

  Result<SetACLResponse, KeeperException> setAcl(Request zkRequest, SetACLRequest setACLRequest);

  Result<MultiResponse, KeeperException> multi(Request zkRequest, MultiTransactionRecord multiTransactionRecord);

  Result<Void, KeeperException> setWatches(Request zkRequest, SetWatches setWatches);

}
