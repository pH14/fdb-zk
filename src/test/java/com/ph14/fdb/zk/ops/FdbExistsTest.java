package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.junit.Test;

import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbExistsTest extends FdbBaseTest {

  @Test
  public void itFindsStatOfExistingNode() {
    Result<CreateResponse, KeeperException> result = new FdbCreate(REQUEST, transaction, new CreateRequest(BASE_PATH,  new byte[0], Collections.emptyList(), 0)).execute();
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<ExistsResponse, KeeperException> exists = new FdbExists(REQUEST, transaction, new ExistsRequest(BASE_PATH, false)).execute();
    assertThat(exists.isOk()).isTrue();
    assertThat(exists.unwrapOrElseThrow().getStat()).isEqualTo(new Stat());
  }

  @Test
  public void itReturnsErrorIfNodeDoesNotExist() {
    Result<ExistsResponse, KeeperException> exists = new FdbExists(REQUEST, transaction, new ExistsRequest(BASE_PATH, false)).execute();
    assertThat(exists.isOk()).isFalse();
    assertThat(exists.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

}
