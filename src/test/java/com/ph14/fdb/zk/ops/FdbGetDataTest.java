package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.junit.Test;

import com.google.common.base.Strings;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbGetDataTest extends FdbBaseTest {

  @Test
  public void itGetsDataFromExistingNode() {
    String data = Strings.repeat("this is the song that never ends ", 10000);

    Result<CreateResponse, KeeperException> result = new FdbCreate(REQUEST, transaction, new CreateRequest(BASE_PATH,  data.getBytes(), Collections.emptyList(), 0)).execute();
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<GetDataResponse, KeeperException> result2 = new FdbGetData(REQUEST, transaction, new GetDataRequest(BASE_PATH, false)).execute();
    assertThat(result2.isOk()).isTrue();
    assertThat(result2.unwrapOrElseThrow()).isEqualTo(new GetDataResponse(data.getBytes(), new Stat()));
  }

  @Test
  public void itReturnsErrorIfNodeDoesNotExist() {
    Result<GetDataResponse, KeeperException> exists = new FdbGetData(REQUEST, transaction, new GetDataRequest(BASE_PATH, false)).execute();
    assertThat(exists.isOk()).isFalse();
    assertThat(exists.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

}
