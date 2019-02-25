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
    byte[] data = "some string thing".getBytes();
    long timeBeforeExecution = System.currentTimeMillis();

    Result<CreateResponse, KeeperException> result = fdbCreate.execute(REQUEST, transaction, new CreateRequest(BASE_PATH, data, Collections.emptyList(), 0));
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    Result<ExistsResponse, KeeperException> exists = fdbExists.execute(REQUEST, transaction, new ExistsRequest(BASE_PATH, false));
    assertThat(exists.isOk()).isTrue();
    assertThat(exists.unwrapOrElseThrow().getStat()).isNotNull();

    Stat stat = exists.unwrapOrElseThrow().getStat();

    assertThat(stat.getCzxid()).isGreaterThan(0L);
    assertThat(stat.getMzxid()).isGreaterThan(0L);
    assertThat(stat.getCtime()).isGreaterThanOrEqualTo(timeBeforeExecution);
    assertThat(stat.getMtime()).isGreaterThanOrEqualTo(timeBeforeExecution);
    assertThat(stat.getVersion()).isEqualTo(1);
    assertThat(stat.getCversion()).isEqualTo(1);
    assertThat(stat.getDataLength()).isEqualTo(data.length);
  }

  @Test
  public void itReturnsErrorIfNodeDoesNotExist() {
    Result<ExistsResponse, KeeperException> exists = fdbExists.execute(REQUEST, transaction, new ExistsRequest(BASE_PATH, false));
    assertThat(exists.isOk()).isFalse();
    assertThat(exists.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

}
