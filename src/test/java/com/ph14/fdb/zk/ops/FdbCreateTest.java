package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.junit.Test;

import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbCreateTest extends FdbBaseTest {

  @Test
  public void itCreatesADirectory() {
    Result<CreateResponse, KeeperException> result = new FdbCreate(REQUEST, transaction, new CreateRequest(BASE_PATH,  new byte[0], Collections.emptyList(), 0)).execute();
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));
  }

  @Test
  public void itDoesNotCreateTheSameDirectoryTwice() {
    Result<CreateResponse, KeeperException> result = new FdbCreate(REQUEST, transaction, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0)).execute();
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    result = new FdbCreate(REQUEST, transaction, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0)).execute();
    assertThat(result.isOk()).isFalse();
    assertThat(result.unwrapErrOrElseThrow().code()).isEqualTo(Code.NODEEXISTS);
  }

  @Test
  public void itDoesNotCreateDirectoryWithoutParent() {
    Result<CreateResponse, KeeperException> result = new FdbCreate(REQUEST, transaction, new CreateRequest(SUBPATH, new byte[0], Collections.emptyList(), 0)).execute();
    assertThat(result.isErr()).isTrue();
    assertThat(result.unwrapErrOrElseThrow().code()).isEqualTo(Code.NONODE);
  }

  @Test
  public void itProgressivelyCreatesNodes() {
    Result<CreateResponse, KeeperException> result = new FdbCreate(REQUEST, transaction, new CreateRequest(BASE_PATH, new byte[0], Collections.emptyList(), 0)).execute();
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(BASE_PATH));

    result = new FdbCreate(REQUEST, transaction, new CreateRequest(SUBPATH, new byte[0], Collections.emptyList(), 0)).execute();
    assertThat(result.isOk()).isTrue();
    assertThat(result.unwrapOrElseThrow()).isEqualTo(new CreateResponse(SUBPATH));
  }

}
