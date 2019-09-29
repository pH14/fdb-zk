package com.ph14.fdb.zk.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Strings;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbMultiOpTest extends FdbBaseTest {

  static final String data = "data";

  @Test
  @Ignore
  public void itCreatesManyNodesAtOnce() {
    MultiTransactionRecord ops = new MultiTransactionRecord();
    ops.add(Op.create("/bac", data.getBytes(), Collections.emptyList(), 0));
    ops.add(Op.create("/abc", data.getBytes(), Collections.emptyList(), 0));
    ops.add(Op.create("/cba", data.getBytes(), Collections.emptyList(), 0));

    MultiResponse opResults = fdbMultiOp.execute(REQUEST, ops);
    assertThat(opResults.getResultList())
        .containsExactly(
            new CreateResult("/bac"),
            new CreateResult("/abc"),
            new CreateResult("/cba"));

    List<Long> createVersions = new ArrayList<>();
    fdb.run(tr -> {
      createVersions.add(fdbExistsOp.execute(REQUEST, tr, new ExistsRequest("/bac", false)).join()
          .unwrapOrElseThrow().getStat().getCzxid());
      createVersions.add(fdbExistsOp.execute(REQUEST, tr, new ExistsRequest("/abc", false)).join()
          .unwrapOrElseThrow().getStat().getCzxid());
      createVersions.add(fdbExistsOp.execute(REQUEST, tr, new ExistsRequest("/cba", false)).join()
          .unwrapOrElseThrow().getStat().getCzxid());
      return createVersions;
    });

    assertThat(createVersions).hasSize(3);
    assertThat(createVersions.get(0)).isEqualTo(createVersions.get(1));
    assertThat(createVersions.get(0)).isEqualTo(createVersions.get(2));
  }

  @Test
  public void itDoesntSetIfOneOpFails() {
    final String data = Strings.repeat("this is the song that never ends ", 10000);

    MultiTransactionRecord ops = new MultiTransactionRecord();
    ops.add(Op.create(BASE_PATH, data.getBytes(), Collections.emptyList(), 0));
    ops.add(Op.check(SUBPATH, 15));

    MultiResponse opResults = fdbMultiOp.execute(REQUEST, ops);
    assertThat(opResults.getResultList())
        .containsExactly(new CreateResult(BASE_PATH), new ErrorResult(Code.NONODE.intValue()));

    Result<ExistsResponse, KeeperException> result = fdb.run(
        tr -> fdbExistsOp.execute(REQUEST, tr, new ExistsRequest(BASE_PATH, false))).join();

    assertThat(result.unwrapErrOrElseThrow().code().intValue()).isEqualTo(Code.NONODE.intValue());
  }

}
