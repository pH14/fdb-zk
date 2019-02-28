package com.ph14.fdb.zk;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

public class FdbTest {

  @Test
  public void itRuns() throws InterruptedException {
    Database fdb = FDB.selectAPIVersion(600).open();

    byte[] key = new byte[] { 1 };
    byte[] versionstamp = Tuple.from(Versionstamp.incomplete()).packWithVersionstamp();

    CountDownLatch countDownLatch = new CountDownLatch(1);

    fdb.run(tr -> tr.watch(key).thenAccept(v -> countDownLatch.countDown()));

    System.out.println("Value before set " + Arrays.toString(fdb.run(tr -> tr.get(key)).join()));

    fdb.run(tr -> {
      tr.set(key, versionstamp);
      return null;
    });

    System.out.println("Value after  set " + Arrays.toString(fdb.run(tr -> tr.get(key)).join()));

    assertThat(countDownLatch.await(1, TimeUnit.SECONDS)).isTrue();
  }

}
