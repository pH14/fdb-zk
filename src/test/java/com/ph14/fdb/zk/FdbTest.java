package com.ph14.fdb.zk;

import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.primitives.Longs;

public class FdbTest {

  @Test
  public void itRuns() throws InterruptedException {
    Database fdb = FDB.selectAPIVersion(600).open();

    Transaction transaction = fdb.createTransaction();


    byte[] key = new byte[] { 1 };
    byte[] versionstamp = Tuple.from(Versionstamp.incomplete()).packWithVersionstamp();

    transaction.set(key, key);
    transaction.commit().join();

    CompletableFuture<byte[]> run = fdb.run(tr -> {
      transaction.set(key, key);
      transaction.set(key, key);
      transaction.set(key, key);
      transaction.set(key, key);
      transaction.set(key, key);
      transaction.set(key, key);
      return tr.getVersionstamp();
    });

    System.out.println("" + Longs.fromByteArray(run.thenApply(Versionstamp::complete).join().getTransactionVersion()));
//    transaction.commit().join();
//
//    CountDownLatch countDownLatch = new CountDownLatch(1);
//
//    fdb.run(tr -> tr.watch(key).thenAccept(v -> countDownLatch.countDown()));
//
//    System.out.println("Value before set " + Arrays.toString(fdb.run(tr -> tr.get(key)).join()));
//
//    fdb.run(tr -> {
//      tr.set(key, versionstamp);
//      return null;
//    });
//
//    System.out.println("Value after  set " + Arrays.toString(fdb.run(tr -> tr.get(key)).join()));
//
//    assertThat(countDownLatch.await(1, TimeUnit.SECONDS)).isTrue();
  }

}
