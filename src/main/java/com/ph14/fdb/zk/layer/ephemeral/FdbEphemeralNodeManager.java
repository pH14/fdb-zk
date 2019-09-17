package com.ph14.fdb.zk.layer.ephemeral;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class FdbEphemeralNodeManager {

  private static final String EPHEMERAL_NODE_SUBSPACE = "fdb-zk-ephemeral-nodes";
  private static final byte[] EMPTY_VALUE = new byte[0];

  private final byte[] ephemeralNodeSubspace;

  @Inject
  public FdbEphemeralNodeManager(Database fdb) {
    this.ephemeralNodeSubspace = DirectoryLayer.getDefault().createOrOpen(
        fdb.createTransaction(),
        Collections.singletonList(EPHEMERAL_NODE_SUBSPACE))
        .join().pack();
  }

  public void addEphemeralNode(Transaction transaction, String zkPath, long sessionId) {
    transaction.set(Tuple.from(ephemeralNodeSubspace, sessionId, zkPath).pack(), EMPTY_VALUE);
  }

  public CompletableFuture<Iterable<String>> getEphemeralNodeZkPaths(Transaction transaction, long sessionId) {
    return transaction.getRange(Range.startsWith(Tuple.from(ephemeralNodeSubspace, sessionId).pack()))
        .asList()
        .thenApply(kvs ->
            Iterables.transform(
                kvs,
                kv -> Tuple.fromBytes(kv.getKey()).getString(2)));
  }

  public void clearEphemeralNodesForSession(Transaction transaction, long sessionId) {
    transaction.clear(Range.startsWith(Tuple.from(ephemeralNodeSubspace, sessionId).pack()));
  }

}
