package com.ph14.fdb.zk.layer.ephemeral;

import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class FdbEphemeralNodeManager {

  private static final String EPHEMERAL_NODE_SUBSPACE = "fdb-zk-ephemeral-nodes";
  private static final byte[] EMPTY_VALUE = new byte[0];

  private final Subspace ephemeralNodeSubspace;

  @Inject
  public FdbEphemeralNodeManager() {
    this.ephemeralNodeSubspace = new Subspace(EPHEMERAL_NODE_SUBSPACE.getBytes(Charsets.UTF_8));
  }

  public void addEphemeralNode(Transaction transaction, String zkPath, long sessionId) {
    transaction.set(ephemeralNodeSubspace.pack(Tuple.from(sessionId, zkPath)), EMPTY_VALUE);
  }

  public CompletableFuture<Iterable<String>> getEphemeralNodeZkPaths(Transaction transaction, long sessionId) {
    return transaction.getRange(Range.startsWith(ephemeralNodeSubspace.pack(sessionId)))
        .asList()
        .thenApply(kvs ->
            Iterables.transform(
                kvs,
                kv -> Tuple.fromBytes(kv.getKey()).getString(2)));
  }

  public void removeNode(Transaction transaction, String zkPath, long sessionId) {
    transaction.clear(ephemeralNodeSubspace.pack(Tuple.from(sessionId, zkPath)));
  }

  public void clearEphemeralNodesForSession(Transaction transaction, long sessionId) {
    transaction.clear(Range.startsWith(ephemeralNodeSubspace.pack(sessionId)));
  }

}
