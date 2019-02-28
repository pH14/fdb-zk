package com.ph14.fdb.zk.layer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.data.Stat;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.ph14.fdb.zk.FdbSchemaConstants;

public class FdbNodeStatReader {

  @Inject
  public FdbNodeStatReader() {
  }

  public CompletableFuture<Stat> readNode(Subspace nodeSubspace, Transaction transaction) {
    return readNode(
        nodeSubspace,
        transaction.getRange(nodeSubspace.get(FdbSchemaConstants.STAT_KEY).range()).asList());
  }

  public CompletableFuture<Stat> readNode(Subspace nodeSubspace, Transaction transaction, StatKey ... statKeys) {
    Subspace statSubspace = nodeSubspace.get(FdbSchemaConstants.STAT_KEY);

    List<CompletableFuture<KeyValue>> futures = new ArrayList<>(statKeys.length);

    for (StatKey statKey : statKeys) {
      byte[] key = statKey.toKey(statSubspace);
      futures.add(transaction.get(key).thenApply(value -> new KeyValue(key, value)));
    }

    return AsyncUtil.getAll(futures).thenApply(kvs -> readNode(nodeSubspace, kvs));
  }

  public CompletableFuture<Stat> readNode(Subspace nodeSubspace, CompletableFuture<List<KeyValue>> keyValues) {
    return keyValues.thenApply(kvs -> readNode(nodeSubspace, kvs));
  }

  public Stat readNode(Subspace nodeSubspace, List<KeyValue> keyValues) {
    Subspace nodeStatSubspace = nodeSubspace.get(FdbSchemaConstants.STAT_KEY);
    return readStatFromKeyValues(nodeStatSubspace, keyValues);
  }

  private Stat readStatFromKeyValues(Subspace nodeStatSubspace, List<KeyValue> keyValues) {
    final Stat stat = new Stat();
    byte[] subspacePrefix = nodeStatSubspace.pack();

    for (KeyValue keyValue : keyValues) {
      if (!ByteArrayUtil.startsWith(keyValue.getKey(), subspacePrefix)) {
        continue;
      }

      StatKey statKey = StatKey.from(nodeStatSubspace.unpack(keyValue.getKey()).getBytes(0));

      switch (statKey) {
        case CZXID:
          stat.setCzxid(Longs.fromByteArray(keyValue.getValue()));
          break;
        case MZXID:
          stat.setMzxid(Longs.fromByteArray(keyValue.getValue()));
          break;
        case CTIME:
          stat.setCtime(Longs.fromByteArray(keyValue.getValue()));
          break;
        case MTIME:
          stat.setMtime(Longs.fromByteArray(keyValue.getValue()));
          break;
        case VERSION:
          stat.setVersion(Ints.fromByteArray(keyValue.getValue()));
          break;
        case AVERSION:
          stat.setAversion(Ints.fromByteArray(keyValue.getValue()));
          break;
        case CVERSION:
          stat.setCversion(Ints.fromByteArray(keyValue.getValue()));
          break;
        case NUM_CHILDREN:
          stat.setNumChildren(Ints.fromByteArray(keyValue.getValue()));
          break;
        case EPHEMERAL_OWNER:
          long sessionId = Longs.fromByteArray(keyValue.getValue());

          if (sessionId == -1) {
            break;
          }

          stat.setEphemeralOwner(sessionId);
          break;
        case DATA_LENGTH:
          stat.setDataLength(Ints.fromByteArray(keyValue.getValue()));
          break;
      }
    }

    return stat;
  }

}
