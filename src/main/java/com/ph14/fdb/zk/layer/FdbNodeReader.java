package com.ph14.fdb.zk.layer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.ph14.fdb.zk.FdbSchemaConstants;

public class FdbNodeReader {

  private static final Logger LOG = LoggerFactory.getLogger(FdbNodeReader.class);

  @Inject
  public FdbNodeReader() {
  }

  public CompletableFuture<FdbNode> getNode(DirectorySubspace nodeSubspace, Transaction transaction) {
    return transaction.getRange(nodeSubspace.range()).asList()
        .thenApply(kvs -> getNode(nodeSubspace, kvs));
  }

  public FdbNode getNode(DirectorySubspace nodeSubspace, List<KeyValue> keyValues) {
    ListMultimap<byte[], KeyValue> keyValuesByPrefix = ArrayListMultimap.create();

    byte[] statPrefix = nodeSubspace.get(FdbSchemaConstants.STAT_KEY).pack();
    byte[] aclPrefix = nodeSubspace.get(FdbSchemaConstants.ACL_KEY).pack();
    byte[] dataPrefix = nodeSubspace.get(FdbSchemaConstants.DATA_KEY).pack();

    for (KeyValue keyValue : keyValues) {
      if (ByteArrayUtil.startsWith(keyValue.getKey(), statPrefix)) {
        keyValuesByPrefix.put(FdbSchemaConstants.STAT_KEY, keyValue);
      } else if (ByteArrayUtil.startsWith(keyValue.getKey(), aclPrefix)) {
        keyValuesByPrefix.put(FdbSchemaConstants.ACL_KEY, keyValue);
      } else if (ByteArrayUtil.startsWith(keyValue.getKey(), dataPrefix)) {
        keyValuesByPrefix.put(FdbSchemaConstants.DATA_KEY, keyValue);
      }
    }

    return new FdbNode(
        FdbPath.toZkPath(nodeSubspace.getPath()),
        getNodeStat(nodeSubspace, keyValuesByPrefix.get(FdbSchemaConstants.STAT_KEY)),
        getData(keyValuesByPrefix.get(FdbSchemaConstants.DATA_KEY)),
        getAcls(keyValuesByPrefix.get(FdbSchemaConstants.ACL_KEY)));
  }

  public CompletableFuture<Stat> getNodeStat(Subspace nodeSubspace, Transaction transaction) {
    return getNodeStat(
        nodeSubspace,
        transaction.getRange(nodeSubspace.get(FdbSchemaConstants.STAT_KEY).range()).asList());
  }

  public CompletableFuture<Stat> getNodeStat(Subspace nodeSubspace, CompletableFuture<List<KeyValue>> keyValues) {
    return keyValues.thenApply(kvs -> getNodeStat(nodeSubspace, kvs));
  }

  public CompletableFuture<Stat> getNodeStat(Subspace nodeSubspace, Transaction transaction, StatKey ... statKeys) {
    Subspace statSubspace = nodeSubspace.get(FdbSchemaConstants.STAT_KEY);

    List<CompletableFuture<KeyValue>> futures = new ArrayList<>(statKeys.length);

    for (StatKey statKey : statKeys) {
      byte[] key = statKey.toKey(statSubspace);
      futures.add(transaction.get(key).thenApply(value -> new KeyValue(key, value)));
    }

    return AsyncUtil.getAll(futures).thenApply(kvs -> getNodeStat(nodeSubspace, kvs));
  }

  private byte[] getData(List<KeyValue> keyValues) {
    return ByteArrayUtil.join(
        new byte[0],
        keyValues.stream()
            .map(KeyValue::getValue)
            .collect(ImmutableList.toImmutableList()));
  }

  private List<ACL> getAcls(List<KeyValue> keyValues) {
    return keyValues.stream()
        .map(kv -> {
          final ACL acl = new ACL();

          try {
            acl.readFields(ByteStreams.newDataInput(kv.getValue()));
          } catch (IOException e) {
            LOG.error("Unknown stat bytes", e);
          }

          return acl;
        })
        .collect(ImmutableList.toImmutableList());
  }

  private Stat getNodeStat(Subspace nodeSubspace, List<KeyValue> keyValues) {
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
