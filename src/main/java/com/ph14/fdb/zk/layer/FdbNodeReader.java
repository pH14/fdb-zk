package com.ph14.fdb.zk.layer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.StatPersisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteStreams;
import com.ph14.fdb.zk.FdbSchemaConstants;

public class FdbNodeReader {

  private static final Logger LOG = LoggerFactory.getLogger(FdbNodeReader.class);

  private final DirectorySubspace nodeSubspace;

  public FdbNodeReader(DirectorySubspace nodeSubspace) {
    this.nodeSubspace = nodeSubspace;
  }

  public FdbNode deserialize(Transaction transaction) {
    return deserialize(transaction.getRange(nodeSubspace.range()).asList().join());
  }

  public FdbNode deserialize(byte[] key, byte[] value) {
    return deserialize(Collections.singletonList(new KeyValue(key, value)));
  }

  public FdbNode deserialize(List<KeyValue> keyValues) {
    ListMultimap<byte[], KeyValue> keyValuesByPrefix = ArrayListMultimap.create();

    byte[] dataPrefix = nodeSubspace.get(FdbSchemaConstants.DATA_KEY).pack();
    byte[] aclPrefix = nodeSubspace.get(FdbSchemaConstants.ACL_KEY).pack();
    byte[] statPrefix = nodeSubspace.get(FdbSchemaConstants.STAT_KEY).pack();

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
        Joiner.on("/").join(nodeSubspace.getPath()),
        getStat(keyValuesByPrefix.get(FdbSchemaConstants.STAT_KEY)),
        getData(keyValuesByPrefix.get(FdbSchemaConstants.DATA_KEY)),
        getAcls(keyValuesByPrefix.get(FdbSchemaConstants.ACL_KEY)));
  }

  private StatPersisted getStat(List<KeyValue> keyValues) {
    if (keyValues.size() != 1) {
      LOG.error("Stat with multiple keys: {}", keyValues);
      return new StatPersisted();
    }

    final StatPersisted statPersisted = new StatPersisted();

    try {
      statPersisted.readFields(ByteStreams.newDataInput(keyValues.get(0).getValue()));
      return statPersisted;
    } catch (IOException e) {
      LOG.error("Unknown stat bytes", e);
    }

    return statPersisted;
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

}
