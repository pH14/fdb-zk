package com.ph14.fdb.zk.layer;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.ph14.fdb.zk.FdbSchemaConstants;

public class FdbNodeReader {

  private static final Logger LOG = LoggerFactory.getLogger(FdbNodeReader.class);

  private final FdbNodeStatReader fdbNodeStatReader;

  @Inject
  public FdbNodeReader(FdbNodeStatReader fdbNodeStatReader) {
    this.fdbNodeStatReader = fdbNodeStatReader;
  }

  public FdbNode deserialize(DirectorySubspace nodeSubspace, Transaction transaction) {
    return deserialize(nodeSubspace, transaction.getRange(nodeSubspace.range()).asList().join());
  }

  public FdbNode deserialize(DirectorySubspace nodeSubspace, List<KeyValue> keyValues) {
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
        Joiner.on("/").join(nodeSubspace.getPath()),
        getStat(nodeSubspace, keyValuesByPrefix.get(FdbSchemaConstants.STAT_KEY)),
        getData(keyValuesByPrefix.get(FdbSchemaConstants.DATA_KEY)),
        getAcls(keyValuesByPrefix.get(FdbSchemaConstants.ACL_KEY)));
  }

  private Stat getStat(Subspace nodeSubspace, List<KeyValue> keyValues) {
    return fdbNodeStatReader.readNode(nodeSubspace, keyValues);
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
