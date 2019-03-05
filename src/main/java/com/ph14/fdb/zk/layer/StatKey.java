package com.ph14.fdb.zk.layer;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

public enum StatKey {
  // Creation ZXID. Use Versionstamp at creation time
  CZXID(1, StatKeyValueType.LONG),
  // Modification ZXID. Use Versionstamp at creation time / data modification time
  MZXID(2, StatKeyValueType.LONG),
  // Parent ZXID. Use versionstamp at creation / child creation+deletion time
  PZXID(3, StatKeyValueType.LONG),
  // Creation timestamp, epoch millis
  CTIME(4, StatKeyValueType.LONG),
  // Modification timestamp, epoch millis
  MTIME(5, StatKeyValueType.LONG),
  // # of updates to this node
  VERSION(6, StatKeyValueType.INT),
  // # of updates to this node's children
  CVERSION(7, StatKeyValueType.INT),
  // # of updates to this node's ACL policies
  AVERSION(8, StatKeyValueType.INT),
  // Session ID of ephemeral owner. -1 if permanent
  EPHEMERAL_OWNER(9, StatKeyValueType.LONG),
  // Num bytes of data in node
  DATA_LENGTH(10, StatKeyValueType.INT),
  // Number of children of this node
  NUM_CHILDREN(11, StatKeyValueType.INT)
  ;

  private enum StatKeyValueType {
    INT,
    LONG
  }

  private static final Map<Short, StatKey> INDEX = Arrays.stream(StatKey.values())
      .collect(ImmutableMap.toImmutableMap(
          k -> k.statKeyId,
          Function.identity()
      ));


  private final short statKeyId;
  private final StatKeyValueType statKeyValueType;

  StatKey(int statKeyId, StatKeyValueType statKeyValueType) {
    this.statKeyId = (short) statKeyId;
    this.statKeyValueType = statKeyValueType;
  }

  public byte[] getStatKeyId() {
    return Shorts.toByteArray(statKeyId);
  }

  public byte[] getValue(long value) {
    switch (statKeyValueType) {
      case INT:
        return Ints.toByteArray((int) value);
      case LONG:
        return Longs.toByteArray(value);
      default:
        throw new RuntimeException("unknown stat key value type: " + statKeyValueType);
    }
  }

  public byte[] toKey(Subspace nodeStatSubspace) {
    return nodeStatSubspace.get(getStatKeyId()).pack();
  }

  public KeyValue toKeyValue(Subspace nodeStatSubspace, byte[] value) {
    return new KeyValue(toKey(nodeStatSubspace), value);
  }

  public KeyValue toKeyValue(Subspace nodeStatSubspace, long value) {
    return new KeyValue(toKey(nodeStatSubspace), getValue(value));
  }

  public static StatKey from(byte[] statKeyId) {
    return INDEX.get(Shorts.fromByteArray(statKeyId));
  }

}
