package com.ph14.fdb.zk.layer;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Shorts;

public enum StatKey {
  // Creation ZXID. Use Versionstamp at creation time
  CZXID(1),
  // Modification ZXID. Use Versionstamp at creation time
  MZXID(2),
  // Creation timestamp, epoch millis
  CTIME(3),
  // Modification timestamp, epoch millis
  MTIME(4),
  // # of updates to this node
  VERSION(5),
  // # of updates to this node's children
  CVERSION(6),
  // # of updates to this node's ACL policies
  AVERSION(7),
  // Session ID of ephemeral owner. -1 if permanent
  EPHEMERAL_OWNER(8),
  // Num bytes of data in node
  DATA_LENGTH(9),
  // Number of children of this node
  NUM_CHILDREN(10)
  ;

  private static final Map<Short, StatKey> INDEX = Arrays.stream(StatKey.values())
      .collect(ImmutableMap.toImmutableMap(
          k -> k.statKeyId,
          Function.identity()
      ));


  private final short statKeyId;

  StatKey(int statKeyId) {
    this.statKeyId = (short) statKeyId;
  }

  public byte[] getStatKeyId() {
    return Shorts.toByteArray(statKeyId);
  }

  public static StatKey from(byte[] statKeyId) {
    return INDEX.get(Shorts.fromByteArray(statKeyId));
  }

}
