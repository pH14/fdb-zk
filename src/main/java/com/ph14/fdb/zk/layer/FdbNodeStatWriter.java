package com.ph14.fdb.zk.layer;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.ph14.fdb.zk.FdbSchemaConstants;

public class FdbNodeStatWriter {

  public static final long VERSIONSTAMP_FLAG = Long.MIN_VALUE;
  private static final byte[] INITIAL_VERSION = Ints.toByteArray(1);
  private final byte[] versionstampValue;

  @Inject
  public FdbNodeStatWriter() {
    this.versionstampValue = Tuple.from(Versionstamp.incomplete()).packWithVersionstamp();
  }

  public Iterable<KeyValue> getNodeCreationKeyValues(Subspace nodeSubspace, byte[] data, Optional<Long> ephemeralOwnerId) {
    byte[] now = Longs.toByteArray(System.currentTimeMillis());
    Subspace nodeStatSubspace = nodeSubspace.get(FdbSchemaConstants.STAT_KEY);

    return ImmutableList.of(
        StatKey.CZXID.toKeyValue(nodeStatSubspace, versionstampValue),
        StatKey.MZXID.toKeyValue(nodeStatSubspace, versionstampValue),
        StatKey.CTIME.toKeyValue(nodeStatSubspace, now),
        StatKey.MTIME.toKeyValue(nodeStatSubspace, now),
        StatKey.DATA_LENGTH.toKeyValue(nodeStatSubspace, data.length),
        StatKey.NUM_CHILDREN.toKeyValue(nodeStatSubspace, 0),
        StatKey.VERSION.toKeyValue(nodeStatSubspace, INITIAL_VERSION),
        StatKey.CVERSION.toKeyValue(nodeStatSubspace, INITIAL_VERSION),
        StatKey.AVERSION.toKeyValue(nodeStatSubspace, INITIAL_VERSION),
        StatKey.EPHEMERAL_OWNER.toKeyValue(nodeStatSubspace, ephemeralOwnerId.orElse(-1L))
    );
  }

  public Iterable<KeyValue> getStatDiffKeyValues(Subspace nodeSubspace, Map<StatKey, Long> newValues) {
    Subspace nodeStatSubspace = nodeSubspace.get(FdbSchemaConstants.STAT_KEY);

    return newValues.entrySet()
        .stream()
        .map(entry -> entry.getValue() == VERSIONSTAMP_FLAG
            ? entry.getKey().toKeyValue(nodeStatSubspace, versionstampValue)
            : entry.getKey().toKeyValue(nodeStatSubspace, entry.getValue()))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  Iterable<KeyValue> getNodeCreationKeyValuesFromExistingStat(Subspace nodeSubspace, FdbNode fdbNode) {
    Subspace nodeStatSubspace = nodeSubspace.get(FdbSchemaConstants.STAT_KEY);

    return ImmutableList.of(
        StatKey.CZXID.toKeyValue(nodeStatSubspace, fdbNode.getStat().getCzxid()),
        StatKey.MZXID.toKeyValue(nodeStatSubspace, fdbNode.getStat().getMzxid()),
        StatKey.CTIME.toKeyValue(nodeStatSubspace, fdbNode.getStat().getCtime()),
        StatKey.MTIME.toKeyValue(nodeStatSubspace, fdbNode.getStat().getMtime()),
        StatKey.DATA_LENGTH.toKeyValue(nodeStatSubspace, fdbNode.getStat().getDataLength()),
        StatKey.NUM_CHILDREN.toKeyValue(nodeStatSubspace, fdbNode.getStat().getNumChildren()),
        StatKey.VERSION.toKeyValue(nodeStatSubspace, fdbNode.getStat().getVersion()),
        StatKey.CVERSION.toKeyValue(nodeStatSubspace, fdbNode.getStat().getCversion()),
        StatKey.AVERSION.toKeyValue(nodeStatSubspace, fdbNode.getStat().getAversion()),
        StatKey.EPHEMERAL_OWNER.toKeyValue(nodeStatSubspace, fdbNode.getStat().getEphemeralOwner())
    );
  }

}
