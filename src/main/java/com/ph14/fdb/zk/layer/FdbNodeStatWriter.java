package com.ph14.fdb.zk.layer;

import java.util.Optional;

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
        new KeyValue(nodeStatSubspace.get(StatKey.CZXID.getStatKeyId()).pack(), versionstampValue),
        new KeyValue(nodeStatSubspace.get(StatKey.MZXID.getStatKeyId()).pack(), versionstampValue),
        new KeyValue(nodeStatSubspace.get(StatKey.CTIME.getStatKeyId()).pack(), now),
        new KeyValue(nodeStatSubspace.get(StatKey.MTIME.getStatKeyId()).pack(), now),
        new KeyValue(nodeStatSubspace.get(StatKey.DATA_LENGTH.getStatKeyId()).pack(), Ints.toByteArray(data.length)),
        new KeyValue(nodeStatSubspace.get(StatKey.NUM_CHILDREN.getStatKeyId()).pack(), Ints.toByteArray(0)),
        new KeyValue(nodeStatSubspace.get(StatKey.VERSION.getStatKeyId()).pack(), INITIAL_VERSION),
        new KeyValue(nodeStatSubspace.get(StatKey.CVERSION.getStatKeyId()).pack(), INITIAL_VERSION),
        new KeyValue(nodeStatSubspace.get(StatKey.AVERSION.getStatKeyId()).pack(), INITIAL_VERSION),
        new KeyValue(nodeStatSubspace.get(StatKey.EPHEMERAL_OWNER.getStatKeyId()).pack(), Longs.toByteArray(ephemeralOwnerId.orElse(-1L)))
    );
  }

  @VisibleForTesting
  Iterable<KeyValue> getNodeCreationKeyValuesFromExistingStat(Subspace nodeSubspace, FdbNode fdbNode) {
    Subspace nodeStatSubspace = nodeSubspace.get(FdbSchemaConstants.STAT_KEY);

    return ImmutableList.of(
        new KeyValue(nodeStatSubspace.get(StatKey.CZXID.getStatKeyId()).pack(), Longs.toByteArray(fdbNode.getStat().getCzxid())),
        new KeyValue(nodeStatSubspace.get(StatKey.MZXID.getStatKeyId()).pack(), Longs.toByteArray(fdbNode.getStat().getMzxid())),
        new KeyValue(nodeStatSubspace.get(StatKey.CTIME.getStatKeyId()).pack(), Longs.toByteArray(fdbNode.getStat().getCtime())),
        new KeyValue(nodeStatSubspace.get(StatKey.MTIME.getStatKeyId()).pack(), Longs.toByteArray(fdbNode.getStat().getMtime())),
        new KeyValue(nodeStatSubspace.get(StatKey.DATA_LENGTH.getStatKeyId()).pack(), Ints.toByteArray(fdbNode.getStat().getDataLength())),
        new KeyValue(nodeStatSubspace.get(StatKey.NUM_CHILDREN.getStatKeyId()).pack(), Ints.toByteArray(fdbNode.getStat().getNumChildren())),
        new KeyValue(nodeStatSubspace.get(StatKey.VERSION.getStatKeyId()).pack(), Ints.toByteArray(fdbNode.getStat().getVersion())),
        new KeyValue(nodeStatSubspace.get(StatKey.CVERSION.getStatKeyId()).pack(), Ints.toByteArray(fdbNode.getStat().getCversion())),
        new KeyValue(nodeStatSubspace.get(StatKey.AVERSION.getStatKeyId()).pack(), Ints.toByteArray(fdbNode.getStat().getAversion())),
        new KeyValue(nodeStatSubspace.get(StatKey.EPHEMERAL_OWNER.getStatKeyId()).pack(), Longs.toByteArray(fdbNode.getStat().getEphemeralOwner()))
    );
  }

}
