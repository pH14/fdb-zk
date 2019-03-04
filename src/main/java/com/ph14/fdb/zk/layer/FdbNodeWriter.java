package com.ph14.fdb.zk.layer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.ph14.fdb.zk.ByteUtil;
import com.ph14.fdb.zk.FdbSchemaConstants;

public class FdbNodeWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FdbNodeWriter.class);

  public static final long VERSIONSTAMP_FLAG = Long.MIN_VALUE;
  public static final long INCREMENT_FLAG = Long.MIN_VALUE + 1;
  private static final byte[] INITIAL_VERSION = Ints.toByteArray(1);
  private final byte[] versionstampValue;

  @Inject
  public FdbNodeWriter() {
    this.versionstampValue = Tuple.from(Versionstamp.incomplete()).packWithVersionstamp();
  }

  public void createNewNode(Transaction transaction, Subspace nodeSubspace, FdbNode fdbNode) {
    writeStat(transaction, nodeSubspace, fdbNode);
    writeData(transaction, nodeSubspace, fdbNode.getData());
    writeACLs(transaction, nodeSubspace, fdbNode.getAcls());
  }

  public void writeData(Transaction transaction, Subspace nodeSubspace, byte[] data) {
    int dataLength = data.length;

    if (dataLength > FdbSchemaConstants.ZK_MAX_DATA_LENGTH) {
      // this is the actual ZK error. it uses jute.maxBuffer + 1024 as the max znode size
      throw new RuntimeException(new IOException("Unreasonable length " + dataLength));
    }

    transaction.clear(new Range(
        nodeSubspace.pack(Tuple.from(FdbSchemaConstants.DATA_KEY, 0)),
        nodeSubspace.pack(Tuple.from(FdbSchemaConstants.DATA_KEY, Integer.MAX_VALUE))
    ));

    List<byte[]> dataBlocks = ByteUtil.divideByteArray(data, FdbSchemaConstants.FDB_MAX_VALUE_SIZE);

    for (int i = 0; i < dataBlocks.size(); i++) {
      transaction.set(
          nodeSubspace.pack(Tuple.from(FdbSchemaConstants.DATA_KEY, i)),
          dataBlocks.get(i)
      );
    }
  }

  public void writeStat(Transaction transaction, Subspace nodeSubspace, Map<StatKey, Long> newValues) {
    Subspace nodeStatSubspace = nodeSubspace.get(FdbSchemaConstants.STAT_KEY);

    for (Entry<StatKey, Long> entry : newValues.entrySet()) {
      if (entry.getValue() == VERSIONSTAMP_FLAG) {
        transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, entry.getKey().toKey(nodeStatSubspace), versionstampValue);
      } else if (entry.getValue() == INCREMENT_FLAG) {
        transaction.mutate(MutationType.ADD, entry.getKey().toKey(nodeStatSubspace), Ints.toByteArray(1));
      } else {
        KeyValue keyValue = entry.getKey().toKeyValue(nodeStatSubspace, entry.getValue());
        transaction.set(keyValue.getKey(), keyValue.getValue());
      }
    }
  }

  private void writeStat(Transaction transaction, Subspace nodeSubspace, FdbNode fdbNode) {
    if (fdbNode.getStat() == null) {
      writeStatForNewNode(transaction, nodeSubspace, fdbNode.getData(), Optional.empty());
    } else {
      writeStatFromExistingNode(transaction, nodeSubspace, fdbNode);
    }
  }

  private void writeACLs(Transaction transaction, Subspace nodeSubspace, List<ACL> acls) {
    for (int i = 0; i < acls.size(); i++) {
      try {
        ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
        acls.get(i).write(dataOutput);

        transaction.set(
            nodeSubspace.pack(Tuple.from(FdbSchemaConstants.ACL_KEY, i)),
            dataOutput.toByteArray()
        );
      } catch (IOException e) {
        LOG.error("Not ideal", e);
      }
    }
  }

  private void writeStatForNewNode(Transaction transaction, Subspace nodeSubspace, byte[] data, Optional<Long> ephemeralOwnerId) {
    byte[] now = Longs.toByteArray(System.currentTimeMillis());
    Subspace nodeStatSubspace = nodeSubspace.get(FdbSchemaConstants.STAT_KEY);

    transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, StatKey.CZXID.toKey(nodeStatSubspace), versionstampValue);
    transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, StatKey.MZXID.toKey(nodeStatSubspace), versionstampValue);

    transaction.set(StatKey.CTIME.toKey(nodeStatSubspace), now);
    transaction.set(StatKey.MTIME.toKey(nodeStatSubspace), now);
    transaction.set(StatKey.DATA_LENGTH.toKey(nodeStatSubspace), StatKey.DATA_LENGTH.getValue(data.length));
    transaction.set(StatKey.NUM_CHILDREN.toKey(nodeStatSubspace), Ints.toByteArray(0));
    transaction.set(StatKey.VERSION.toKey(nodeStatSubspace), INITIAL_VERSION);
    transaction.set(StatKey.CVERSION.toKey(nodeStatSubspace), INITIAL_VERSION);
    transaction.set(StatKey.AVERSION.toKey(nodeStatSubspace), INITIAL_VERSION);
    transaction.set(StatKey.EPHEMERAL_OWNER.toKey(nodeStatSubspace), StatKey.EPHEMERAL_OWNER.getValue(ephemeralOwnerId.orElse(-1L)));
  }

  @VisibleForTesting
  void writeStatFromExistingNode(Transaction transaction, Subspace nodeSubspace, FdbNode fdbNode) {
    Subspace nodeStatSubspace = nodeSubspace.get(FdbSchemaConstants.STAT_KEY);

    transaction.set(
        StatKey.CZXID.toKey(nodeStatSubspace),
        StatKey.CZXID.getValue(fdbNode.getStat().getCzxid()));
    transaction.set(
        StatKey.MZXID.toKey(nodeStatSubspace),
        StatKey.MZXID.getValue(fdbNode.getStat().getMzxid()));
    transaction.set(
        StatKey.CTIME.toKey(nodeStatSubspace),
        StatKey.CTIME.getValue(fdbNode.getStat().getCtime()));
    transaction.set(
        StatKey.MTIME.toKey(nodeStatSubspace),
        StatKey.MTIME.getValue(fdbNode.getStat().getMtime()));
    transaction.set(
        StatKey.DATA_LENGTH.toKey(nodeStatSubspace),
        StatKey.DATA_LENGTH.getValue(fdbNode.getStat().getDataLength()));
    transaction.set(
        StatKey.NUM_CHILDREN.toKey(nodeStatSubspace),
        StatKey.NUM_CHILDREN.getValue(fdbNode.getStat().getNumChildren()));
    transaction.set(
        StatKey.VERSION.toKey(nodeStatSubspace),
        StatKey.VERSION.getValue(fdbNode.getStat().getVersion()));
    transaction.set(
        StatKey.CVERSION.toKey(nodeStatSubspace),
        StatKey.CVERSION.getValue(fdbNode.getStat().getCversion()));
    transaction.set(
        StatKey.AVERSION.toKey(nodeStatSubspace),
        StatKey.AVERSION.getValue(fdbNode.getStat().getAversion()));
    transaction.set(
        StatKey.EPHEMERAL_OWNER.toKey(nodeStatSubspace),
        StatKey.EPHEMERAL_OWNER.getValue(fdbNode.getStat().getEphemeralOwner()));
  }


}
