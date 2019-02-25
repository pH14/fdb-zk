package com.ph14.fdb.zk.layer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.ph14.fdb.zk.ByteUtil;
import com.ph14.fdb.zk.FdbSchemaConstants;

public class FdbNodeWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FdbNodeWriter.class);

  private final FdbNodeStatWriter fdbNodeStatWriter;

  @Inject
  public FdbNodeWriter(FdbNodeStatWriter fdbNodeStatWriter) {
    this.fdbNodeStatWriter = fdbNodeStatWriter;
  }

  public Iterable<KeyValue> createNewNode(Subspace nodeSubspace, FdbNode fdbNode) {
    return Iterables.concat(
        getDataKeyValues(nodeSubspace, fdbNode.getData()),
        getStatKeyValues(nodeSubspace, fdbNode),
        getAclKeyValues(nodeSubspace, fdbNode.getAcls()),
        getWatchKeys(nodeSubspace));
  }

  public List<KeyValue> getDataKeyValues(Subspace nodeSubspace, byte[] data) {
    int dataLength = data.length;

    if (dataLength > FdbSchemaConstants.ZK_MAX_DATA_LENGTH) {
      // this is the actual ZK error. it uses jute.maxBuffer + 1024
      // throw new IOException("Unreasonable length " + dataLength);
    }

    Preconditions.checkArgument(dataLength < FdbSchemaConstants.ZK_MAX_DATA_LENGTH, "node data too large, was: " + dataLength);
    List<byte[]> dataBlocks = ByteUtil.divideByteArray(data, FdbSchemaConstants.FDB_MAX_VALUE_SIZE);

    ImmutableList.Builder<KeyValue> keyValues = ImmutableList.builder();

    for (int i = 0; i < dataBlocks.size(); i++) {
      keyValues.add(
          new KeyValue(
              nodeSubspace.pack(Tuple.from(FdbSchemaConstants.DATA_KEY, i)),
              dataBlocks.get(i))
      );
    }

    return keyValues.build();
  }

  private Iterable<KeyValue> getStatKeyValues(Subspace nodeSubspace, FdbNode fdbNode) {
    if (fdbNode.getStat() != null) {
      return fdbNodeStatWriter.getNodeCreationKeyValuesFromExistingStat(nodeSubspace, fdbNode);
    }

    return fdbNodeStatWriter.getNodeCreationKeyValues(nodeSubspace, fdbNode.getData(), Optional.empty());
  }

  private List<KeyValue> getAclKeyValues(Subspace nodeSubspace, List<ACL> acls) {
    ImmutableList.Builder<KeyValue> keyValues = ImmutableList.builder();

    for (int i = 0; i < acls.size(); i++) {
      try {
        ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
        acls.get(i).write(dataOutput);

        keyValues.add(
            new KeyValue(
                nodeSubspace.pack(Tuple.from(FdbSchemaConstants.ACL_KEY, i)),
                dataOutput.toByteArray())
        );
      } catch (IOException e) {
        LOG.error("Not ideal", e);
      }
    }

    return keyValues.build();
  }

  private List<KeyValue> getWatchKeys(Subspace nodeSubspace) {
    return ImmutableList.of(
        new KeyValue(nodeSubspace.get(FdbSchemaConstants.NODE_CREATED_WATCH_KEY).pack(), FdbSchemaConstants.EMPTY_BYTES),
        new KeyValue(nodeSubspace.get(FdbSchemaConstants.CHILD_CREATED_WATCH_KEY).pack(), FdbSchemaConstants.EMPTY_BYTES)
    );
  }

}
