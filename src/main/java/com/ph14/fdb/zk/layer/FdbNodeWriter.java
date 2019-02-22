package com.ph14.fdb.zk.layer;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.StatPersisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.ph14.fdb.zk.ByteUtil;
import com.ph14.fdb.zk.FdbSchemaConstants;

public class FdbNodeWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FdbNodeWriter.class);

  private final DirectorySubspace nodeSubspace;

  public FdbNodeWriter(DirectorySubspace nodeSubspace) {
    this.nodeSubspace = nodeSubspace;
  }

  public Iterable<KeyValue> serialize(FdbNode fdbNode) {
    int dataLength = fdbNode.getData().length;

    if (dataLength > FdbSchemaConstants.ZK_MAX_DATA_LENGTH) {
      // this is the actual ZK error. it uses jute.maxBuffer + 1024
      // throw new IOException("Unreasonable length " + dataLength);
    }

    Preconditions.checkArgument(dataLength < FdbSchemaConstants.ZK_MAX_DATA_LENGTH, "node data too large, was: " + dataLength);

    return Iterables.concat(
        getDataKeyValues(fdbNode.getData()),
        getStatKeyValues(fdbNode.getStat()),
        getAclKeyValues(fdbNode.getAcls()));
  }

  private List<KeyValue> getDataKeyValues(byte[] data) {
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

  private List<KeyValue> getStatKeyValues(StatPersisted stat) {
    ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();

    try {
      stat.write(dataOutput);

      return ImmutableList.of(
          new KeyValue(
              nodeSubspace.pack(FdbSchemaConstants.STAT_KEY),
              dataOutput.toByteArray())
      );
    } catch (IOException e) {
      LOG.error("Not ideal", e);
    }

    return ImmutableList.of();
  }

  private List<KeyValue> getAclKeyValues(List<ACL> acls) {
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

}
