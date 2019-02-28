package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbSchemaConstants;
import com.ph14.fdb.zk.layer.FdbNodeStatReader;
import com.ph14.fdb.zk.layer.FdbNodeStatWriter;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.layer.StatKey;

public class FdbSetDataOp implements BaseFdbOp<SetDataRequest, SetDataResponse> {

  private final FdbNodeWriter fdbNodeWriter;
  private final FdbNodeStatReader fdbNodeStatReader;
  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbSetDataOp(FdbNodeStatReader fdbNodeStatReader,
                      FdbNodeWriter fdbNodeWriter,
                      FdbWatchManager fdbWatchManager) {
    this.fdbNodeStatReader = fdbNodeStatReader;
    this.fdbNodeWriter = fdbNodeWriter;
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public Result<SetDataResponse, KeeperException> execute(Request zkRequest, Transaction transaction, SetDataRequest request) {
    List<String> path = ImmutableList.copyOf(request.getPath().split("/"));

    final DirectorySubspace subspace;
    final Stat stat;
    try {
      subspace = DirectoryLayer.getDefault().open(transaction, path).join();

      stat = fdbNodeStatReader.readNode(subspace, transaction).join();

      if (stat.getVersion() != request.getVersion()) {
        return Result.err(new BadVersionException(request.getPath()));
      }

      transaction.clear(new Range(
          subspace.pack(Tuple.from(FdbSchemaConstants.DATA_KEY, 0)),
          subspace.pack(Tuple.from(FdbSchemaConstants.DATA_KEY, Integer.MAX_VALUE))
      ));

      List<KeyValue> newDataKeyValues = fdbNodeWriter.getDataKeyValues(subspace, request.getData());
      newDataKeyValues.forEach(kv -> transaction.set(kv.getKey(), kv.getValue()));

      fdbNodeWriter.getStatDiffKeyValues(subspace, ImmutableMap.<StatKey, Long>builder()
          .put(StatKey.MZXID, FdbNodeStatWriter.VERSIONSTAMP_FLAG)
          .put(StatKey.MTIME, System.currentTimeMillis())
          .put(StatKey.VERSION, stat.getVersion() + 1L)
          .put(StatKey.DATA_LENGTH, (long) request.getData().length)
          .build())
          .forEach(kv -> transaction.set(kv.getKey(), kv.getValue()));

      fdbWatchManager.triggerNodeUpdatedWatch(transaction, request.getPath());
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return Result.err(new NoNodeException(request.getPath()));
      } else {
        throw new RuntimeException(e);
      }
    }

    Stat updatedStat = fdbNodeStatReader.readNode(subspace, transaction).join();

    return Result.ok(new SetDataResponse(updatedStat));
  }

}
