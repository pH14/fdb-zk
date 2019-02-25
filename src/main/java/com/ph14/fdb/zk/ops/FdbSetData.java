package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbSchemaConstants;
import com.ph14.fdb.zk.layer.FdbNodeWriter;

public class FdbSetData implements BaseFdbOp<SetDataRequest, SetDataResponse> {

  private final FdbNodeWriter fdbNodeWriter;

  @Inject
  public FdbSetData(FdbNodeWriter fdbNodeWriter) {
    this.fdbNodeWriter = fdbNodeWriter;
  }

  @Override
  public Result<SetDataResponse, KeeperException> execute(Request zkRequest, Transaction transaction, SetDataRequest request) {
    List<String> path = ImmutableList.copyOf(request.getPath().split("/"));

    final DirectorySubspace subspace;
    try {
      subspace = DirectoryLayer.getDefault().open(transaction, path).join();

      // TODO: check version
      // TODO: Update Stat timestamps

      transaction.clear(new Range(
          subspace.pack(Tuple.from(FdbSchemaConstants.DATA_KEY, 0)),
          subspace.pack(Tuple.from(FdbSchemaConstants.DATA_KEY, Integer.MAX_VALUE))
      ));

      List<KeyValue> newDataKeyValues = fdbNodeWriter.getDataKeyValues(subspace, request.getData());
      newDataKeyValues.forEach(kv -> transaction.set(kv.getKey(), kv.getValue()));

      transaction.mutate(
          MutationType.SET_VERSIONSTAMPED_VALUE,
          subspace.get(FdbSchemaConstants.NODE_DATA_UPDATED_KEY).pack(),
          Tuple.from(Versionstamp.incomplete()).packWithVersionstamp());
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return Result.err(new NoNodeException(request.getPath()));
      } else {
        throw new RuntimeException(e);
      }
    }

    return Result.ok(new SetDataResponse(new Stat()));
  }

}
