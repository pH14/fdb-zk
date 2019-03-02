package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.hubspot.algebra.Results;
import com.ph14.fdb.zk.FdbSchemaConstants;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.layer.StatKey;

public class FdbSetDataOp implements BaseFdbOp<SetDataRequest, SetDataResponse> {

  private final FdbNodeWriter fdbNodeWriter;
  private final FdbNodeReader fdbNodeReader;
  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbSetDataOp(FdbNodeReader fdbNodeReader,
                      FdbNodeWriter fdbNodeWriter,
                      FdbWatchManager fdbWatchManager) {
    this.fdbNodeReader = fdbNodeReader;
    this.fdbNodeWriter = fdbNodeWriter;
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public CompletableFuture<Result<SetDataResponse, KeeperException>> execute(Request zkRequest, Transaction transaction, SetDataRequest request) {
    List<String> path = ImmutableList.copyOf(request.getPath().split("/"));

    final DirectorySubspace subspace;
    final Stat stat;
    try {
      subspace = DirectoryLayer.getDefault().open(transaction, path).join();

      stat = fdbNodeReader.getNodeStat(subspace, transaction).join();

      if (stat.getVersion() != request.getVersion()) {
        return CompletableFuture.completedFuture(Result.err(new BadVersionException(request.getPath())));
      }

      transaction.clear(new Range(
          subspace.pack(Tuple.from(FdbSchemaConstants.DATA_KEY, 0)),
          subspace.pack(Tuple.from(FdbSchemaConstants.DATA_KEY, Integer.MAX_VALUE))
      ));

      fdbNodeWriter.writeData(transaction, subspace, request.getData());

      fdbNodeWriter.writeStat(transaction, subspace, ImmutableMap.<StatKey, Long>builder()
          .put(StatKey.MZXID, FdbNodeWriter.VERSIONSTAMP_FLAG)
          .put(StatKey.MTIME, System.currentTimeMillis())
          .put(StatKey.VERSION, stat.getVersion() + 1L)
          .put(StatKey.DATA_LENGTH, (long) request.getData().length)
          .build());

      fdbWatchManager.triggerNodeUpdatedWatch(transaction, request.getPath());
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return CompletableFuture.completedFuture(Results.err(new NoNodeException(request.getPath())));
      } else {
        throw new RuntimeException(e);
      }
    }

    // This is a little messy... the ZK API returns the Stat of the node after modification.
    // The MZXID, the global transaction id at modification time, will be updated by the
    // transaction's `mutate` call, but since the mutate will apply the versionstamp
    // atomically once it hits the db, this means that we can't read-our-writes here and
    // we have to spin up a new transaction to observe it.
    //
    // In the new transaction read, we also don't want to return anything _more_ recent
    // than the commit made here so that it looks like it's part of the same ZK transaction,
    // so we need to record the commit id and set the read version to get exactly what
    // we just wrote. However... we don't know the version of the write until after it
    // commits, which is after this method is called, so we chain the read onto the commit
    // of the initial transaction.
    final Database database = transaction.getDatabase();
    CompletableFuture<byte[]> commitVersionstamp = transaction.getVersionstamp();

    return CompletableFuture.completedFuture(
        commitVersionstamp.thenApply(versionstamp ->
            database.run(tr -> {
              long readVersionFromStamp = Longs.fromByteArray(Versionstamp.complete(versionstamp).getTransactionVersion());
              tr.setReadVersion(readVersionFromStamp);

              Stat updatedStat = fdbNodeReader.getNodeStat(subspace, tr).join();
              return Result.<SetDataResponse, KeeperException>ok(new SetDataResponse(updatedStat));
            }))).join();
  }

}
