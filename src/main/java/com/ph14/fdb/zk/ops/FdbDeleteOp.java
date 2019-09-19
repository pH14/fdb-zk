package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.apache.zookeeper.KeeperException.SystemErrorException;
import org.apache.zookeeper.OpResult.DeleteResult;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.hubspot.algebra.Results;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.layer.StatKey;
import com.ph14.fdb.zk.layer.ephemeral.FdbEphemeralNodeManager;

public class FdbDeleteOp implements FdbOp<DeleteRequest, DeleteResult> {

  private static final Logger LOG = LoggerFactory.getLogger(FdbDeleteOp.class);

  private static final int ALL_VERSIONS_FLAG = -1;

  private final FdbNodeReader fdbNodeReader;
  private final FdbNodeWriter fdbNodeWriter;
  private final FdbWatchManager fdbWatchManager;
  private final FdbEphemeralNodeManager fdbEphemeralNodeManager;

  @Inject
  public FdbDeleteOp(FdbNodeReader fdbNodeReader,
                     FdbNodeWriter fdbNodeWriter,
                     FdbWatchManager fdbWatchManager,
                     FdbEphemeralNodeManager fdbEphemeralNodeManager) {
    this.fdbNodeReader = fdbNodeReader;
    this.fdbNodeWriter = fdbNodeWriter;
    this.fdbWatchManager = fdbWatchManager;
    this.fdbEphemeralNodeManager = fdbEphemeralNodeManager;
  }

  @Override
  public CompletableFuture<Result<DeleteResult, KeeperException>> execute(Request zkRequest, Transaction transaction, DeleteRequest request) {
    List<String> path = FdbPath.toFdbPath(request.getPath());

    final DirectorySubspace nodeSubspace;
    final Stat stat;

    final DirectorySubspace parentNodeSubspace;
    final Stat parentStat;
    try {
      nodeSubspace = DirectoryLayer.getDefault().open(transaction, path).join();
      stat = fdbNodeReader.getNodeStat(nodeSubspace, transaction).join();

      if (stat.getNumChildren() != 0) {
        return CompletableFuture.completedFuture(Result.err(new NotEmptyException(request.getPath())));
      }

      if (request.getVersion() != ALL_VERSIONS_FLAG && stat.getVersion() != request.getVersion()) {
        return CompletableFuture.completedFuture(Result.err(new BadVersionException(request.getPath())));
      }
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return CompletableFuture.completedFuture(Results.err(new NoNodeException(request.getPath())));
      } else {
        LOG.error("Error completing request: {}, {}", zkRequest, e);
        return CompletableFuture.completedFuture(Results.err(new SystemErrorException()));
      }
    }

    parentNodeSubspace = DirectoryLayer.getDefault().open(transaction, FdbPath.toFdbParentPath(request.getPath())).join();

    // could eliminate this read by using atomic ops and using endianness correctly
    parentStat = fdbNodeReader.getNodeStat(parentNodeSubspace, transaction, StatKey.CVERSION, StatKey.NUM_CHILDREN, StatKey.EPHEMERAL_OWNER).join();

    fdbNodeWriter.writeStat(transaction, parentNodeSubspace,
        ImmutableMap.of(
            StatKey.PZXID, FdbNodeWriter.VERSIONSTAMP_FLAG,
            StatKey.CVERSION, parentStat.getCversion() + 1L,
            StatKey.NUM_CHILDREN, parentStat.getNumChildren() - 1L
        ));

    if (stat.getEphemeralOwner() != 0) {
      fdbEphemeralNodeManager.removeNode(transaction, request.getPath(), zkRequest.sessionId);
    }

    fdbNodeWriter.deleteNodeAsync(transaction, nodeSubspace).join();

    fdbWatchManager.triggerNodeDeletedWatch(transaction, request.getPath());
    fdbWatchManager.triggerNodeChildrenWatch(transaction, FdbPath.toZkParentPath(request.getPath()));

    return CompletableFuture.completedFuture(Result.ok(new DeleteResult()));
  }

}
