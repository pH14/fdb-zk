package com.ph14.fdb.zk.ops;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoChildrenForEphemeralsException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbNode;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.layer.StatKey;
import com.ph14.fdb.zk.layer.ephemeral.FdbEphemeralNodeManager;

public class FdbCreateOp implements FdbOp<CreateRequest, CreateResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(FdbCreateOp.class);

  private final FdbNodeReader fdbNodeReader;
  private final FdbNodeWriter fdbNodeWriter;
  private final FdbWatchManager fdbWatchManager;
  private final FdbEphemeralNodeManager fdbEphemeralNodeManager;

  @Inject
  public FdbCreateOp(FdbNodeReader fdbNodeReader,
                     FdbNodeWriter fdbNodeWriter,
                     FdbWatchManager fdbWatchManager,
                     FdbEphemeralNodeManager fdbEphemeralNodeManager) {
    this.fdbNodeReader = fdbNodeReader;
    this.fdbNodeWriter = fdbNodeWriter;
    this.fdbWatchManager = fdbWatchManager;
    this.fdbEphemeralNodeManager = fdbEphemeralNodeManager;
  }

  @Override
  public CompletableFuture<Result<CreateResponse, KeeperException>> execute(Request zkRequest, Transaction transaction, CreateRequest request) {
    final CreateMode createMode;
    try {
      createMode = CreateMode.fromFlag(request.getFlags());
    } catch (KeeperException e) {
      return CompletableFuture.completedFuture(Result.err(e));
    }

    final DirectorySubspace parentSubspace;
    final Stat parentStat;

    try {
      parentSubspace = DirectoryLayer.getDefault().open(transaction, FdbPath.toFdbParentPath(request.getPath())).join();
      parentStat = fdbNodeReader.getNodeStat(parentSubspace, transaction).join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        LOG.error("Couldn't find parent: {} for {}", FdbPath.toFdbParentPath(request.getPath()), request.getPath());
        return CompletableFuture.completedFuture(Result.err(new KeeperException.NoNodeException("parent: " + request.getPath())));
      } else {
        LOG.error("Error completing request : {}. {}", request, e);
        return CompletableFuture.completedFuture(Result.err(new KeeperException.APIErrorException()));
      }
    }

    if (parentStat.getEphemeralOwner() != 0) {
      return CompletableFuture.completedFuture(Result.err(new NoChildrenForEphemeralsException()));
    }

    final String finalZkPath;
    final FdbNode fdbNode;
    final DirectorySubspace subspace;

    try {
      if (createMode.isSequential()) {
        finalZkPath = request.getPath() + String.format(Locale.ENGLISH, "%010d", parentStat.getCversion());
      } else {
        finalZkPath = request.getPath();
      }

      Optional<Long> ephemeralOwner = createMode.isEphemeral() ? Optional.of(zkRequest.sessionId) : Optional.empty();

      fdbNode = new FdbNode(finalZkPath, null, request.getData(), request.getAcl(), ephemeralOwner);
      subspace = DirectoryLayer.getDefault().create(transaction, FdbPath.toFdbPath(finalZkPath)).join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof DirectoryAlreadyExistsException) {
        return CompletableFuture.completedFuture(Result.err(new NodeExistsException(request.getPath())));
      } else {
        LOG.error("Error completing request : {}. {}", request, e);
        return CompletableFuture.completedFuture(Result.err(new KeeperException.SystemErrorException()));
      }
    }

    fdbNodeWriter.createNewNode(transaction, subspace, fdbNode);

    if (createMode.isEphemeral()) {
      fdbEphemeralNodeManager.addEphemeralNode(transaction, finalZkPath, zkRequest.sessionId);
    }

    // need atomic ops / little-endian storage if we want multis to work
    fdbNodeWriter.writeStat(
        transaction,
        parentSubspace,
        ImmutableMap.of(
            StatKey.PZXID, FdbNodeWriter.VERSIONSTAMP_FLAG,
            StatKey.CVERSION, parentStat.getCversion() + 1L,
            StatKey.NUM_CHILDREN, parentStat.getNumChildren() + 1L));

    fdbWatchManager.triggerNodeCreatedWatch(transaction, request.getPath());
    fdbWatchManager.triggerNodeChildrenWatch(transaction, FdbPath.toZkParentPath(request.getPath()));

    return CompletableFuture.completedFuture(Result.ok(new CreateResponse(finalZkPath)));
  }

}
