package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.collect.ImmutableList;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbSchemaConstants;
import com.ph14.fdb.zk.layer.FdbNode;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbWatchManager;

public class FdbExists extends BaseFdbOp<ExistsRequest, ExistsResponse> {

  public FdbExists(Request rawRequest, Transaction transaction, ExistsRequest request) {
    super(rawRequest, transaction, request);
  }

  @Override
  public Result<ExistsResponse, KeeperException> execute() {
    List<String> path = ImmutableList.copyOf(request.getPath().split("/"));

    final DirectorySubspace subspace;
    try {
      subspace = DirectoryLayer.getDefault().open(transaction, path).join();

      byte[] key = subspace.get(FdbSchemaConstants.STAT_KEY).pack();

      FdbNode fdbNode = new FdbNodeReader(subspace).deserialize(
          key,
          transaction.get(key).join());

      Stat stat = new Stat();
      copyStat(fdbNode.getStat(), stat);

      return Result.ok(new ExistsResponse(stat));
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return Result.err(new NoNodeException(request.getPath()));
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  public void copyStat(StatPersisted stat, Stat to) {
    to.setAversion(stat.getAversion());
    to.setCtime(stat.getCtime());
    to.setCzxid(stat.getCzxid());
    to.setMtime(stat.getMtime());
    to.setMzxid(stat.getMzxid());
    to.setPzxid(stat.getPzxid());
    to.setVersion(stat.getVersion());
    to.setEphemeralOwner(stat.getEphemeralOwner());

    // TODO: What does this look like
    //    to.setDataLength(data == null ? 0 : data.length);
    //    int numChildren = 0;
    //    if (this.children != null) {
    //      numChildren = children.size();
    //    }
    //    // when we do the Cversion we need to translate from the count of the creates
    //    // to the count of the changes (v3 semantics)
    //    // for every create there is a delete except for the children still present
    //    to.setCversion(stat.getCversion()*2 - numChildren);
    //    to.setNumChildren(numChildren);
  }


}
