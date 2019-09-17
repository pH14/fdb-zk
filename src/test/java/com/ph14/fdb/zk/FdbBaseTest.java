package com.ph14.fdb.zk;

import java.util.Collections;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.MockFdbServerCnxn;
import org.apache.zookeeper.server.Request;
import org.junit.After;
import org.junit.Before;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.ph14.fdb.zk.layer.FdbNode;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.layer.changefeed.WatchEventChangefeed;
import com.ph14.fdb.zk.layer.ephemeral.FdbEphemeralNodeManager;
import com.ph14.fdb.zk.ops.FdbCreateOp;
import com.ph14.fdb.zk.ops.FdbDeleteOp;
import com.ph14.fdb.zk.ops.FdbExistsOp;
import com.ph14.fdb.zk.ops.FdbGetChildrenOp;
import com.ph14.fdb.zk.ops.FdbGetChildrenWithStatOp;
import com.ph14.fdb.zk.ops.FdbGetDataOp;
import com.ph14.fdb.zk.ops.FdbSetDataOp;

public class FdbBaseTest {

  protected static final String BASE_PATH = "/foo";
  protected static final String SUBPATH = "/foo/bar";
  protected static final MockFdbServerCnxn SERVER_CNXN = new MockFdbServerCnxn();
  protected static Request REQUEST = new Request(SERVER_CNXN, System.currentTimeMillis(), 1, 2, null, Collections.emptyList());

  protected FdbNodeWriter fdbNodeWriter;
  protected FdbWatchManager fdbWatchManager;
  protected WatchEventChangefeed watchEventChangefeed;
  protected FdbNodeReader fdbNodeReader;
  protected FdbEphemeralNodeManager fdbEphemeralNodeManager;

  protected FdbCreateOp fdbCreateOp;
  protected FdbGetDataOp fdbGetDataOp;
  protected FdbSetDataOp fdbSetDataOp;
  protected FdbExistsOp fdbExistsOp;
  protected FdbGetChildrenOp fdbGetChildrenOp;
  protected FdbGetChildrenWithStatOp fdbGetChildrenWithStatOp;
  protected FdbDeleteOp fdbDeleteOp;

  protected Database fdb;
  protected Transaction transaction;

  @Before
  public void setUp() {
    this.fdb = FDB.selectAPIVersion(600).open();

    SERVER_CNXN.clearWatchedEvents();
    REQUEST = new Request(SERVER_CNXN, System.nanoTime(), 1, 2, null, Collections.emptyList());

    fdbNodeWriter = new FdbNodeWriter();
    watchEventChangefeed = new WatchEventChangefeed(fdb);
    fdbWatchManager = new FdbWatchManager(watchEventChangefeed);
    fdbNodeReader = new FdbNodeReader();
    fdbEphemeralNodeManager = new FdbEphemeralNodeManager(fdb);

    fdbCreateOp = new FdbCreateOp(fdbNodeReader, fdbNodeWriter, fdbWatchManager, fdbEphemeralNodeManager);
    fdbGetDataOp = new FdbGetDataOp(fdbNodeReader, fdbWatchManager);
    fdbSetDataOp = new FdbSetDataOp(fdbNodeReader, fdbNodeWriter, fdbWatchManager);
    fdbExistsOp = new FdbExistsOp(fdbNodeReader, fdbWatchManager);
    fdbExistsOp = new FdbExistsOp(fdbNodeReader, fdbWatchManager);
    fdbGetChildrenWithStatOp = new FdbGetChildrenWithStatOp(fdbNodeReader, fdbWatchManager);
    fdbGetChildrenOp = new FdbGetChildrenOp(fdbGetChildrenWithStatOp);
    fdbDeleteOp = new FdbDeleteOp(fdbNodeReader, fdbNodeWriter, fdbWatchManager, fdbEphemeralNodeManager);

    fdb.run(tr -> {
      DirectoryLayer.getDefault().removeIfExists(tr, Collections.singletonList(FdbPath.ROOT_PATH)).join();
      DirectorySubspace rootSubspace = DirectoryLayer.getDefault().create(tr, Collections.singletonList(FdbPath.ROOT_PATH)).join();

      fdbNodeWriter.createNewNode(tr, rootSubspace, new FdbNode("/", null, new byte[0], Ids.OPEN_ACL_UNSAFE));

      return null;
    });

    this.transaction = fdb.createTransaction();
  }

  @After
  public void tearDown() {
    this.transaction.cancel();
    this.fdb.close();
  }

}
