package com.ph14.fdb.zk;

import java.util.Arrays;
import java.util.Collections;

import org.apache.zookeeper.server.MockFdbServerCnxn;
import org.apache.zookeeper.server.Request;
import org.junit.After;
import org.junit.Before;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.ops.FdbCreateOp;
import com.ph14.fdb.zk.ops.FdbExistsOp;
import com.ph14.fdb.zk.ops.FdbGetDataOp;
import com.ph14.fdb.zk.ops.FdbSetDataOp;

public class FdbBaseTest {

  protected static final String BASE_PATH = "/foo";
  protected static final String SUBPATH = "/foo/bar";
  protected static final MockFdbServerCnxn SERVER_CNXN = new MockFdbServerCnxn();
  protected static final Request REQUEST = new Request(SERVER_CNXN, System.currentTimeMillis(), 1, 2, null, Collections.emptyList());

  protected FdbNodeWriter fdbNodeWriter;
  protected FdbWatchManager fdbWatchManager;
  protected FdbNodeReader fdbNodeReader;

  protected FdbCreateOp fdbCreateOp;
  protected FdbGetDataOp fdbGetDataOp;
  protected FdbSetDataOp fdbSetDataOp;
  protected FdbExistsOp fdbExistsOp;

  protected Database fdb;
  protected Transaction transaction;

  @Before
  public void setUp() {
    this.fdb = FDB.selectAPIVersion(600).open();

    SERVER_CNXN.clearWatchedEvents();

    fdbNodeWriter = new FdbNodeWriter();
    fdbWatchManager = new FdbWatchManager(fdb);
    fdbNodeReader = new FdbNodeReader();

    fdbCreateOp = new FdbCreateOp(fdbNodeWriter, fdbWatchManager);
    fdbGetDataOp = new FdbGetDataOp(fdbNodeReader, fdbWatchManager);
    fdbSetDataOp = new FdbSetDataOp(fdbNodeReader, fdbNodeWriter, fdbWatchManager);
    fdbExistsOp = new FdbExistsOp(fdbNodeReader, fdbWatchManager);

    fdb.run(tr -> {
      DirectoryLayer.getDefault().removeIfExists(tr, Arrays.asList("", "foo")).join();
      DirectoryLayer.getDefault().removeIfExists(tr, Arrays.asList("", "foo", "bar")).join();
      DirectoryLayer.getDefault().removeIfExists(tr, Arrays.asList("foo")).join();
      DirectoryLayer.getDefault().removeIfExists(tr, Arrays.asList("foo", "bar")).join();
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
