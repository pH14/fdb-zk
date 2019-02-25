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
import com.ph14.fdb.zk.layer.FdbNodeStatReader;
import com.ph14.fdb.zk.layer.FdbNodeStatWriter;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.ops.FdbCreate;
import com.ph14.fdb.zk.ops.FdbExists;
import com.ph14.fdb.zk.ops.FdbGetData;
import com.ph14.fdb.zk.ops.FdbSetData;

public class FdbBaseTest {

  protected static final String BASE_PATH = "/foo";
  protected static final String SUBPATH = "/foo/bar";
  protected static final MockFdbServerCnxn SERVER_CNXN = new MockFdbServerCnxn();
  protected static final Request REQUEST = new Request(SERVER_CNXN, System.currentTimeMillis(), 1, 2, null, Collections.emptyList());

  protected FdbNodeStatWriter fdbNodeStatWriter;
  protected FdbNodeWriter fdbNodeWriter;
  protected FdbWatchManager fdbWatchManager;
  protected FdbNodeStatReader fdbStatReader;
  protected FdbNodeReader fdbNodeReader;

  protected FdbCreate fdbCreate;
  protected FdbGetData fdbGetData;
  protected FdbSetData fdbSetData;
  protected FdbExists fdbExists;

  protected Database fdb;
  protected Transaction transaction;

  @Before
  public void setUp() {
    this.fdb = FDB.selectAPIVersion(600).open();

    fdbNodeStatWriter = new FdbNodeStatWriter();
    fdbNodeWriter = new FdbNodeWriter(fdbNodeStatWriter);
    fdbWatchManager = new FdbWatchManager();
    fdbStatReader = new FdbNodeStatReader();
    fdbNodeReader = new FdbNodeReader(fdbStatReader);

    fdbCreate = new FdbCreate(fdbNodeWriter);
    fdbGetData = new FdbGetData(fdbNodeReader, fdbWatchManager);
    fdbSetData = new FdbSetData(fdbNodeWriter);
    fdbExists = new FdbExists(fdbNodeReader);

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
