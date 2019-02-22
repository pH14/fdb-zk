package com.ph14.fdb.zk;

import org.junit.After;
import org.junit.Before;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;

public class FdbBaseTest {

  protected static final String BASE_PATH = "/foo";
  protected static final String SUBPATH = "/foo/bar";

  private Database fdb;
  protected Transaction transaction;

  @Before
  public void setUp() {
    this.fdb = FDB.selectAPIVersion(600).open();
    this.transaction = fdb.createTransaction();
  }

  @After
  public void tearDown() {
    this.transaction.cancel();
    this.fdb.close();
  }

}
