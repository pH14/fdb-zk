package com.ph14.fdb.zk;

public class FdbSchemaConstants {

  public static final int FDB_MAX_VALUE_SIZE = 100_000;
  public static final int ZK_MAX_DATA_LENGTH = 1_048_576;

  public static final byte[] DATA_KEY = "d".getBytes();
  public static final byte[] ACL_KEY = "a".getBytes();
  public static final byte[] STAT_KEY = "s".getBytes();

  public static final byte[] NODE_CREATED_WATCH_KEY = "w".getBytes();
  public static final byte[] CHILD_CREATED_WATCH_KEY = "c".getBytes();
  public static final byte[] NODE_DATA_UPDATED_KEY = "u".getBytes();

  // Creation ZXID. Use Versionstamp at creation time
  public static final byte[] CZXID_KEY = "sc".getBytes();
  // Modified ZXID. Use Versionstamp at creation + updates
  public static final byte[] MZXID_KEY = "sm".getBytes();
  // Creation timestamp
  public static final byte[] CTIME_KEY = "sct".getBytes();
  // Modified timestamp
  public static final byte[] MTIME_KEY = "smt".getBytes();
  // # of changes to this node
  public static final byte[] VERSION_KEY = "sv".getBytes();
  // # of changes to this node's children
  public static final byte[] CVERSION_KEY = "svc".getBytes();
  // # of changes to this node's ACL
  public static final byte[] AVERSION_KEY = "sva".getBytes();
  // Ephemeral node owner
  public static final byte[] EPHEMERAL_OWNER_KEY = "se".getBytes();
  // Data length
  public static final byte[] DATA_LENGTH_KEY = "sd".getBytes();
  // Number of children
  public static final byte[] NUM_CHILDREN_KEY = "snc".getBytes();

  public static final byte[] EMPTY_BYTES = new byte[0];

}
