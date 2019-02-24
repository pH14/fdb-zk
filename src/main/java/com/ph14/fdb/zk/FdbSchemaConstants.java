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

  public static final byte[] EMPTY_BYTES = new byte[0];
}
