package com.ph14.fdb.zk.ops;

import java.util.concurrent.CompletionException;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.ph14.fdb.zk.layer.FdbWatchManager;

class ThrowingWatchManager extends FdbWatchManager {
  public ThrowingWatchManager(Database database) {
    super(database);
  }

  @Override
  public void triggerNodeCreatedWatch(Transaction transaction, String zkPath) {
    throw new CompletionException(new RuntimeException());
  }

  @Override
  public void triggerNodeUpdatedWatch(Transaction transaction, String zkPath) {
    throw new CompletionException(new RuntimeException());
  }

  @Override
  public void triggerNodeDeletedWatch(Transaction transaction, String zkPath) {
    throw new CompletionException(new RuntimeException());
  }

  @Override
  public void triggerNodeChildrenWatch(Transaction transaction, String zkPath) {
    throw new CompletionException(new RuntimeException());
  }
}
