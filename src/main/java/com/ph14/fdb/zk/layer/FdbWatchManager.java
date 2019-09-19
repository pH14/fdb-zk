package com.ph14.fdb.zk.layer;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.apple.foundationdb.Transaction;
import com.google.inject.Inject;
import com.ph14.fdb.zk.layer.changefeed.WatchEventChangefeed;

// TODO: Still need this class?
public class FdbWatchManager {

  private final WatchEventChangefeed watchChangefeed;

  @Inject
  public FdbWatchManager(WatchEventChangefeed watchChangefeed) {
    this.watchChangefeed = watchChangefeed;
  }

  public void checkForWatches(long sessionId, Watcher watcher) {
    watchChangefeed.playChangefeed(sessionId, watcher);
  }

  public void triggerNodeCreatedWatch(Transaction transaction, String zkPath) {
    watchChangefeed.appendToChangefeed(transaction, EventType.NodeCreated, zkPath).join();
  }

  public void triggerNodeUpdatedWatch(Transaction transaction, String zkPath) {
    watchChangefeed.appendToChangefeed(transaction, EventType.NodeDataChanged, zkPath).join();
  }

  public void triggerNodeDeletedWatch(Transaction transaction, String zkPath) {
    watchChangefeed.appendToChangefeed(transaction, EventType.NodeDeleted, zkPath).join();
  }

  public void triggerNodeChildrenWatch(Transaction transaction, String zkPath) {
    watchChangefeed.appendToChangefeed(transaction, EventType.NodeChildrenChanged, zkPath).join();
  }

  public void addNodeCreatedWatch(Transaction transaction, String zkPath, Watcher watcher, long sessionId) {
    watchChangefeed.setZKChangefeedWatch(transaction, watcher, sessionId, EventType.NodeCreated, zkPath);
  }

  public void addNodeDataUpdatedWatch(Transaction transaction, String zkPath, Watcher watcher, long sessionId) {
    watchChangefeed.setZKChangefeedWatch(transaction, watcher, sessionId, EventType.NodeDataChanged, zkPath);
  }

  public void addNodeDeletedWatch(Transaction transaction, String zkPath, Watcher watcher, long sessionId) {
    watchChangefeed.setZKChangefeedWatch(transaction, watcher, sessionId, EventType.NodeDeleted, zkPath);
  }

  public void addNodeChildrenWatch(Transaction transaction, String zkPath, Watcher watcher, long sessionId) {
    watchChangefeed.setZKChangefeedWatch(transaction, watcher, sessionId, EventType.NodeChildrenChanged, zkPath);
  }

}
