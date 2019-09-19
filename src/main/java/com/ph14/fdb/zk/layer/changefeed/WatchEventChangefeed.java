package com.ph14.fdb.zk.layer.changefeed;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class WatchEventChangefeed {

  private static final String ACTIVE_WATCH_NAMESPACE = "fdb-zk-watch-active";
  private static final String CHANGEFEED_TRIGGER_NAMESPACE = "fdb-zk-watch-trigger";
  private static final String CHANGEFEED_NAMESPACE = "fdb-zk-watch-cf";

  private static final byte[] EMPTY_VALUE = new byte[0];

  // TODO: this can currently grow without bound, since it doesn't evict stale Watchers
  private final ConcurrentHashMap<Watcher, ReentrantLock> changefeedLocks;

  private final Database database;

  @Inject
  public WatchEventChangefeed(Database database) {
    this.database = database;
    this.changefeedLocks = new ConcurrentHashMap<>();
  }

  /**
   * Sets a ZK Watch on a particular ZKPath for a given event type.
   *
   * To do this, it:
   *
   * 1. Marks that the client is actively watching a given (zkPath, eventType)
   * 2. Subscribes the client to a FDB watch for the ZK Watch Trigger
   */
  public CompletableFuture<Void> setZKChangefeedWatch(Transaction transaction, Watcher watcher, long sessionId, EventType eventType, String zkPath) {
    // register as a ZK watcher for a given path + event type
    transaction.set(getActiveWatchKey(sessionId, zkPath, eventType), EMPTY_VALUE);

    // subscribe to the trigger-key, to avoid having to poll for watch events
    CompletableFuture<Void> triggerFuture = transaction.watch(getTriggerKey(sessionId, eventType));
    return triggerFuture.whenComplete((v, e) -> playChangefeed(sessionId, watcher));
  }

  /**
   * * Add an update to the active-watcher changefeeds for a (session, watchEventType, zkPath)
   * * Triggers all FDB watches for the changefeed
   */
  public void appendToChangefeed(Transaction transaction, EventType eventType, String zkPath) {
    Range activeWatches = Range.startsWith(Tuple.from(ACTIVE_WATCH_NAMESPACE, zkPath, eventType.getIntValue()).pack());
    List<KeyValue> keyValues = transaction.getRange(activeWatches).asList().join();

    for (KeyValue keyValue : keyValues) {
      long sessionId = Tuple.fromBytes(keyValue.getKey()).getLong(3);

      Tuple changefeedKey = Tuple.from(CHANGEFEED_NAMESPACE, sessionId, Versionstamp.incomplete(), eventType.getIntValue());
      // append the event to the changefeed of each ZK watcher
      transaction.mutate(MutationType.SET_VERSIONSTAMPED_KEY, changefeedKey.packWithVersionstamp(), Tuple.from(zkPath).pack());

      // update the watched trigger-key for each ZK watcher, so they know to check their changefeeds for updates
      transaction.mutate(
          MutationType.SET_VERSIONSTAMPED_VALUE,
          Tuple.from(CHANGEFEED_TRIGGER_NAMESPACE, sessionId, eventType.getIntValue()).pack(),
          Tuple.from(Versionstamp.incomplete()).packWithVersionstamp());
    }

    transaction.clear(activeWatches);
  }

  public void playChangefeed(long sessionId, Watcher watcher) {
    synchronized (changefeedLocks) {
      changefeedLocks.computeIfAbsent(watcher, x -> new ReentrantLock());
    }

    try {
      // We don't want concurrent changefeed lookups to duplicate events to the same client.
      changefeedLocks.get(watcher).lock();

      Range allAvailableZKWatchEvents = Range.startsWith(Tuple.from(CHANGEFEED_NAMESPACE, sessionId).pack());

      List<ChangefeedWatchEvent> watchEvents = database.run(
          transaction -> transaction.getRange(allAvailableZKWatchEvents).asList())
          .thenApply(kvs -> kvs.stream()
              .map(WatchEventChangefeed::toWatchEvent)
              .collect(ImmutableList.toImmutableList()))
          .join();

      if (watchEvents.isEmpty()) {
        return;
      }

      Set<ByteBuffer> seenCommitVersionstamps = new HashSet<>(watchEvents.size());
      for (ChangefeedWatchEvent watchEvent : watchEvents) {
        ByteBuffer watchCommitVersion = ByteBuffer.wrap(watchEvent.getVersionstamp().getTransactionVersion());

        // in the case that one transaction committed multiple watches for different event types (e.g. getData),
        // we only want to trigger one of the watches. TBH this might only be needed for one of the test cases, not sure
        if (seenCommitVersionstamps.add(watchCommitVersion)) {
          watcher.process(new WatchedEvent(watchEvent.getEventType(), KeeperState.SyncConnected, watchEvent.getZkPath()));
        }
      }

      Versionstamp greatestVersionstamp = Iterables.getLast(watchEvents).getVersionstamp();
      database.run(transaction -> {
        // if watch entries were written between reading and executing, we want to only remove entries up to what we just had
        byte[] lastProcessedEvent = ByteArrayUtil.strinc(Tuple.from(CHANGEFEED_NAMESPACE, sessionId, greatestVersionstamp).pack());
        transaction.clear(allAvailableZKWatchEvents.begin, lastProcessedEvent);

        return null;
      });
    } finally {
      changefeedLocks.get(watcher).unlock();
    }
  }

  public CompletableFuture<Void> clearAllWatchesForSession(Transaction transaction, long sessionId) {
    Range allAvailableZKWatchEvents = Range.startsWith(Tuple.from(CHANGEFEED_NAMESPACE, sessionId).pack());

    return transaction.getRange(allAvailableZKWatchEvents).asList()
        .thenApply(kvs -> {
          kvs.stream()
              .map(WatchEventChangefeed::toWatchEvent)
              .forEach(watchEvent -> {
                transaction.clear(getActiveWatchKey(sessionId, watchEvent.getZkPath(), watchEvent.getEventType()));
                transaction.clear(getTriggerKey(sessionId, watchEvent.getEventType()));
              });

          transaction.clear(allAvailableZKWatchEvents);
          return null;
        });
  }

  private static byte[] getActiveWatchKey(long sessionId, String zkPath, EventType eventType) {
    return Tuple.from(ACTIVE_WATCH_NAMESPACE, zkPath, eventType.getIntValue(), sessionId).pack();
  }

  private static byte[] getTriggerKey(long sessionId, EventType eventType) {
    return Tuple.from(CHANGEFEED_TRIGGER_NAMESPACE, sessionId, eventType.getIntValue()).pack();
  }

  private static ChangefeedWatchEvent toWatchEvent(KeyValue keyValue) {
    Tuple tuple = Tuple.fromBytes(keyValue.getKey());
    Versionstamp versionstamp = tuple.getVersionstamp(2);
    EventType eventType = EventType.fromInt((int) tuple.getLong(3)); // tuple layer upcasts ints to longs
    String zkPath = Tuple.fromBytes(keyValue.getValue()).getString(0);

    return new ChangefeedWatchEvent(versionstamp, eventType, zkPath);
  }

}
