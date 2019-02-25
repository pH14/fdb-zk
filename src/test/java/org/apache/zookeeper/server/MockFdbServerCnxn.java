package org.apache.zookeeper.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.proto.ReplyHeader;

public class MockFdbServerCnxn extends ServerCnxn {

  private final BlockingQueue<WatchedEvent> watchedEvents = new ArrayBlockingQueue<>(10);

  @Override
  public void sendResponse(ReplyHeader h, Record r, String tag) throws IOException {

  }

  @Override
  public void process(WatchedEvent event) {
    watchedEvents.add(event);
  }

  @Override
  protected ServerStats serverStats() {
    return null;
  }

  @Override
  public long getOutstandingRequests() {
    return 0;
  }

  @Override
  public InetSocketAddress getRemoteSocketAddress() {
    return null;
  }

  @Override
  public int getInterestOps() {
    return 0;
  }

  @Override
  int getSessionTimeout() {
    return 0;
  }

  @Override
  void close() {

  }

  @Override
  void sendCloseSession() {

  }

  @Override
  long getSessionId() {
    return 0;
  }

  @Override
  void setSessionId(long sessionId) {

  }

  @Override
  void sendBuffer(ByteBuffer closeConn) {

  }

  @Override
  void enableRecv() {

  }

  @Override
  void disableRecv() {

  }

  @Override
  void setSessionTimeout(int sessionTimeout) {

  }

  public BlockingQueue<WatchedEvent> getWatchedEvents() {
    return watchedEvents;
  }

}
