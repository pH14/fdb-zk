package com.ph14.fdb.zk.layer.changefeed;

import java.util.Objects;

import org.apache.zookeeper.Watcher.Event.EventType;

import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.base.MoreObjects;

public class ChangefeedWatchEvent {

  private final Versionstamp versionstamp;
  private final EventType eventType;
  private final String zkPath;

  public ChangefeedWatchEvent(Versionstamp versionstamp, EventType eventType, String zkPath) {
    this.versionstamp = versionstamp;
    this.eventType = eventType;
    this.zkPath = zkPath;
  }

  public Versionstamp getVersionstamp() {
    return versionstamp;
  }

  public EventType getEventType() {
    return eventType;
  }

  public String getZkPath() {
    return zkPath;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof ChangefeedWatchEvent) {
      final ChangefeedWatchEvent that = (ChangefeedWatchEvent) obj;
      return Objects.equals(this.getVersionstamp(), that.getVersionstamp())
          && Objects.equals(this.getEventType(), that.getEventType())
          && Objects.equals(this.getZkPath(), that.getZkPath());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getVersionstamp(), getEventType(), getZkPath());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("versionstamp", versionstamp)
        .add("eventType", eventType)
        .add("zkPath", zkPath)
        .toString();
  }

}
