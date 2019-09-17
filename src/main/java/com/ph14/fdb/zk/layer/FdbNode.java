package com.ph14.fdb.zk.layer;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.MoreObjects;

public class FdbNode {

  private final String zkPath;
  private final Stat stat;
  private final byte[] data;
  private final List<ACL> acls;
  private final Optional<Long> ephemeralSessionId;

  public FdbNode(String zkPath, Stat stat, byte[] data, List<ACL> acls) {
    this(zkPath, stat, data, acls, Optional.empty());
  }

  public FdbNode(String zkPath, Stat stat, byte[] data, List<ACL> acls, Optional<Long> ephemeralSessionId) {
    this.zkPath = zkPath;
    this.stat = stat;
    this.data = data;
    this.acls = acls;
    this.ephemeralSessionId = ephemeralSessionId;
  }

  public String getZkPath() {
    return zkPath;
  }

  public List<String> getFdbPath() {
    return FdbPath.toFdbPath(zkPath);
  }

  public Stat getStat() {
    return stat;
  }

  public byte[] getData() {
    return data;
  }

  public List<ACL> getAcls() {
    return acls;
  }

  public Optional<Long> getEphemeralSessionId() {
    return ephemeralSessionId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof FdbNode) {
      final FdbNode that = (FdbNode) obj;
      return Objects.equals(this.getZkPath(), that.getZkPath())
          && Objects.equals(this.getStat(), that.getStat())
          && Objects.equals(this.getData(), that.getData())
          && Objects.equals(this.getAcls(), that.getAcls())
          && Objects.equals(this.getEphemeralSessionId(), that.getEphemeralSessionId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getZkPath(), getStat(), getData(), getAcls(), getEphemeralSessionId());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("zkPath", zkPath)
        .add("stat", stat)
        .add("data", data)
        .add("acls", acls)
        .add("ephemeralSessionId", ephemeralSessionId)
        .toString();
  }
}
