package com.ph14.fdb.zk.layer;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public class FdbPath {

  private static final Logger LOG = LoggerFactory.getLogger(FdbPath.class);

  public static final String ROOT_PATH = "fdb-zk-root";

  private static final Splitter SPLITTER = Splitter.on("/");
  private static final Joiner JOINER = Joiner.on("/");

  public static List<String> toFdbPath(String zkPath) {
    PathUtils.validatePath(zkPath);

    List<String> path = ImmutableList.copyOf(SPLITTER.split(zkPath))
        .stream()
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());

    if (path.size() == 0) {
      return Collections.singletonList(ROOT_PATH);
    } else {
      return ImmutableList.<String>builder()
          .add(ROOT_PATH)
          .addAll(path)
          .build();
    }
  }

  public static String toZkPath(List<String> fdbPath) {
    return "/" + JOINER.join(fdbPath.subList(1, fdbPath.size()));
  }

  public static List<String> toFdbParentPath(String zkPath) {
    List<String> fullFdbPath = toFdbPath(zkPath);
    return fullFdbPath.subList(0, fullFdbPath.size() - 1);
  }

  public static String toZkParentPath(String zkPath) {
    int lastSlash = zkPath.lastIndexOf('/');

    if (lastSlash == -1 || zkPath.indexOf('\0') != -1) {
//      throw new KeeperException.BadArgumentsException(zkPath);
    }

    // how to handle this properly? how does ZK fix this?
    if (lastSlash == 0) {
      return "/";
    }

    return zkPath.substring(0, lastSlash);
  }

}
