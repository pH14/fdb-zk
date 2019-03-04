package com.ph14.fdb.zk.layer;

import static com.ph14.fdb.zk.layer.FdbPath.ROOT_PATH;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class FdbPathTest {

  @Test
  public void itConvertsToFdbPath() {
    List<String> path = FdbPath.toFdbPath("/abc/foo/bar");
    assertThat(path).containsExactly(ROOT_PATH, "abc", "foo", "bar");
  }

  @Test
  public void itConvertsToFdbParentPath() {
    List<String> path = FdbPath.toFdbParentPath("/abc/foo/bar");
    assertThat(path).containsExactly(ROOT_PATH, "abc", "foo");
  }

  @Test
  public void itConvertsRoot() {
    List<String> path = FdbPath.toFdbPath("/");
    assertThat(path).containsExactly(ROOT_PATH);
  }

  @Test
  public void itConvertsBackAndForth() {
    assertThat(FdbPath.toZkPath(FdbPath.toFdbPath("/abc/foo/bar"))).isEqualTo("/abc/foo/bar");
  }

  @Test
  public void itConvertsParentPathBackAndForth() {
    assertThat(FdbPath.toZkPath(FdbPath.toFdbParentPath("/abc/foo/bar"))).isEqualTo("/abc/foo");
  }

  @Test
  public void itConvertsToZkPath() {
    String path = FdbPath.toZkPath(ImmutableList.of(ROOT_PATH, "abc", "foo", "bar"));
    assertThat(path).isEqualTo("/abc/foo/bar");
  }

}
