
# NOTE: `bazel query @maven//:all --output=build`

java_library(
    name = "fdb_zk",
    srcs = glob(["src/main/java/**/*.java"]),
    resources = glob(["src/main/resources/**"]),
    deps = [
          "@maven//:org_foundationdb_fdb_java",
          "@maven//:org_apache_zookeeper_zookeeper",
          "@maven//:com_google_inject_guice",
          "@maven//:com_google_guava_guava",
          "@maven//:com_hubspot_algebra",
          "@maven//:org_slf4j_slf4j_api",
          "@maven//:org_slf4j_slf4j_log4j12",
          "@maven//:io_netty_netty",
    ],
)

java_test(
    name = "fdb_zk_test",
    srcs = glob(["src/test/java/**/*.java"]),
    test_class = "com.ph14.fdb.zk.FdbTest",
    size = "small",
    runtime_deps = [
        
    ],
    deps = [
          "@maven//:junit_junit",
          "@maven//:org_assertj_assertj_core",
          "@maven//:com_google_guava_guava",
          "@maven//:org_foundationdb_fdb_java",
          "@maven//:org_apache_zookeeper_zookeeper",
          "@maven//:com_hubspot_algebra",
          "@maven//:org_slf4j_slf4j_api",
          ":fdb_zk",
    ],
)