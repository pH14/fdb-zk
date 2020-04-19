## fdb-zk

`fdb-zk` is a FoundationDB layer that mimics the behavior of Zookeeper. It is installed as a local service to an application, and replaces connections to and the operation of a ZooKeeper cluster.

While the core operations are implemented, `fdb-zk` has not been vetted for proper production use.

### Talk & Slides

Learn about how the layer works in greater detail:

* [In video format](https://www.youtube.com/watch?v=3FYpf1QMPgQ)
* [In slidedeck format](https://static.sched.com/hosted_files/foundationdbsummit2019/86/zookeeper_layer.pdf)

#### Architecture

Similar to the [FoundationDB Document Layer](https://foundationdb.github.io/fdb-document-layer/), `fdb-zk` is hosted locally and translates requests for the target service into FoundationDB transactions.

Applications can continue to use their preferred `Zookeeper` clients:

```
┌──────────────────────┐     ┌──────────────────────┐
│ ┌──────────────────┐ │     │ ┌──────────────────┐ │
│ │   Application    │ │     │ │   Application    │ │
│ └──────────────────┘ │     │ └──────────────────┘ │
│           │          │     │           │          │
│           │          │     │           │          │
│       ZooKeeper      │     │       ZooKeeper      │
│        protocol      │     │        protocol      │
│           │          │     │           │          │
│           │          │     │           │          │
│           ▼          │     │           ▼          │
│ ┌──────────────────┐ │     │ ┌──────────────────┐ │
│ │  fdb-zk service  │ │     │ │  fdb-zk service  │ │
│ └──────────────────┘ │     │ └──────────────────┘ │
└──────────────────────┘     └──────────────────────┘
            │                            │
         FDB ops                      FDB ops
            │                            │
            ▼                            ▼
┌───────────────────────────────────────────────────┐
│                   FoundationDB                    │
└───────────────────────────────────────────────────┘
```

### Features

`fdb-zk` implements the core Zookeeper 3.4.6 API:

* `create`
* `exists`
* `delete`
* `getData`
* `setData`
* `getChildren`
* watches
* session management

It partially implements:

* `multi` transactions (reads are fine, but there are no read-your-writes)

It does not yet implement:

* `getACL/setACL`
* quotas

### Initial Design Discussion

https://forums.foundationdb.org/t/fdb-zk-rough-cut-of-zookeeper-api-layer/1278/

### Building with Bazel

* Compiling: `bazel build //:fdb_zk`
* Testing: `bazel test //:fdb_zk_test`
* Dependencies: `bazel query @maven//:all --output=build`

### License

`fdb-zk` is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
