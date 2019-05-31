## fdb-zk

`fdb-zk` is a FoundationDB layer that mimics the behavior of Zookeeper. It is installed as a local service to an application, and replaces connections to and the operation of a ZooKeeper cluster.

While the core CRUD operations are implemented, `fdb-zk` is not remotely ready for production use. Outside of a general need for rigorous testing, it does not yet support the [crucial](https://github.com/pH14/fdb-zk/issues/3) [features](https://github.com/pH14/fdb-zk/issues/4) that make Zookeeper special.

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

TODO:
- [ ] `multi`
- [ ] `getACL`
- [ ] `setACL`

### Further Discussion

https://forums.foundationdb.org/t/fdb-zk-rough-cut-of-zookeeper-api-layer/1278/

### Building with Bazel

* Compiling: `bazel build //:fdb_zk`
* Testing: `bazel test //:fdb_zk_test`
* Dependencies: `bazel query @maven//:all --output=build`

### License

`fdb-zk` is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
