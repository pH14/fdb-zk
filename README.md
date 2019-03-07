## fdb-zk

`fdb-zk` is a FoundationDB layer that mimics the behavior of Zookeeper. It is installed as a local service to an application, and replaces connections to and the operation of a ZooKeeper cluster.

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


### License

`fdb-zk` is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
