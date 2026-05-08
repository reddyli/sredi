# SREDI

A high-performance, distributed in-memory storage system written in Java.

## Commands

| Category | Supported |
|----------|-----------|
| Basic | PING, ECHO, GET, SET, DEL, INCR, TYPE, KEYS, INFO, CONFIG, AUTH |
| Lists | LPUSH, RPUSH, LPOP, RPOP, LRANGE |
| Streams | XADD, XRANGE, XREAD |
| Pub/Sub | SUBSCRIBE, UNSUBSCRIBE, PUBLISH |
| Bloom Filter | BF.ADD, BF.EXISTS, BF.RESERVE |
| Replication | PSYNC, REPLCONF, WAIT |
| Transactions | MULTI, EXEC, DISCARD |

## Features

| Feature | Description |
|---------|-------------|
| Streams | Append-only logs with ID generation and range queries |
| Pub/Sub | Channel-based publish/subscribe with local fanout |
| Bloom Filter | Probabilistic set membership |
| Replication | Leader-follower with full resync and command propagation |
| Cluster Mesh | Persistent peer-to-peer TCP control plane between every node |
| Leader Election | Bully algorithm with epoch-stable leadership and automatic failover |
| Heartbeats | Leader-broadcast liveness with follower-side timeout detection |
| Transactions | MULTI/EXEC/DISCARD with per-connection command queuing |
| Persistence | RDB file reading on startup |
| Authentication | Password-based AUTH with per-connection tracking |
| TTL Cleanup | Scheduled active expiration of keys |
| LRU Eviction | Least recently used eviction with `--maxkeys` |
| Max Connections | Semaphore-based connection limiting |
| Rate Limiting | Per-client token bucket |
| Parallel Execution | Opt-in multi-threaded commands with striped read-write locks |
| Backpressure | Bounded replication queue with follower auto-disconnect and reconnection |
| Protocol | RESP (Redis Serialization Protocol) |

## Usage

### Single node

```bash
docker run --rm -p 6379:6379 reddyli/sredi
```

Then in another terminal:

```bash
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
```

### 3-node cluster with automatic failover

```bash
docker network create sredi-net

SPEC='n1@n1:6379,n2@n2:6379,n3@n3:6379'

docker run -d --rm --name n1 --network sredi-net -p 7001:6379 reddyli/sredi --node-id=n1 --cluster="$SPEC"
docker run -d --rm --name n2 --network sredi-net -p 7002:6379 reddyli/sredi --node-id=n2 --cluster="$SPEC"
docker run -d --rm --name n3 --network sredi-net -p 7003:6379 reddyli/sredi --node-id=n3 --cluster="$SPEC"
```

The highest-id node (`n3`) is elected leader. Verify:

```bash
redis-cli -p 7003 INFO replication   # role:master
redis-cli -p 7001 INFO replication   # role:slave of n3
```

Trigger a failover:

```bash
docker stop n3
redis-cli -p 7002 INFO replication   # n2 should now be master
```

Tear down:

```bash
docker rm -f n1 n2 n3
docker network rm sredi-net
```

See [`docs/cluster-mesh-and-election.md`](src/main/resources/docs/cluster-mesh-and-election.md) for the design.

## Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `--port` | Server port | 6379 |
| `--replicaof` | Leader host for replication | - |
| `--replicaof-port` | Leader port for replication | - |
| `--dir` | Directory for persistence files | - |
| `--dbfilename` | RDB filename | dump.rdb |
| `--requirepass` | Password for AUTH | - |
| `--maxkeys` | Max keys before LRU eviction | -1 (no limit) |
| `--maxclients` | Max concurrent connections | 100 |
| `--maxrps` | Max requests per second per client | -1 (no limit) |
| `--parallel` | Enable parallel command execution | false |
| `--parallel-threads` | Number of worker threads | CPU cores |
| `--max-repl-backlog` | Max replication queue size per follower | 25 |
| `--node-id` | Stable id for this node within the cluster | - |
| `--cluster` | Cluster spec: `id@host:port,id@host:port,...` | - |
