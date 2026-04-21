# SREDI

A High Performance, distributed in-memory storage system written in Java.

## Commands

| Category | Supported |
|----------|-----------|
| Basic | PING, ECHO, GET, SET, DEL, INCR, TYPE, KEYS, INFO, CONFIG, AUTH |
| Lists | LPUSH, RPUSH, LPOP, RPOP, LRANGE |
| Streams | XADD, XRANGE, XREAD |
| Replication | PSYNC, REPLCONF, WAIT |
| Transactions | MULTI, EXEC, DISCARD |

## Features

### Current
| Feature | Description                                              |
|---------|----------------------------------------------------------|
| Streams | Append-only logs with ID generation and range queries    |
| Replication | Leader-follower with full resync and command propagation |
| Transactions | MULTI/EXEC/DISCARD with per-connection command queuing   |
| Persistence | RDB file reading on startup                              |
| Authentication | Password-based AUTH with per-connection tracking         |
| TTL Background Cleanup | Scheduled active expiration of keys |
| LRU Eviction | Least recently used eviction with maxkeys config |
| Max Connections | Semaphore-based connection limiting |
| Rate Limiting | Per-client token bucket rate limiting |
| Parallel Execution | Opt-in multi-threaded commands with striped read-write locks |
| Backpressure | Bounded replication queue with follower auto-disconnect and reconnection |
| Protocol | RESP (Redis Serialization Protocol) |

### Upcoming
| Feature | Description |
|---------|-------------|
| Pub/Sub | SUBSCRIBE, PUBLISH messaging |
| Leader Election | Bully algorithm |
| Consistent Hashing | Key sharding across multiple nodes |

## Getting Started

### Docker

```bash
# Run as leader
docker run -p 6379:6379 reddyli/sredi

# Run as follower
docker run -p 6380:6379 reddyli/sredi --replicaof <leader-host> 6379

# Run with persistence
docker run -p 6379:6379 -v /path/to/data:/data reddyli/sredi --dir /data --dbfilename dump.rdb
```

### Configuration

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


## Performance

TODO - Benchmarks, throughput limits, and comparison with Redis.
