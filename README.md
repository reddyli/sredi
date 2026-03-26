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
| Feature | Description |
|---------|-------------|
| Streams | Append-only logs with ID generation and range queries |
| Replication | Leader-follower with full resync and command propagation |
| Transactions | MULTI/EXEC/DISCARD with per-connection command queuing |
| Persistence | RDB file reading on startup |
| Authentication | Password-based AUTH with per-connection tracking |
| Protocol | RESP (Redis Serialization Protocol) |

### Upcoming
| Feature | Description |
|---------|-------------|
| TTL Background Cleanup | Active expiration thread |
| LRU Eviction | Cache eviction with maxmemory config |
| Rate Limiting | Token bucket algorithm |
| Max Connections | Resource management with semaphores |
| Read-Write Locks | Concurrent reads, exclusive writes |
| Pipelining | Batch command processing |
| Pub/Sub | SUBSCRIBE, PUBLISH messaging |
| Backpressure | Flow control when followers lag |
| Leader Election | Bully algorithm |

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


## Performance

TODO - Benchmarks, throughput limits, and comparison with Redis.
