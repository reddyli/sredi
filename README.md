# SREDI

A High Performance,distributed in-memory storage system written in Java.

## Features

| Category | Supported |
|----------|-----------|
| Commands | GET, SET, INCR, TYPE, KEYS |
| Streams | XADD, XRANGE, XREAD |
| Replication | PSYNC, REPLCONF, WAIT |
| Transactions | MULTI, EXEC, DISCARD |
| Persistence | RDB file reading |
| Protocol | RESP (Redis Serialization Protocol) |

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


## Architecture

![Request Flow](src/main/resources/images/Request_Flow.png)


## TODO

- [ ] RESP Parser Tests
- [ ] Command Tests (GET, SET, INCR, KEYS, TYPE)
- [ ] Stream Tests (XADD, XRANGE, XREAD)
- [ ] RDB Tests
- [ ] Integration Tests
- [ ] DEL command
- [ ] EXISTS command
- [ ] EXPIRE/TTL/PERSIST commands
- [ ] Lists (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN)
- [ ] RDB Writing (SAVE/BGSAVE)
- [ ] AOF
- [ ] AOF Rewrite
- [ ] SUBSCRIBE/UNSUBSCRIBE
- [ ] PUBLISH
- [ ] PSUBSCRIBE
- [ ] Pipelining
- [ ] AUTH
- [ ] Memory Limits and LRU
- [ ] Leader Election
- [ ] Consistent Hashing
- [ ] Gossip Protocol
