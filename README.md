# SREDI

A Distributed In-Memory storage system written in Java.

## Overview

sredi is a lightweight, high-performance data structure server that supports:
- Key-value storage
- Stream data types
- Asynchronous Replication (Leader-follower model)
- Persistence with RDB files
- Transactions

## Features

- **Commands**: GET, SET, DEL, INCR, TYPE, KEYS
- **Streams**: XADD, XRANGE, XREAD
- **Replication**: PSYNC, REPLCONF, WAIT
- **Transactions**: MULTI, DISCARD
- **Server Info**: INFO, PING

## Getting Started

### Docker

You can quickly get started with sredi using Docker:

```bash
# Pull the image
docker pull reddyli/sredi

# Run as leader
docker run -p 6379:6379 reddyli/sredi

# Run as follower (replace <leader-host> with the actual leader host)
docker run -p 6380:6379 reddyli/sredi --replicaof <leader-host> 6379

# Run with custom configuration
docker run -p 6379:6379 -v /path/to/data:/data reddyli/sredi --dir /data --dbfilename dump.rdb
```

### Configuration Options

- `--port <port>`: Server port (default: 6379)
- `--role <role>`: Server role (master/slave)
- `--replicaof <host>`: Leader host for replication
- `--replicaof-port <port>`: Leader port for replication
- `--dir <directory>`: Directory for persistence files
- `--dbfilename <filename>`: RDB filename for persistence

### Protocol

RESP (Redis Serialization Protocol) is implemented for client-server communication.
