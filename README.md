# sredi

A distributed In-Memory storage system written in Java.

## Overview

sredi is a lightweight, high-performance data structure server that supports:
- Key-value storage
- Stream data types
- Replication (leader-follower model)
- Persistence with RDB files
- Transactions

## Features

- **Commands**: GET, SET, DEL, INCR, TYPE, KEYS
- **Streams**: XADD, XRANGE, XREAD
- **Replication**: PSYNC, REPLCONF, WAIT
- **Transactions**: MULTI, DISCARD
- **Server Info**: INFO, PING

## Getting Started

### Prerequisites
- Java 11 or higher
- Gradle

### Running the Server

```bash
# Start as leader
java -jar sredi.jar --port 6379

# Start as follower
java -jar sredi.jar --port 6380 --replicaof localhost 6379
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
