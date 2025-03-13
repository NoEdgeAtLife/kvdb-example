# Toy Key-Value Database

A simple key-value database implementation in Rust with the following features:

- Log-structured storage engine for durability and write efficiency
- In-memory index for fast lookups
- LRU cache for frequently accessed data
- Automatic garbage collection to reclaim space
- gRPC interface for client-server communication

## Architecture

This toy key-value database uses a log-structured approach to storage, where all write operations are appended to the end of a log file. This provides:

1. **Durability**: Data is immediately written to disk
2. **Write efficiency**: Sequential writes are faster than random access
3. **Simplicity**: The implementation is straightforward

The system maintains an in-memory index that maps keys to their locations in the log file, allowing for fast reads. As updates happen, the database may accumulate stale entries, which are periodically cleaned up through garbage collection.

## Components

- **Storage Engine**: Handles reading and writing data to disk
- **In-Memory Index**: Maps keys to locations in the log file
- **LRU Cache**: Stores frequently accessed values to reduce disk I/O
- **Garbage Collection**: Reclaims space by removing stale entries
- **gRPC Service**: Provides a network interface for clients

## Getting Started

### Prerequisites

- Rust and Cargo (1.58.0 or newer recommended)
- Protobuf compiler (required for gRPC)

### Building the Project

```bash
cargo build --release
```

### Running the Server

```bash
cargo run --bin kvdb-server -- [ADDRESS] [DB_PATH]
```

For example:

```bash
cargo run --bin kvdb-server -- [::1]:50051 ./my_database
```

### Using the Client

The client supports three operations: `set`, `get`, and `remove`.

Set a key-value pair:

```bash
cargo run --bin kvdb-client -- --server http://[::1]:50051 set 1 "Hello, World!"
```

Get a value by key:

```bash
cargo run --bin kvdb-client -- --server http://[::1]:50051 get 1
```

Remove a key-value pair:

```bash
cargo run --bin kvdb-client -- --server http://[::1]:50051 remove 1
```

## Implementation Details

### Data Format

The log file stores entries in the following format:

1. Operation type (1 byte): 0 for Set, 1 for Remove
2. Key (8 bytes): Int64 key
3. For Set operations:
   - Value size (8 bytes): Length of the value in bytes
   - Value (variable length): The actual value as bytes
4. For Remove operations: No additional data

### Garbage Collection

Garbage collection is triggered when the log file exceeds a configurable size threshold. During garbage collection:

1. A new temporary file is created
2. Valid key-value pairs are copied to the new file
3. The original file is replaced with the new one
4. The in-memory index is updated to point to the new locations

### Performance Considerations

- The database uses an LRU cache to improve read performance for frequently accessed keys
- All database operations are thread-safe through the use of Rust's synchronization primitives
- The garbage collector is designed to minimize the impact on ongoing operations

## Testing

Run the test suite with:

```bash
cargo test
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- This is a toy project inspired by educational key-value stores like Bitcask and LevelDB
- The implementation is not intended for production use