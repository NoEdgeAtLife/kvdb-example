fn main() {
    println!("Toy Key-Value Database");
    println!("=====================");
    println!("\nThis project provides a simple key-value database with the following components:");
    println!("1. A log-structured storage engine");
    println!("2. An LRU cache for frequently accessed data");
    println!("3. Automatic garbage collection");
    println!("4. A gRPC interface for client-server communication");
    println!("\nTo use this database:");
    println!("\n- Start the server:");
    println!("  cargo run --bin kvdb-server -- [ADDRESS] [DB_PATH]");
    println!("  Example: cargo run --bin kvdb-server -- [::1]:50051 ./my_database");
    println!("\n- Use the client to interact with the server:");
    println!("  cargo run --bin kvdb-client -- --server http://[::1]:50051 set <key> <value>");
    println!("  cargo run --bin kvdb-client -- --server http://[::1]:50051 get <key>");
    println!("  cargo run --bin kvdb-client -- --server http://[::1]:50051 remove <key>");
    println!("\nExample:");
    println!("  cargo run --bin kvdb-client -- set 1 \"Hello, World!\"");
    println!("  cargo run --bin kvdb-client -- get 1");
}
