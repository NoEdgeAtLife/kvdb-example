use clap::{Parser, Subcommand};
use tonic::Request;

// Include the generated proto code
pub mod kvdb_proto {
    tonic::include_proto!("kvdb");
}

use kvdb_proto::{
    kv_service_client::KvServiceClient, GetRequest, RemoveRequest, SetRequest,
};

#[derive(Parser)]
#[clap(author, version, about = "KVDB Client")]
struct Cli {
    /// Server address
    #[clap(short, long, default_value = "http://[::1]:50051")]
    server: String,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Set a key-value pair
    Set {
        /// The key (an integer)
        key: i64,
        /// The value (a string)
        value: String,
    },
    /// Get a value by key
    Get {
        /// The key to look up
        key: i64,
    },
    /// Remove a key-value pair
    Remove {
        /// The key to remove
        key: i64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command-line arguments
    let cli = Cli::parse();

    // Connect to the server
    let mut client = KvServiceClient::connect(cli.server).await?;

    // Execute the appropriate command
    match cli.command {
        Commands::Set { key, value } => {
            let request = Request::new(SetRequest { key, value });
            let response = client.set(request).await?;
            let resp = response.into_inner();

            if resp.success {
                if !resp.old_value.is_empty() {
                    println!("Successfully updated key: {}. Old value: {}", key, resp.old_value);
                } else {
                    println!("Successfully set key: {}", key);
                }
            } else {
                eprintln!("Failed to set key: {}. Error: {}", key, resp.error);
            }
        }
        Commands::Get { key } => {
            let request = Request::new(GetRequest { key });
            let response = client.get(request).await?;
            let resp = response.into_inner();

            if resp.exists {
                println!("Value for key {}: {}", key, resp.value);
            } else if resp.error.is_empty() {
                println!("Key not found: {}", key);
            } else {
                eprintln!("Error retrieving key {}: {}", key, resp.error);
            }
        }
        Commands::Remove { key } => {
            let request = Request::new(RemoveRequest { key });
            let response = client.remove(request).await?;
            let resp = response.into_inner();

            if resp.success {
                println!("Successfully removed key: {}", key);
                if !resp.old_value.is_empty() {
                    println!("Old value was: {}", resp.old_value);
                }
            } else if resp.error.is_empty() {
                println!("Key not found: {}", key);
            } else {
                eprintln!("Failed to remove key: {}. Error: {}", key, resp.error);
            }
        }
    }

    Ok(())
}