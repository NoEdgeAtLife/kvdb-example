use kvdb::{Config, KvDb};
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};

// Include the generated proto code
pub mod kvdb_proto {
    tonic::include_proto!("kvdb");
}

use kvdb_proto::{
    kv_service_server::{KvService, KvServiceServer},
    GetRequest, GetResponse, RemoveRequest, RemoveResponse, SetRequest, SetResponse,
};

// Our KVDB gRPC service implementation
struct KvDbService {
    db: Arc<KvDb>,
}

impl KvDbService {
    fn new(db: KvDb) -> Self {
        Self {
            db: Arc::new(db),
        }
    }
}

// Implement the KvService trait for our service
#[tonic::async_trait]
impl KvService for KvDbService {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        
        // Attempt to set the key-value pair
        match self.db.set(req.key, &req.value) {
            Ok(old_value) => Ok(Response::new(SetResponse {
                success: true,
                old_value: old_value.unwrap_or_default(),
                error: String::new(),
            })),
            Err(err) => Ok(Response::new(SetResponse {
                success: false,
                old_value: String::new(),
                error: format!("{}", err),
            })),
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        
        // Attempt to get the value for the key
        match self.db.get(req.key) {
            Ok(value_opt) => {
                let exists = value_opt.is_some();
                Ok(Response::new(GetResponse {
                    exists,
                    value: value_opt.unwrap_or_default(),
                    error: String::new(),
                }))
            },
            Err(err) => Ok(Response::new(GetResponse {
                exists: false,
                value: String::new(),
                error: format!("{}", err),
            })),
        }
    }

    async fn remove(&self, request: Request<RemoveRequest>) -> Result<Response<RemoveResponse>, Status> {
        let req = request.into_inner();
        
        // Attempt to remove the key
        match self.db.remove(req.key) {
            Ok(old_value) => Ok(Response::new(RemoveResponse {
                success: true,
                old_value: old_value.unwrap_or_default(),
                error: String::new(),
            })),
            Err(err) => Ok(Response::new(RemoveResponse {
                success: false,
                old_value: String::new(),
                error: format!("{}", err),
            })),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    env_logger::init();
    
    // Parse command-line arguments
    let args: Vec<String> = std::env::args().collect();
    let addr = if args.len() > 1 {
        args[1].parse()?
    } else {
        "[::1]:50051".parse()?
    };
    
    let db_path = if args.len() > 2 {
        std::path::PathBuf::from(&args[2])
    } else {
        std::path::PathBuf::from("db")
    };
    
    // Configure and open the database
    let config = Config {
        path: db_path.clone(),
        ..Config::default()
    };
    
    let db = KvDb::open(config)?;
    let service = KvDbService::new(db);
    
    println!("KVDB Server listening on {}", addr);
    println!("Database path: {:?}", db_path);
    
    // Start the gRPC server
    Server::builder()
        .add_service(KvServiceServer::new(service))
        .serve(addr)
        .await?;
    
    Ok(())
}