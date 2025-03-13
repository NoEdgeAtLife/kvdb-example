use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use lru::LruCache;
use thiserror::Error;

// Define the error types for our database operations
#[derive(Error, Debug)]
pub enum KvError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Invalid data format")]
    InvalidFormat,

    #[error("Database is closed")]
    DbClosed,
}

pub type Result<T> = std::result::Result<T, KvError>;

// The type of operation in our log-structured storage
#[derive(Debug, Clone, Copy, PartialEq)]
enum OpType {
    Set = 0,
    Remove = 1,
}

impl OpType {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(OpType::Set),
            1 => Ok(OpType::Remove),
            _ => Err(KvError::InvalidFormat),
        }
    }
}

// Represents the position of a value in the data file
#[derive(Debug, Clone, Copy)]
struct ValuePos {
    offset: u64,
    size: u64,
}

// Our in-memory index maps keys to their value positions
type MemIndex = HashMap<i64, Option<ValuePos>>;

// The maximum size of our cache in bytes (16MB)
const MAX_CACHE_SIZE: usize = 16 * 1024 * 1024;

// Configuration for the database
#[derive(Debug, Clone)]
pub struct Config {
    // The path to the database files
    pub path: PathBuf,
    
    // The threshold size in bytes to trigger garbage collection
    pub gc_threshold: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: PathBuf::from("db"),
            gc_threshold: 1024 * 1024 * 100, // 100MB
        }
    }
}

// The main database structure
pub struct KvDb {
    config: Config,
    file: Arc<Mutex<File>>,
    index: Arc<RwLock<MemIndex>>,
    // LRU cache using our keys as i64 and values as strings
    // LRU eviction policy is used to keep the most frequently accessed items
    cache: Arc<Mutex<LruCache<i64, String>>>,
    file_size: Arc<Mutex<u64>>,
    closed: Arc<RwLock<bool>>,
}

impl KvDb {
    pub fn open(config: Config) -> Result<Self> {
        // Create the database directory if it doesn't exist
        std::fs::create_dir_all(&config.path)?;
        
        let data_path = config.path.join("data.db");
        
        // Open the data file, create it if it doesn't exist
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&data_path)?;
        
        // Get the current size of the file
        let file_size = file.metadata()?.len();
        
        // Create an empty index
        let index = MemIndex::new();
        
        // Create a new database instance
        let mut db = Self {
            config,
            file: Arc::new(Mutex::new(file)),
            index: Arc::new(RwLock::new(index)),
            // Initialize the cache with a maximum size based on bytes
            cache: Arc::new(Mutex::new(LruCache::unbounded())),
            file_size: Arc::new(Mutex::new(file_size)),
            closed: Arc::new(RwLock::new(false)),
        };
        
        // Load the index from the data file
        db.load_index()?;
        
        Ok(db)
    }
    
    // Load the index by reading through the entire data file
    fn load_index(&mut self) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        let file_size = file.metadata()?.len();
        
        if file_size == 0 {
            return Ok(());
        }
        
        file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&mut *file);
        let mut offset = 0;
        
        // Read through the file and build the index
        while offset < file_size {
            let op_type = OpType::from_u8(reader.read_u8()?)?;
            let key = reader.read_i64::<LittleEndian>()?;
            
            match op_type {
                OpType::Set => {
                    let value_size = reader.read_u64::<LittleEndian>()?;
                    let value_pos = ValuePos {
                        offset: offset + 1 + 8 + 8, // op_type + key + value_size
                        size: value_size,
                    };
                    
                    // Skip over the value content
                    reader.seek(SeekFrom::Current(value_size as i64))?;
                    
                    // Update the index
                    let mut index = self.index.write().unwrap();
                    index.insert(key, Some(value_pos));
                    
                    offset += 1 + 8 + 8 + value_size; // op_type + key + value_size + value
                },
                OpType::Remove => {
                    // Mark the key as removed in the index
                    let mut index = self.index.write().unwrap();
                    index.insert(key, None);
                    
                    offset += 1 + 8; // op_type + key
                }
            }
        }
        
        Ok(())
    }
    
    // Set a key-value pair in the database
    pub fn set(&self, key: i64, value: &str) -> Result<Option<String>> {
        // Check if the database is closed
        if *self.closed.read().unwrap() {
            return Err(KvError::DbClosed);
        }
        
        // Get the old value for the key, if it exists
        let old_value = self.get(key)?;
        
        // Write the new key-value pair to the file
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::End(0))?;
        
        // Write the operation type (Set)
        file.write_u8(OpType::Set as u8)?;
        
        // Write the key
        file.write_i64::<LittleEndian>(key)?;
        
        // Write the value size
        let value_bytes = value.as_bytes();
        file.write_u64::<LittleEndian>(value_bytes.len() as u64)?;
        
        // Write the value
        file.write_all(value_bytes)?;
        file.flush()?;
        
        // Update the file size
        let offset = *self.file_size.lock().unwrap();
        let value_pos = ValuePos {
            offset: offset + 1 + 8 + 8, // op_type + key + value_size
            size: value_bytes.len() as u64,
        };
        
        // Update the file size
        let new_size = offset + 1 + 8 + 8 + value_bytes.len() as u64;
        *self.file_size.lock().unwrap() = new_size;
        
        // Update the index
        let mut index = self.index.write().unwrap();
        index.insert(key, Some(value_pos));
        
        // Update the cache
        let mut cache = self.cache.lock().unwrap();
        self.manage_cache_size(&mut cache, key, value);
        cache.put(key, value.to_string());
        
        // Check if we need to do garbage collection
        if new_size > self.config.gc_threshold {
            drop(file);
            drop(index);
            drop(cache);
            self.garbage_collect()?;
        }
        
        Ok(old_value)
    }
    
    // Get a value from the database
    pub fn get(&self, key: i64) -> Result<Option<String>> {
        // Check if the database is closed
        if *self.closed.read().unwrap() {
            return Err(KvError::DbClosed);
        }
        
        // First check the cache
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(value) = cache.get(&key) {
                return Ok(Some(value.clone()));
            }
        }
        
        // If not in cache, check the index
        let index = self.index.read().unwrap();
        
        match index.get(&key) {
            Some(Some(pos)) => {
                // Read the value from the file
                let mut file = self.file.lock().unwrap();
                file.seek(SeekFrom::Start(pos.offset))?;
                
                let mut value_bytes = vec![0; pos.size as usize];
                file.read_exact(&mut value_bytes)?;
                
                let value = String::from_utf8_lossy(&value_bytes).to_string();
                
                // Update the cache
                let mut cache = self.cache.lock().unwrap();
                self.manage_cache_size(&mut cache, key, &value);
                cache.put(key, value.clone());
                
                Ok(Some(value))
            },
            Some(None) => Ok(None), // Key was removed
            None => Ok(None), // Key doesn't exist
        }
    }
    
    // Remove a key from the database
    pub fn remove(&self, key: i64) -> Result<Option<String>> {
        // Check if the database is closed
        if *self.closed.read().unwrap() {
            return Err(KvError::DbClosed);
        }
        
        // Get the old value for the key, if it exists
        let old_value = match self.get(key)? {
            Some(val) => {
                // Store a copy of the old value to return later
                let old_val = Some(val);
                
                // Write the removal operation to the file
                let mut file = self.file.lock().unwrap();
                file.seek(SeekFrom::End(0))?;
                
                // Write the operation type (Remove)
                file.write_u8(OpType::Remove as u8)?;
                
                // Write the key
                file.write_i64::<LittleEndian>(key)?;
                file.flush()?;
                
                // Update the file size
                let offset = *self.file_size.lock().unwrap();
                *self.file_size.lock().unwrap() = offset + 1 + 8; // op_type + key
                
                // Update the index
                let mut index = self.index.write().unwrap();
                index.insert(key, None);
                
                // Remove from the cache
                let mut cache = self.cache.lock().unwrap();
                cache.pop(&key);
                
                // Check if we need to do garbage collection
                if *self.file_size.lock().unwrap() > self.config.gc_threshold {
                    drop(file);
                    drop(index);
                    drop(cache);
                    self.garbage_collect()?;
                }
                
                old_val
            },
            None => None,
        };
        
        Ok(old_value)
    }
    
    // Close the database
    pub fn close(&self) -> Result<()> {
        let mut closed = self.closed.write().unwrap();
        *closed = true;
        Ok(())
    }
    
    // Manage the cache size to ensure it doesn't exceed MAX_CACHE_SIZE
    fn manage_cache_size(&self, cache: &mut LruCache<i64, String>, key: i64, value: &str) {
        // If the cache already has this key, remove it first to recalculate
        if cache.contains(&key) {
            cache.pop(&key);
        }
        
        // Calculate size of the new entry (key size + value size)
        let new_entry_size = std::mem::size_of::<i64>() + value.len();
        
        // Keep removing entries until we have enough space
        let mut current_size: usize = cache.iter().map(|(_, v)| v.len() + std::mem::size_of::<i64>()).sum();
        
        while current_size + new_entry_size > MAX_CACHE_SIZE && !cache.is_empty() {
            if let Some((_, removed_value)) = cache.pop_lru() {
                current_size -= removed_value.len() + std::mem::size_of::<i64>();
            }
        }
    }

    // Garbage collect the database to reclaim space
    fn garbage_collect(&self) -> Result<()> {
        // Create a temporary file for the new data
        let temp_path = self.config.path.join("temp.db");
        let mut temp_file = File::create(&temp_path)?;
        
        // Get a copy of the current index
        let index = self.index.read().unwrap();
        
        // Initialize a new index for the compacted data
        let mut new_index = MemIndex::new();
        
        // Start at the beginning of the temporary file
        let mut new_offset = 0u64;
        
        // For each key with a value in the index, write it to the new file
        for (&key, &pos_opt) in index.iter() {
            if let Some(pos) = pos_opt {
                // Read the value from the original file
                let mut file = self.file.lock().unwrap();
                file.seek(SeekFrom::Start(pos.offset))?;
                
                let mut value_bytes = vec![0; pos.size as usize];
                file.read_exact(&mut value_bytes)?;
                
                // Write to the new file
                // Write the operation type (Set)
                temp_file.write_u8(OpType::Set as u8)?;
                
                // Write the key
                temp_file.write_i64::<LittleEndian>(key)?;
                
                // Write the value size
                temp_file.write_u64::<LittleEndian>(pos.size)?;
                
                // Write the value
                temp_file.write_all(&value_bytes)?;
                
                // Update the new index
                let new_pos = ValuePos {
                    offset: new_offset + 1 + 8 + 8, // op_type + key + value_size
                    size: pos.size,
                };
                
                new_index.insert(key, Some(new_pos));
                
                // Update the new offset
                new_offset += 1 + 8 + 8 + pos.size; // op_type + key + value_size + value
            } else {
                // This key was removed, just update the index
                new_index.insert(key, None);
            }
        }
        
        // Flush and sync the temporary file
        temp_file.flush()?;
        temp_file.sync_all()?;
        
        // Replace the old file with the new one
        let data_path = self.config.path.join("data.db");
        std::fs::rename(temp_path, &data_path)?;
        
        // Update the file and index
        {
            let mut file_lock = self.file.lock().unwrap();
            *file_lock = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&data_path)?;
            
            *self.file_size.lock().unwrap() = new_offset;
        }
        
        {
            let mut index_lock = self.index.write().unwrap();
            *index_lock = new_index;
        }
        
        Ok(())
    }
}

// Implement Drop for KvDb to ensure resources are properly closed
impl Drop for KvDb {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    
    fn setup_test_db() -> (KvDb, PathBuf) {
        let test_dir = PathBuf::from("test_db");
        // Clean up any previous test data
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();
        
        let config = Config {
            path: test_dir.clone(),
            gc_threshold: 1024 * 1024, // 1MB for tests
        };
        
        let db = KvDb::open(config).unwrap();
        (db, test_dir)
    }
    
    #[test]
    fn test_set_get() {
        let (db, test_dir) = setup_test_db();
        
        // Test setting and getting a key
        assert_eq!(db.set(1, "value1").unwrap(), None);
        assert_eq!(db.get(1).unwrap(), Some("value1".to_string()));
        
        // Test overwriting a key
        assert_eq!(db.set(1, "value2").unwrap(), Some("value1".to_string()));
        assert_eq!(db.get(1).unwrap(), Some("value2".to_string()));
        
        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }
    
    #[test]
    fn test_remove() {
        // Use a unique directory for this test to avoid conflicts
        let test_dir = PathBuf::from("test_remove_db");
        // Clean up any previous test data
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();
        
        let config = Config {
            path: test_dir.clone(),
            gc_threshold: 1024 * 1024, // 1MB for tests
        };
        
        let db = KvDb::open(config).unwrap();
        
        // Set a key
        assert_eq!(db.set(1, "value1").unwrap(), None);
        assert_eq!(db.get(1).unwrap(), Some("value1".to_string()));
        
        // Remove the key
        let remove_result = db.remove(1).unwrap();
        assert_eq!(remove_result, Some("value1".to_string()));
        assert_eq!(db.get(1).unwrap(), None);
        
        // Remove a non-existent key
        assert_eq!(db.remove(2).unwrap(), None);
        
        // Clean up
        drop(db); // Ensure db is closed before removing the directory
        let _ = fs::remove_dir_all(test_dir);
    }
    
    #[test]
    fn test_persistence() {
        let test_dir = PathBuf::from("test_persistence");
        // Clean up any previous test data
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();
        
        let config = Config {
            path: test_dir.clone(),
            gc_threshold: 1024 * 1024, // 1MB for tests
        };
        
        // Create a database and write some data
        {
            let db = KvDb::open(config.clone()).unwrap();
            db.set(1, "value1").unwrap();
            db.set(2, "value2").unwrap();
            db.remove(1).unwrap();
        }
        
        // Open the database again and check the data
        {
            let db = KvDb::open(config).unwrap();
            assert_eq!(db.get(1).unwrap(), None);
            assert_eq!(db.get(2).unwrap(), Some("value2".to_string()));
        }
        
        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }
}