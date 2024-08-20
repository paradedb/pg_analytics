use anyhow::{anyhow, Result};
use duckdb::Connection;
use pgrx::*;
use std::ffi::CStr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

// One connection per database, so 128 databases can have a DuckDB connection
const MAX_CONNECTIONS: usize = 128;
pub static DUCKDB_CONNECTION_CACHE: PgLwLock<DuckdbConnection> = PgLwLock::new();

pub struct DuckdbConnection {
    conn_map: heapless::FnvIndexMap<u32, DuckdbConnectionInner, MAX_CONNECTIONS>,
    conn_lru: heapless::Deque<u32, MAX_CONNECTIONS>,
}

unsafe impl PGRXSharedMemory for DuckdbConnection {}

impl Default for DuckdbConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl DuckdbConnection {
    fn new() -> Self {
        Self {
            conn_map: heapless::FnvIndexMap::<_, _, MAX_CONNECTIONS>::new(),
            conn_lru: heapless::Deque::<_, MAX_CONNECTIONS>::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct DuckdbConnectionInner(Arc<Mutex<Connection>>);

impl Default for DuckdbConnectionInner {
    fn default() -> Self {
        let mut duckdb_path = postgres_data_dir_path();
        duckdb_path.push("pg_analytics");

        if !duckdb_path.exists() {
            std::fs::create_dir_all(duckdb_path.clone())
                .expect("failed to create duckdb data directory");
        }

        duckdb_path.push(postgres_database_oid().to_string());
        duckdb_path.set_extension("db3");

        let conn = Connection::open(duckdb_path).expect("failed to open duckdb connection");
        DuckdbConnectionInner(Arc::new(Mutex::new(conn)))
    }
}

fn postgres_data_dir_path() -> PathBuf {
    let data_dir = unsafe {
        CStr::from_ptr(pg_sys::DataDir)
            .to_string_lossy()
            .into_owned()
    };
    PathBuf::from(data_dir)
}

fn postgres_database_oid() -> u32 {
    unsafe { pg_sys::MyDatabaseId.as_u32() }
}

#[macro_export]
macro_rules! with_connection {
    ($body:expr) => {{
        let conn = get_global_connection()?;
        let conn = conn
            .lock()
            .map_err(|e| anyhow!("Failed to acquire lock: {}", e))?;
        $body(&*conn) // Dereference the MutexGuard to get &Connection
    }};
}

pub fn get_global_connection() -> Result<Arc<Mutex<Connection>>> {
    let database_id = postgres_database_oid();
    let mut cache = DUCKDB_CONNECTION_CACHE.exclusive();

    if cache.conn_map.contains_key(&database_id) {
        // Move the accessed connection to the back of the LRU queue
        let mut new_lru = heapless::Deque::<_, MAX_CONNECTIONS>::new();
        for &id in cache.conn_lru.iter() {
            if id != database_id {
                new_lru
                    .push_back(id)
                    .unwrap_or_else(|_| panic!("Failed to push to LRU queue"));
            }
        }
        new_lru
            .push_back(database_id)
            .unwrap_or_else(|_| panic!("Failed to push to LRU queue"));
        cache.conn_lru = new_lru;

        // Now we can safely borrow conn_map again
        Ok(cache.conn_map.get(&database_id).unwrap().0.clone())
    } else {
        if cache.conn_map.len() >= MAX_CONNECTIONS {
            if let Some(least_recently_used) = cache.conn_lru.pop_front() {
                cache.conn_map.remove(&least_recently_used);
            }
        }
        let conn = DuckdbConnectionInner::default();
        cache
            .conn_map
            .insert(database_id, conn.clone())
            .map_err(|_| anyhow!("Failed to insert into connection map"))?;
        cache
            .conn_lru
            .push_back(database_id)
            .map_err(|_| anyhow!("Failed to push to LRU queue"))?;
        Ok(conn.0)
    }
}

pub fn interrupt_all_connections() -> Result<()> {
    let cache = DUCKDB_CONNECTION_CACHE.exclusive();
    for &database_id in cache.conn_lru.iter() {
        if let Some(conn) = cache.conn_map.get(&database_id) {
            let conn = conn
                .0
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
            conn.interrupt();
        }
    }
    Ok(())
}
