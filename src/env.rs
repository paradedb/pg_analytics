use anyhow::{anyhow, Result};
use duckdb::Connection;
use pgrx::*;
use std::collections::VecDeque;
use std::ffi::CStr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct DuckdbConnection(pub Arc<Mutex<Connection>>);
unsafe impl PGRXSharedMemory for DuckdbConnection {}

// One connection per database, so 128 databases can have a DuckDB connection
const MAX_CONNECTIONS: usize = 128;
pub static DUCKDB_CONNECTION_CACHE: PgLwLock<
    heapless::FnvIndexMap<u32, DuckdbConnection, MAX_CONNECTIONS>,
> = PgLwLock::new();

pub static mut DUCKDB_CONNECTION_LRU: PgLwLock<heapless::Deque<u32, 128>> = PgLwLock::new();

impl Default for DuckdbConnection {
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
        DuckdbConnection(Arc::new(Mutex::new(conn)))
    }
}

pub fn get_global_connection() -> Result<Arc<Mutex<Connection>>> {
    let database_id = postgres_database_oid();

    let mut cache = DUCKDB_CONNECTION_CACHE.exclusive();
    let mut conn_order = unsafe { DUCKDB_CONNECTION_LRU.exclusive() };

    if cache.get(&database_id).is_some() {
        // Since heapless::Deque does not have a method to retain elements that satisfy a predicate.
        // Following implementation is O(n) but it is acceptable since MAX_CONNECTIONS is small.
        let mut new_order: heapless::Deque<u32, 128> = heapless::Deque::new();

        for &id in conn_order.iter() {
            if id != database_id {
                new_order.push_back(id).ok();
            }
        }

        conn_order.clear();
        for id in new_order.iter() {
            conn_order.push_back(*id).ok();
        }

        let _ = conn_order.push_back(database_id);
    } else {
        if cache.len() >= MAX_CONNECTIONS {
            if let Some(least_recently_used) = conn_order.pop_front() {
                cache.remove(&least_recently_used);
            }
        }

        let conn = DuckdbConnection::default();
        let _ = cache.insert(database_id, conn);
        let _ = conn_order.push_back(database_id);
    }

    Ok(cache
        .get(&database_id)
        .ok_or_else(|| anyhow!("connection not found"))?
        .0
        .clone())
}

pub fn postgres_data_dir_path() -> PathBuf {
    let data_dir = unsafe {
        CStr::from_ptr(pg_sys::DataDir)
            .to_string_lossy()
            .into_owned()
    };
    PathBuf::from(data_dir)
}

pub fn postgres_database_oid() -> u32 {
    unsafe { pg_sys::MyDatabaseId.as_u32() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_behavior() {
        {
            let mut cache = DUCKDB_CONNECTION_CACHE.exclusive();
            let mut order: PgLwLockExclusiveGuard<heapless::Deque<u32, 128>> =
                unsafe { DUCKDB_CONNECTION_LRU.exclusive() };
            cache.clear();
            order.clear();
        }

        // Insert MAX_CONNECTIONS + 1 connections to test eviction
        for i in 0..=MAX_CONNECTIONS as u32 {
            unsafe {
                pg_sys::MyDatabaseId = i.into();
            }

            let _ = get_global_connection().expect("Failed to get a connection");
        }

        // Verify that the cache size does not exceed MAX_CONNECTIONS
        let cache = DUCKDB_CONNECTION_CACHE.share();
        assert_eq!(cache.len(), MAX_CONNECTIONS);

        // Verify that the first inserted connection was evicted
        let order = unsafe { DUCKDB_CONNECTION_LRU.share() };
        let cache = DUCKDB_CONNECTION_CACHE.share();

        assert!(!cache.contains_key(&0));
        assert_eq!(order.len(), MAX_CONNECTIONS);

        // Ensure all other inserted connections are still present
        for i in 1..=MAX_CONNECTIONS as u32 {
            assert!(cache.contains_key(&i));
        }
    }
}
