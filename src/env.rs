use duckdb::Connection;
use pgrx::*;
use std::ffi::CStr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct DuckdbConnection(pub Arc<Mutex<Connection>>);
unsafe impl PGRXSharedMemory for DuckdbConnection {}

// One connection per database, so 128 databases can have a DuckDB connection
const MAX_CONNECTIONS: usize = 128;
pub static DUCKDB_CONNECTION: PgLwLock<
    heapless::FnvIndexMap<u32, DuckdbConnection, MAX_CONNECTIONS>,
> = PgLwLock::new();

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

pub fn get_global_connection() -> Arc<Mutex<Connection>> {
    match DUCKDB_CONNECTION.exclusive().entry(postgres_database_oid()) {
        heapless::Entry::Occupied(entry) => entry.get().0.clone(),
        heapless::Entry::Vacant(entry) => {
            let conn = DuckdbConnection::default();
            let _ = entry.insert(conn.clone());
            conn.0.clone()
        }
    }
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
