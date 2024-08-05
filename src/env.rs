use duckdb::Connection;
use pgrx::*;
use std::ffi::CStr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct DuckdbConnection(pub Arc<Mutex<Connection>>);
unsafe impl PGRXSharedMemory for DuckdbConnection {}

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

        let conn =
            Connection::open_with_flags(duckdb_path, duckdb::Config::default().threads(1).unwrap())
                .expect("failed to open duckdb connection");
        DuckdbConnection(Arc::new(Mutex::new(conn)))
    }
}

pub static DUCKDB_CONNECTION: PgLwLock<DuckdbConnection> = PgLwLock::new();

pub fn get_global_connection() -> Arc<Mutex<Connection>> {
    (*DUCKDB_CONNECTION.share()).0.clone()
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
