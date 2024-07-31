use pgrx::*;
use std::ffi::CStr;
use std::path::PathBuf;

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
