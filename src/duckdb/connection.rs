// Copyright (c) 2023-2024 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use anyhow::{anyhow, Result};
use duckdb::arrow::array::RecordBatch;
use duckdb::{Params, Statement};
use signal_hook::consts::signal::*;
use signal_hook::iterator::Signals;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::Once;
use std::thread;

use crate::env::get_global_connection;

use super::csv;
use super::delta;
use super::iceberg;
use super::parquet;
use super::secret;

// Global mutable static variables
static mut GLOBAL_STATEMENT: Option<UnsafeCell<Option<Statement<'static>>>> = None;
static mut GLOBAL_ARROW: Option<UnsafeCell<Option<duckdb::Arrow<'static>>>> = None;
static INIT: Once = Once::new();

fn init_globals() {
    unsafe {
        GLOBAL_STATEMENT = Some(UnsafeCell::new(None));
        GLOBAL_ARROW = Some(UnsafeCell::new(None));
    }

    thread::spawn(move || {
        let mut signals =
            Signals::new([SIGTERM, SIGINT, SIGQUIT]).expect("error registering signal listener");
        for _ in signals.forever() {
            let conn = get_global_connection().expect("failed to get connection");
            let conn = conn.lock().unwrap();
            conn.interrupt();
        }
    });
}

fn iceberg_loaded() -> Result<bool> {
    let conn = get_global_connection()?;
    let conn = conn.lock().unwrap();
    let mut statement = conn.prepare("SELECT * FROM duckdb_extensions() WHERE extension_name = 'iceberg' AND installed = true AND loaded = true")?;
    match statement.query([])?.next() {
        Ok(Some(_)) => Ok(true),
        _ => Ok(false),
    }
}

fn get_global_statement() -> &'static UnsafeCell<Option<Statement<'static>>> {
    INIT.call_once(|| {
        init_globals();
    });
    unsafe {
        GLOBAL_STATEMENT
            .as_ref()
            .expect("Statement not initialized")
    }
}

fn get_global_arrow() -> &'static UnsafeCell<Option<duckdb::Arrow<'static>>> {
    INIT.call_once(|| {
        init_globals();
    });
    unsafe { GLOBAL_ARROW.as_ref().expect("Arrow not initialized") }
}

pub fn create_csv_relation(
    table_name: &str,
    schema_name: &str,
    table_options: HashMap<String, String>,
) -> Result<usize> {
    let statement = csv::create_duckdb_relation(table_name, schema_name, table_options)?;
    execute(statement.as_str(), [])
}

pub fn create_delta_relation(
    table_name: &str,
    schema_name: &str,
    table_options: HashMap<String, String>,
) -> Result<usize> {
    let statement = delta::create_duckdb_relation(table_name, schema_name, table_options)?;
    execute(statement.as_str(), [])
}

pub fn create_iceberg_relation(
    table_name: &str,
    schema_name: &str,
    table_options: HashMap<String, String>,
) -> Result<usize> {
    if !iceberg_loaded()? {
        execute("INSTALL iceberg", [])?;
        execute("LOAD iceberg", [])?;
    }

    let statement = iceberg::create_duckdb_relation(table_name, schema_name, table_options)?;
    execute(statement.as_str(), [])
}

pub fn create_parquet_relation(
    table_name: &str,
    schema_name: &str,
    table_options: HashMap<String, String>,
) -> Result<usize> {
    let statement = parquet::create_duckdb_relation(table_name, schema_name, table_options)?;
    execute(statement.as_str(), [])
}

pub fn create_arrow(sql: &str) -> Result<bool> {
    unsafe {
        let conn = get_global_connection()?;
        let conn = conn.lock().unwrap();
        let statement = conn.prepare(sql)?;
        let static_statement: Statement<'static> = std::mem::transmute(statement);

        *get_global_statement().get() = Some(static_statement);

        if let Some(static_statement) = get_global_statement().get().as_mut().unwrap() {
            let arrow = static_statement.query_arrow([])?;
            *get_global_arrow().get() = Some(std::mem::transmute::<
                duckdb::Arrow<'_>,
                duckdb::Arrow<'_>,
            >(arrow));
        }
    }

    Ok(true)
}

pub fn clear_arrow() {
    unsafe {
        *get_global_statement().get() = None;
        *get_global_arrow().get() = None;
    }
}

pub fn create_secret(user_mapping_options: HashMap<String, String>) -> Result<usize> {
    const DEFAULT_SECRET: &str = "default_secret";
    let statement = secret::create_secret(DEFAULT_SECRET, user_mapping_options)?;
    execute(statement.as_str(), [])
}

pub fn get_next_batch() -> Result<Option<RecordBatch>> {
    unsafe {
        if let Some(arrow) = get_global_arrow().get().as_mut().unwrap() {
            Ok(arrow.next())
        } else {
            Err(anyhow!("No Arrow batches found in GLOBAL_ARROW"))
        }
    }
}

pub fn get_batches() -> Result<Vec<RecordBatch>> {
    unsafe {
        if let Some(arrow) = get_global_arrow().get().as_mut().unwrap() {
            Ok(arrow.collect())
        } else {
            Err(anyhow!("No Arrow batches found in GLOBAL_ARROW"))
        }
    }
}

pub fn execute<P: Params>(sql: &str, params: P) -> Result<usize> {
    let conn = get_global_connection()?;
    let conn = conn.lock().unwrap();
    conn.execute(sql, params).map_err(|err| anyhow!("{err}"))
}

pub fn drop_relation(table_name: &str, schema_name: &str) -> Result<()> {
    let conn = get_global_connection()?;
    let conn = conn.lock().unwrap();
    let mut statement = conn.prepare(format!("SELECT table_type from information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' LIMIT 1").as_str())?;
    if let Ok(Some(row)) = statement.query([])?.next() {
        let table_type: String = row.get(0)?;
        let table_type = table_type.replace("BASE", "").trim().to_string();
        let statement = format!("DROP {table_type} {schema_name}.{table_name}");
        execute(statement.as_str(), [])?;
    }

    Ok(())
}
