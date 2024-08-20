use anyhow::{anyhow, Result};
use duckdb::{types::FromSql, Connection, ToSql};
use std::path::PathBuf;

pub trait FromDuckDBRow: Sized {
    fn from_row(row: &duckdb::Row<'_>) -> Result<Self>;
}

pub fn fetch_duckdb_results<T>(parquet_path: &PathBuf, query: &str) -> Result<Vec<T>>
where
    T: FromDuckDBRow + Send + 'static,
{
    let conn = Connection::open_in_memory()?;

    // Register the Parquet file as a table
    conn.execute(
        &format!(
            "CREATE TABLE auto_sales AS SELECT * FROM read_parquet('{}')",
            parquet_path.to_str().unwrap()
        ),
        [],
    )?;

    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map([], |row| {
        T::from_row(row).map_err(|e| duckdb::Error::InvalidQuery)
    })?;

    // Collect the results
    let mut results = Vec::new();
    for row in rows {
        results.push(row?);
    }

    Ok(results)
}

// Helper function to convert DuckDB list to Vec<i64>
fn duckdb_list_to_vec(value: duckdb::types::Value) -> Result<Vec<i64>> {
    match value {
        duckdb::types::Value::List(list) => list
            .iter()
            .map(|v| match v {
                duckdb::types::Value::BigInt(i) => Ok(*i),
                _ => Err(anyhow!("Unexpected type in list")),
            })
            .collect(),
        _ => Err(anyhow!("Expected a list")),
    }
}

// Example implementation for (i32, i32, i64, Vec<i64>)
impl FromDuckDBRow for (i32, i32, i64, Vec<i64>) {
    fn from_row(row: &duckdb::Row<'_>) -> Result<Self> {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, i32>(1)?,
            row.get::<_, i64>(2)?,
            duckdb_list_to_vec(row.get::<_, duckdb::types::Value>(3)?)?,
        ))
    }
}

impl FromDuckDBRow for (i32, i32, i64, f64) {
    fn from_row(row: &duckdb::Row<'_>) -> Result<Self> {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, i32>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, f64>(3)?,
        ))
    }
}
