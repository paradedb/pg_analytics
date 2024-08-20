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

use anyhow::Result;
use sqlx::PgConnection;
use tracing_subscriber::{fmt, EnvFilter};

pub mod duckdb_utils;
pub mod print_utils;

pub fn init_tracer() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .try_init()
        .ok();
}

pub async fn execute_query(conn: &mut PgConnection, query: &str) -> Result<()> {
    sqlx::query(query).execute(conn).await?;
    Ok(())
}

pub async fn fetch_results<T>(conn: &mut PgConnection, query: &str) -> Result<Vec<T>>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    let results = sqlx::query_as::<_, T>(query).fetch_all(conn).await?;
    Ok(results)
}
