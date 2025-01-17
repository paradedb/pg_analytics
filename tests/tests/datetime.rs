// Copyright (c) 2023-2025 Retake, Inc.
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

mod fixtures;

use crate::fixtures::db::Query;
use crate::fixtures::duckdb_conn;
use crate::fixtures::tables::duckdb_types::DuckdbTypesTable;
use crate::fixtures::{conn, tempdir};
use anyhow::Result;
use rstest::*;
use sqlx::PgConnection;
use tempfile::TempDir;
use time::macros::datetime;
use time::PrimitiveDateTime;

#[rstest]
async fn test_date_trunc(
    mut conn: PgConnection,
    tempdir: TempDir,
    duckdb_conn: duckdb::Connection,
) -> Result<()> {
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");

    duckdb_conn
        .execute(&DuckdbTypesTable::create_duckdb_table(), [])
        .unwrap();

    duckdb_conn
        .execute(&DuckdbTypesTable::populate_duckdb_table(), [])
        .unwrap();

    duckdb_conn
        .execute(
            &DuckdbTypesTable::export_duckdb_table(parquet_path.to_str().unwrap()),
            [],
        )
        .unwrap();

    DuckdbTypesTable::create_foreign_table(parquet_path.to_str().unwrap()).execute(&mut conn);
    let row: (PrimitiveDateTime,) =
        "SELECT date_trunc('day', timestamp_col) FROM duckdb_types_test".fetch_one(&mut conn);
    assert_eq!(row, (datetime!(2023-06-27 00:00:00),));

    Ok(())
}
