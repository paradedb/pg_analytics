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

use crate::fixtures::arrow::{
    primitive_record_batch, primitive_setup_fdw_local_file_listing, record_batch_with_casing,
    reserved_column_record_batch, setup_local_file_listing_with_casing,
    setup_parquet_wrapper_and_server,
};
use crate::fixtures::db::Query;
use crate::fixtures::{conn, tempdir};
use anyhow::Result;
use datafusion::parquet::arrow::ArrowWriter;
use rstest::*;
use sqlx::PgConnection;
use std::fs::File;
use tempfile::TempDir;

#[rstest]
async fn test_table_case_sensitivity(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(
        parquet_path.as_path().to_str().unwrap(),
        "\"PrimitiveTable\"",
    )
    .execute(&mut conn);

    let retrieved_batch =
        "SELECT * FROM \"PrimitiveTable\"".fetch_recordbatch(&mut conn, &stored_batch.schema());

    assert_eq!(stored_batch.num_columns(), retrieved_batch.num_columns());

    let retrieved_batch = "SELECT * FROM public.\"PrimitiveTable\""
        .fetch_recordbatch(&mut conn, &stored_batch.schema());

    assert_eq!(stored_batch.num_columns(), retrieved_batch.num_columns());

    let retrieved_batch = "SELECT * FROM \"public\".\"PrimitiveTable\""
        .fetch_recordbatch(&mut conn, &stored_batch.schema());

    assert_eq!(stored_batch.num_columns(), retrieved_batch.num_columns());

    Ok(())
}

#[rstest]
async fn test_reserved_table_name(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    match primitive_setup_fdw_local_file_listing(
        parquet_path.as_path().to_str().unwrap(),
        "duckdb_types",
    )
    .execute_result(&mut conn)
    {
        Ok(_) => {
            panic!("should have failed to create table with reserved name")
        }
        Err(e) => {
            assert_eq!(e.to_string(), "error returned from database: Table name 'duckdb_types' is not allowed because it is reserved by DuckDB")
        }
    }

    Ok(())
}

#[rstest]
fn test_reserved_column_name(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = reserved_column_record_batch()?;
    let parquet_path = tempdir.path().join("reserved_column_table.parquet");
    let parquet_file = File::create(&parquet_path).unwrap();

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    setup_parquet_wrapper_and_server().execute(&mut conn);

    match format!(
        "CREATE FOREIGN TABLE reserved_table_name () SERVER parquet_server OPTIONS (files '{}', preserve_casing 'true')",
        parquet_path.to_str().unwrap()
    )
    .execute_result(&mut conn)
    {
        Ok(_) => {}
        Err(e) => {
            panic!("fail to create table with reserved column name: {}", e)
        }
    }

    Ok(())
}

#[rstest]
async fn test_invalid_file(mut conn: PgConnection) -> Result<()> {
    match primitive_setup_fdw_local_file_listing("invalid_file.parquet", "primitive")
        .execute_result(&mut conn)
    {
        Ok(_) => panic!("should have failed to create table with invalid file"),
        Err(e) => {
            assert_eq!(
                e.to_string(),
                "error returned from database: IO Error: No files found that match the pattern \"invalid_file.parquet\""
            )
        }
    }

    Ok(())
}

#[rstest]
async fn test_recreated_view(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(parquet_path.as_path().to_str().unwrap(), "primitive")
        .execute(&mut conn);

    "DROP FOREIGN TABLE primitive".execute(&mut conn);
    format!(
        "CREATE FOREIGN TABLE primitive (id INT) SERVER parquet_server OPTIONS (files '{}')",
        parquet_path.to_str().unwrap()
    )
    .execute(&mut conn);

    Ok(())
}

#[rstest]
async fn test_preserve_casing(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = record_batch_with_casing()?;
    let parquet_path = tempdir.path().join("test_casing.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    setup_local_file_listing_with_casing(parquet_path.as_path().to_str().unwrap(), "primitive")
        .execute(&mut conn);

    let retrieved_batch: Vec<(bool,)> = "SELECT \"Boolean_Col\" FROM primitive".fetch(&mut conn);
    assert_eq!(retrieved_batch.len(), 3);

    Ok(())
}

#[rstest]
async fn test_preserve_casing_table_name(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(parquet_path.as_path().to_str().unwrap(), "MyTable")
        .execute(&mut conn);

    format!(
        r#"CREATE FOREIGN TABLE "PrimitiveTable" () SERVER parquet_server OPTIONS (files '{}', preserve_casing 'true')"#,
        parquet_path.to_str().unwrap()
    ).execute(&mut conn);

    let count: (i64,) = r#"SELECT COUNT(*) FROM "PrimitiveTable" LIMIT 1"#.fetch_one(&mut conn);
    assert_eq!(count.0, 3);

    Ok(())
}

#[rstest]
async fn test_table_with_custom_schema(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(parquet_path.as_path().to_str().unwrap(), "MyTable")
        .execute(&mut conn);

    // test quoted schema name
    "CREATE SCHEMA \"MY_SCHEMA\"".to_string().execute(&mut conn);
    match
        format!( "CREATE FOREIGN TABLE \"MY_SCHEMA\".\"MyTable\" () SERVER parquet_server OPTIONS (files '{}', preserve_casing 'true')",
                parquet_path.to_str().unwrap()
        ).execute_result(&mut conn) {
        Ok(_) => {}
        Err(error) => {
            panic!(
                "should have successfully created table with custom schema \"MY_SCHEMA\".\"MyTable\": {}",
                error
            );
        }
    }

    // test non-quoted schema name
    "CREATE SCHEMA MY_SCHEMA".to_string().execute(&mut conn);
    match
        format!("CREATE FOREIGN TABLE MY_SCHEMA.MyTable () SERVER parquet_server OPTIONS (files '{}', preserve_casing 'true')",
                parquet_path.to_str().unwrap()
        ).execute_result(&mut conn) {
        Ok(_) => {}
        Err(error) => {
            panic!(
                "should have successfully created table with custom schema MY_SCHEMA.MyTable: {}",
                error
            );
        }
    }

    Ok(())
}

#[rstest]
async fn test_configure_columns(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");

    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(
        parquet_path.as_path().to_str().unwrap(),
        "primitive_table",
    )
    .execute(&mut conn);

    format!(
        r#"CREATE FOREIGN TABLE primitive () SERVER parquet_server OPTIONS (files '{}', select 'boolean_col AS bool_col, 2020 as year')"#,
        parquet_path.to_str().unwrap()
    ).execute(&mut conn);

    let retrieved_batch: Vec<(bool, i32)> =
        "SELECT bool_col, year FROM primitive LIMIT 1".fetch(&mut conn);
    assert_eq!(retrieved_batch, vec![(true, 2020)]);

    Ok(())
}
