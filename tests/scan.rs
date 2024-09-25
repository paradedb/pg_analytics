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

mod fixtures;

use crate::fixtures::arrow::{
    delta_primitive_record_batch, primitive_create_foreign_data_wrapper, primitive_create_server,
    primitive_create_table, primitive_create_user_mapping_options, primitive_record_batch,
    primitive_record_batch_single, primitive_setup_fdw_local_file_delta,
    primitive_setup_fdw_local_file_listing, primitive_setup_fdw_s3_delta,
    primitive_setup_fdw_s3_listing, setup_parquet_wrapper_and_server,
};
use crate::fixtures::db::Query;
use crate::fixtures::{conn, duckdb_conn, s3, tempdir, S3};
use anyhow::Result;
use datafusion::parquet::arrow::ArrowWriter;
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use rstest::*;
use sqlx::postgres::types::PgInterval;
use sqlx::types::{BigDecimal, Json, Uuid};
use sqlx::PgConnection;
use std::collections::HashMap;
use std::fs::File;
use std::str::FromStr;
use tempfile::TempDir;
use time::macros::{date, datetime, time};

use crate::fixtures::tables::duckdb_types::DuckdbTypesTable;
use crate::fixtures::tables::nyc_trips::NycTripsTable;

const S3_TRIPS_BUCKET: &str = "test-trip-setup";
const S3_TRIPS_KEY: &str = "test_trip_setup.parquet";

#[rstest]
async fn test_trip_count(#[future(awt)] s3: S3, mut conn: PgConnection) -> Result<()> {
    NycTripsTable::setup().execute(&mut conn);
    let rows: Vec<NycTripsTable> = "SELECT * FROM nyc_trips".fetch(&mut conn);
    s3.client
        .create_bucket()
        .bucket(S3_TRIPS_BUCKET)
        .send()
        .await?;
    s3.create_bucket(S3_TRIPS_BUCKET).await?;
    s3.put_rows(S3_TRIPS_BUCKET, S3_TRIPS_KEY, &rows).await?;

    NycTripsTable::setup_s3_listing_fdw(
        &s3.url.clone(),
        &format!("s3://{S3_TRIPS_BUCKET}/{S3_TRIPS_KEY}"),
    )
    .execute(&mut conn);

    let count: (i64,) = "SELECT COUNT(*) FROM trips".fetch_one(&mut conn);
    assert_eq!(count.0, 100);

    Ok(())
}

#[rstest]
async fn test_arrow_types_s3_listing(#[future(awt)] s3: S3, mut conn: PgConnection) -> Result<()> {
    let s3_bucket = "test-arrow-types-s3-listing";
    let s3_key = "test_arrow_types.parquet";
    let s3_endpoint = s3.url.clone();
    let s3_object_path = format!("s3://{s3_bucket}/{s3_key}");

    let stored_batch = primitive_record_batch()?;
    s3.create_bucket(s3_bucket).await?;
    s3.put_batch(s3_bucket, s3_key, &stored_batch).await?;

    primitive_setup_fdw_s3_listing(&s3_endpoint, &s3_object_path, "primitive").execute(&mut conn);

    let retrieved_batch =
        "SELECT * FROM primitive".fetch_recordbatch(&mut conn, &stored_batch.schema());

    assert_eq!(stored_batch.num_columns(), retrieved_batch.num_columns());
    for field in stored_batch.schema().fields() {
        assert_eq!(
            stored_batch.column_by_name(field.name()),
            retrieved_batch.column_by_name(field.name())
        )
    }

    Ok(())
}

#[rstest]
async fn test_wrong_user_mapping_s3_listing(
    #[future(awt)] s3: S3,
    mut conn: PgConnection,
) -> Result<()> {
    let s3_bucket = "test-wrong-user-mapping-s3-listing";
    let s3_key = "test_wrong_user_mapping_s3_listing.parquet";
    let s3_endpoint = s3.url.clone();
    let s3_object_path = format!("s3://{s3_bucket}/{s3_key}");

    let stored_batch = primitive_record_batch()?;
    s3.create_bucket(s3_bucket).await?;
    s3.put_batch(s3_bucket, s3_key, &stored_batch).await?;

    let create_foreign_data_wrapper = primitive_create_foreign_data_wrapper(
        "parquet_wrapper",
        "parquet_fdw_handler",
        "parquet_fdw_validator",
    );
    let create_user_mapping_options =
        primitive_create_user_mapping_options("public", "parquet_server");
    let create_server = primitive_create_server("parquet_server", "parquet_wrapper");
    let create_table = primitive_create_table("parquet_server", "primitive");

    // this is the wrong user mapping because the type is not provided
    let wrong_user_mapping = format!(
        r#"
        {create_foreign_data_wrapper};
        {create_server};
        {create_user_mapping_options} OPTIONS (region 'us-east-1', endpoint '{s3_endpoint}', use_ssl 'false', url_style 'path');
        {create_table} OPTIONS (files '{s3_object_path}');
    "#
    );

    let result = wrong_user_mapping.execute_result(&mut conn);
    assert!(result.is_err());

    Ok(())
}

#[rstest]
async fn test_arrow_types_s3_delta(
    #[future(awt)] s3: S3,
    mut conn: PgConnection,
    tempdir: TempDir,
) -> Result<()> {
    let s3_bucket = "test-arrow-types-s3-delta";
    let s3_path = "test_arrow_types";
    let s3_endpoint = s3.url.clone();
    let s3_object_path = format!("s3://{s3_bucket}/{s3_path}");
    let temp_path = tempdir.path();

    let batch = delta_primitive_record_batch()?;

    let delta_schema = deltalake::kernel::Schema::try_from(batch.schema().as_ref())?;
    let mut table = CreateBuilder::new()
        .with_location(temp_path.to_string_lossy())
        .with_columns(delta_schema.fields().to_vec())
        .await?;
    let mut writer = RecordBatchWriter::for_table(&table)?;
    writer.write(batch.clone()).await?;
    writer.flush_and_commit(&mut table).await?;

    s3.create_bucket(s3_bucket).await?;
    s3.put_directory(s3_bucket, s3_path, temp_path).await?;

    primitive_setup_fdw_s3_delta(&s3_endpoint, &s3_object_path, "delta_primitive")
        .execute(&mut conn);

    let retrieved_batch =
        "SELECT * FROM delta_primitive".fetch_recordbatch(&mut conn, &batch.schema());

    assert_eq!(batch.num_columns(), retrieved_batch.num_columns());
    for field in batch.schema().fields() {
        assert_eq!(
            batch.column_by_name(field.name()),
            retrieved_batch.column_by_name(field.name())
        )
    }

    Ok(())
}

#[rstest]
async fn test_arrow_types_local_file_listing(
    mut conn: PgConnection,
    tempdir: TempDir,
) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(parquet_path.as_path().to_str().unwrap(), "primitive")
        .execute(&mut conn);

    let retrieved_batch =
        "SELECT * FROM primitive".fetch_recordbatch(&mut conn, &stored_batch.schema());

    assert_eq!(stored_batch.num_columns(), retrieved_batch.num_columns());
    for field in stored_batch.schema().fields() {
        assert_eq!(
            stored_batch.column_by_name(field.name()),
            retrieved_batch.column_by_name(field.name())
        )
    }

    Ok(())
}

#[rstest]
async fn test_arrow_types_local_file_delta(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let temp_path = tempdir.path();
    let batch = delta_primitive_record_batch()?;
    let delta_schema = deltalake::kernel::Schema::try_from(batch.schema().as_ref())?;
    let mut table = CreateBuilder::new()
        .with_location(temp_path.to_string_lossy().as_ref())
        .with_columns(delta_schema.fields().to_vec())
        .await?;
    let mut writer = RecordBatchWriter::for_table(&table)?;
    writer.write(batch.clone()).await?;
    writer.flush_and_commit(&mut table).await?;

    primitive_setup_fdw_local_file_delta(&temp_path.to_string_lossy(), "delta_primitive")
        .execute(&mut conn);

    let retrieved_batch =
        "SELECT * FROM delta_primitive".fetch_recordbatch(&mut conn, &batch.schema());

    assert_eq!(batch.num_columns(), retrieved_batch.num_columns());
    for field in batch.schema().fields() {
        assert_eq!(
            batch.column_by_name(field.name()),
            retrieved_batch.column_by_name(field.name())
        )
    }

    Ok(())
}

#[rstest]
async fn test_duckdb_types_parquet_local(
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
    let row: Vec<DuckdbTypesTable> = "SELECT * FROM duckdb_types_test".fetch(&mut conn);

    assert_eq!(
        row,
        vec![DuckdbTypesTable {
            bool_col: true,
            tinyint_col: 127,
            smallint_col: 32767,
            integer_col: 2147483647,
            bigint_col: 9223372036854775807,
            utinyint_col: 255,
            usmallint_col: 65535,
            uinteger_col: 4294967295,
            ubigint_col: BigDecimal::from_str("18446744073709551615").unwrap(),
            float_col: 1.2300000190734863,
            double_col: 2.34,
            date_col: date!(2023 - 06 - 27),
            time_col: time!(12:34:56),
            timestamp_col: datetime!(2023-06-27 12:34:56),
            interval_col: PgInterval {
                months: 0,
                days: 1,
                microseconds: 0
            },
            hugeint_col: 1.2345678901234567e19,
            uhugeint_col: 1.2345678901234567e19,
            varchar_col: "Example text".to_string(),
            blob_col: "\x41".to_string(),
            decimal_col: BigDecimal::from_str("12345.6700").unwrap(),
            timestamp_s_col: datetime!(2023-06-27 12:34:56),
            timestamp_ms_col: datetime!(2023-06-27 12:34:56),
            timestamp_ns_col: datetime!(2023-06-27 12:34:56),
            list_col: vec![1, 2, 3],
            struct_col: Json(HashMap::from_iter(vec![
                ("b".to_string(), "def".to_string()),
                ("a".to_string(), "abc".to_string())
            ])),
            array_col: [1, 2, 3],
            uuid_col: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            time_tz_col: time!(12:34:56),
            timestamp_tz_col: datetime!(2023-06-27 10:34:56 +00:00:00),
        }]
    );

    Ok(())
}

#[rstest]
async fn test_create_heap_from_parquet(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(parquet_path.as_path().to_str().unwrap(), "primitive")
        .execute(&mut conn);

    "CREATE TABLE primitive_copy AS SELECT * FROM primitive".execute(&mut conn);

    let count: (i64,) = "SELECT COUNT(*) FROM primitive_copy".fetch_one(&mut conn);
    assert_eq!(count.0, 3);

    Ok(())
}

#[rstest]
async fn test_quals_pushdown(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(parquet_path.as_path().to_str().unwrap(), "primitive")
        .execute(&mut conn);

    "CREATE TABLE t1 (a int);".execute(&mut conn);

    let test_case: Vec<(&str, &str, &str, i32)> = vec![
        ("boolean_col", "false", "false", 0),
        ("int8_col", "-1", "-1", -1),
        ("int16_col", "0", "0", 0),
        ("int32_col", "1", "1", 1),
        ("int64_col", "-1", "-1", -1),
        ("uint8_col", "0", "0", 0),
        ("uint16_col", "1", "1", 1),
        ("uint32_col", "2", "2", -1),
        ("uint64_col", "0", "0", 0),
        ("float32_col", "1.0", "1", 1),
        ("float64_col", "-1.0", "-1", -1),
        ("date32_col", r#"'2020-01-01'"#, r#"'2020-01-01'"#, 1),
        ("date64_col", r#"'2021-01-02'"#, r#"'2021-01-02'"#, -1),
        (
            "binary_col",
            r#"decode(encode('hello', 'hex'),'hex')"#,
            r#"'\x68\x65\x6C\x6C\x6F'"#,
            1,
        ),
        ("binary_col", r#"E''"#, r#"''"#, -1),
        (
            "large_binary_col",
            r#"'\x68656C6C6F'"#,
            r#"'\x68\x65\x6C\x6C\x6F'"#,
            1,
        ),
        (
            "large_binary_col",
            r#"'\x70617271756574'"#,
            r#"'\x70\x61\x72\x71\x75\x65\x74'"#,
            0,
        ),
        ("utf8_col", "'Hello'", "'Hello'", 1),
        ("utf8_col", "'There'", "'There'", -1),
        ("large_utf8_col", "'Hello'", "'Hello'", 1),
        ("large_utf8_col", "'World'", "'World'", 0),
    ];

    for (col_name, val, plan_val, res) in test_case {
        let where_clause = format!("{col_name} = {val}");
        // The condition in the clause may undergo simplification
        let plan_clause = format!("{col_name} = {plan_val}");

        // prevent executor push down, make sure it goes FDW (by using LEFT JOIN with normal postgres table)
        let query =
            format!("SELECT int32_col from primitive LEFT JOIN t1 on true WHERE {where_clause}");
        let explain: Vec<(String,)> = format!("EXPLAIN {query}").fetch(&mut conn);

        assert!(
            explain[3].0.contains(&plan_clause),
            "explain plan error: explain: {}\nplan_clause: {}\n",
            explain[3].0,
            plan_clause,
        );
        // make sure the result is correct
        let rows: Vec<(i32,)> = query.clone().fetch(&mut conn);
        assert!(
            rows.len() == 1,
            "result error: rows length: {}\nquery: {}\n",
            rows.len(),
            query
        );
        assert_eq!(
            res, rows[0].0,
            "result error: expect: {},  result: {} \n query: {}",
            res, rows[0].0, query
        );
    }
    Ok(())
}

#[rstest]
async fn test_complex_quals_pushdown(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(parquet_path.as_path().to_str().unwrap(), "primitive")
        .execute(&mut conn);

    "CREATE TABLE t1 (a int);".execute(&mut conn);

    let query = r#"SELECT int64_col
            FROM primitive LEFT JOIN t1 ON true
        WHERE (
            boolean_col = TRUE
            AND int8_col = 1
            AND int16_col = 1
            AND int32_col = 1
            AND int64_col = 1
            AND uint8_col = 1
            AND uint16_col = 1
            AND uint32_col = 1
            AND uint64_col = 1
            AND float32_col = 1.0
            AND float64_col = 1.0
            AND date32_col = DATE '2020-01-01'
            AND date64_col = TIMESTAMP '2021-01-01'
            AND binary_col = E'\\x68656c6c6f'
            AND large_binary_col = E'\\x68656c6c6f'
            AND utf8_col = 'Hello'
            AND large_utf8_col = 'Hello'
        )
        OR (
            boolean_col = FALSE
            AND int8_col = 0
            AND int16_col = 0
            AND int32_col = 0
            AND int64_col = 0
            AND uint8_col = 0
            AND uint16_col = 0
            AND uint32_col = 0
            AND uint64_col = 0
            AND float32_col = 0.0
            AND float64_col = 0.0
            AND date32_col = DATE '2020-01-03'
            AND date64_col = TIMESTAMP '2021-01-03'
            AND binary_col = E'\\x70617271756574'
            AND large_binary_col = E'\\x70617271756574'
            AND utf8_col = 'World'
            AND large_utf8_col = 'World'
        );"#;

    // make sure the result is correct with complex clauses.
    let rows: Vec<(i64,)> = query.fetch(&mut conn);

    // TODO: check the plan. Wrappers not parse quals correctly. So there is not qual pushdown
    assert!(
        rows.len() == 2,
        "result error: rows length: {}\nquery: {}\n",
        rows.len(),
        query
    );

    assert_eq!(
        1, rows[0].0,
        "result error: expect: {}, result: {} \n query: {}",
        1, rows[0].0, query
    );

    assert_eq!(
        0, rows[1].0,
        "result error: expect: {}, result: {} \n query: {}",
        0, rows[1].0, query
    );

    Ok(())
}

#[rstest]
async fn test_executor_hook_search_path(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    "CREATE SCHEMA tpch1".execute(&mut conn);
    "CREATE SCHEMA tpch2".execute(&mut conn);

    let file_path = parquet_path.as_path().to_str().unwrap();

    primitive_setup_fdw_local_file_listing(file_path, "t3").execute(&mut conn);

    let create_table_t1 = primitive_create_table("parquet_server", "tpch1.t1");

    let create_table_t2 = primitive_create_table("parquet_server", "tpch2.t2");

    (&format!("{create_table_t1} OPTIONS (files '{file_path}');")).execute(&mut conn);
    (&format!("{create_table_t2} OPTIONS (files '{file_path}');")).execute(&mut conn);

    // Set force executor hook pushdown
    "SET paradedb.disable_fdw = true".execute(&mut conn);

    let ret = "SELECT * FROM t1".execute_result(&mut conn);
    assert!(ret.is_err(), "{:?}", ret);

    let ret = "SELECT * FROM t2".execute_result(&mut conn);
    assert!(ret.is_err(), "{:?}", ret);

    let ret = "SELECT * FROM t3".execute_result(&mut conn);
    assert!(ret.is_ok(), "{:?}", ret);

    let ret = "SELECT * FROM t3 LEFT JOIN tpch1.t1 ON TRUE".execute_result(&mut conn);
    assert!(ret.is_ok(), "{:?}", ret);

    // Set search path
    "SET search_path TO tpch1, tpch2, public".execute(&mut conn);

    let ret = "SELECT * FROM t1".execute_result(&mut conn);
    assert!(ret.is_ok(), "{:?}", ret);

    let ret = "SELECT * FROM t2".execute_result(&mut conn);
    assert!(ret.is_ok(), "{:?}", ret);

    let ret = "SELECT * FROM t3".execute_result(&mut conn);
    assert!(ret.is_ok(), "{:?}", ret);

    let ret =
        "SELECT * FROM t1 LEFT JOIN t2 ON true LEFT JOIN t3 on true".execute_result(&mut conn);
    assert!(ret.is_ok(), "{:?}", ret);

    Ok(())
}


#[rstest]
async fn test_prepare_stmt_execute(#[future(awt)] s3: S3, mut conn: PgConnection) -> Result<()> {
    NycTripsTable::setup().execute(&mut conn);
    let rows: Vec<NycTripsTable> = "SELECT * FROM nyc_trips".fetch(&mut conn);
    s3.client
        .create_bucket()
        .bucket(S3_TRIPS_BUCKET)
        .send()
        .await?;
    s3.create_bucket(S3_TRIPS_BUCKET).await?;
    s3.put_rows(S3_TRIPS_BUCKET, S3_TRIPS_KEY, &rows).await?;

    NycTripsTable::setup_s3_listing_fdw(
        &s3.url.clone(),
        &format!("s3://{S3_TRIPS_BUCKET}/{S3_TRIPS_KEY}"),
    )
    .execute(&mut conn);

    r#"PREPARE test_query(int) AS SELECT count(*) FROM trips WHERE "VendorID" = $1;"#
        .execute(&mut conn);

    let count: (i64,) = "EXECUTE test_query(1)".fetch_one(&mut conn);
    assert_eq!(count.0, 39);

    let count: (i64,) = "EXECUTE test_query(3)".fetch_one(&mut conn);
    assert_eq!(count.0, 0);

    "DEALLOCATE test_query".execute(&mut conn);

    assert!("EXECUTE test_query(3)".execute_result(&mut conn).is_err());

    Ok(())
}

// Note: PostgreSQL will replan the query when certain catalog changes occur,
// such as changes to the search path or when a table is deleted.
// In contrast, DuckDB does not replan when the search path is changed.
// If there are two foreign tables in different schemas and the prepared statements do not specify the schemas,
// it may lead to ambiguity or errors when referencing the tables.
#[rstest]
async fn test_prepare_search_path(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    let stored_batch_less = primitive_record_batch_single()?;
    let less_parquet_path = tempdir.path().join("test_arrow_types_less.parquet");
    let less_parquet_file = File::create(&less_parquet_path)?;

    let mut writer =
        ArrowWriter::try_new(less_parquet_file, stored_batch_less.schema(), None).unwrap();
    writer.write(&stored_batch_less)?;
    writer.close()?;

    // In this example, we create two tables with identical structures and names, but in different schemas.
    // We expect that when the search path is changed, the correct table (the one in the current schema) will be referenced in DuckDB.
    "CREATE SCHEMA tpch1".execute(&mut conn);
    "CREATE SCHEMA tpch2".execute(&mut conn);

    setup_parquet_wrapper_and_server().execute(&mut conn);

    let file_path = parquet_path.as_path().to_str().unwrap();
    let file_less_path = less_parquet_path.as_path().to_str().unwrap();

    let create_table_t1 = primitive_create_table("parquet_server", "tpch1.t1");
    (&format!("{create_table_t1} OPTIONS (files '{file_path}');")).execute(&mut conn);

    let create_table_less_t1 = primitive_create_table("parquet_server", "tpch2.t1");
    (&format!("{create_table_less_t1} OPTIONS (files '{file_less_path}');")).execute(&mut conn);

    "SET search_path TO tpch1".execute(&mut conn);

    "PREPARE q1 AS SELECT * FROM t1 WHERE boolean_col = $1".execute(&mut conn);

    let result: Vec<(bool,)> = "EXECUTE q1(true)".fetch_collect(&mut conn);
    assert_eq!(result.len(), 2);

    "SET search_path TO tpch2".execute(&mut conn);
    let result: Vec<(bool,)> = "EXECUTE q1(true)".fetch_collect(&mut conn);
    assert_eq!(result.len(), 1);

    "DEALLOCATE q1".execute(&mut conn);
    assert!("EXECUTE q1(true)".execute_result(&mut conn).is_err());

    Ok(())
}

// Test view creation with foreign table
#[rstest]
async fn test_view_foreign_table(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = primitive_record_batch()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(parquet_path.as_path().to_str().unwrap(), "primitive")
        .execute(&mut conn);

    // fully pushdown to the DuckDB
    "CREATE VIEW primitive_view AS SELECT * FROM primitive".execute(&mut conn);
    let res: (bool,) = "SELECT boolean_col FROM primitive_view".fetch_one(&mut conn);
    assert!(res.0);

    // cannot fully pushdown to the DuckDB
    "CREATE TABLE t1 (a int);".execute(&mut conn);
    "INSERT INTO t1 VALUES (1);".execute(&mut conn);
    r#"
    CREATE VIEW primitive_join_view AS
    SELECT *
    FROM primitive
    JOIN t1 ON t1.a = primitive.int32_col
    "#
    .execute(&mut conn);

    let res: (i32,) = "SELECT int32_col FROM primitive_join_view".fetch_one(&mut conn);
    assert_eq!(res.0, 1);
    Ok(())
}