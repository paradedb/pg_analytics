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

use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::parquet::arrow::ArrowWriter;
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use fixtures::*;
use rstest::*;
use shared::fixtures::arrow::{
    delta_primitive_record_batch, primitive_record_batch, primitive_setup_fdw_local_file_delta,
    primitive_setup_fdw_local_file_listing, primitive_setup_fdw_s3_delta,
    primitive_setup_fdw_s3_listing, setup_fdw_local_parquet_file_listing, FieldSpec,
};
use shared::fixtures::tempfile::TempDir;
use sqlx::postgres::types::PgInterval;
use sqlx::types::{BigDecimal, Json, JsonValue, Uuid};
use sqlx::PgConnection;
use std::collections::HashMap;
use std::str::FromStr;
use time::macros::{date, datetime, time};

const S3_TRIPS_BUCKET: &str = "test-trip-setup";
const S3_TRIPS_KEY: &str = "test_trip_setup.parquet";

fn json_string_record_batch() -> Result<(RecordBatch, FieldSpec)> {
    let field_spec = FieldSpec::from(vec![
        ("id_col", DataType::Int64, false, "bigint"),
        ("small_col", DataType::Utf8, false, "text"),
        ("large_col", DataType::LargeUtf8, false, "text"),
    ]);

    let data = vec![
        Some("[]"),
        Some("{}"),
        Some(r#"{ "name": "joe", "age": 12 }"#),
        Some(r#"["john", "jane"]"#),
    ];

    let schema = Arc::new(field_spec.arrow_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(data.clone())),
            Arc::new(LargeStringArray::from(data)),
        ],
    )?;

    Ok((batch, field_spec))
}

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
#[ignore = "bug in DuckDB delta_scan over custom endpoints"]
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
async fn test_json_conversion_from_text(mut conn: PgConnection) -> Result<()> {
    let query_str = r#"SELECT to_json('foo'::text), to_jsonb('bar'::text), '{"a": 2}'::json"#;
    let fetched_row = query_str
        .fetch_result::<(Json<String>, Json<String>, Json<HashMap<String, i64>>)>(&mut conn)?;

    let expected_row = vec![(
        Json::from("foo".to_string()),
        Json::from("bar".to_string()),
        Json::from(
            [("a".to_string(), 2)]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        ),
    )];
    assert_eq!(fetched_row, expected_row);
    Ok(())
}

#[rstest]
async fn test_json_conversion_with_field_access(
    mut conn: PgConnection,
    tempdir: TempDir,
) -> Result<()> {
    let (stored_batch, fields_spec) = json_string_record_batch()?;
    let parquet_path = tempdir
        .path()
        .join("test_json_cast_with_string_col.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    setup_fdw_local_parquet_file_listing(
        parquet_path.as_path().to_str().unwrap(),
        "json_table",
        &fields_spec.postgres_schema(),
    )
    .execute(&mut conn);

    let fetched_rows = "SELECT small_col::json->>'name', large_col::jsonb->>'age' FROM json_table WHERE id_col = 3"
        .fetch_result::<(String, String)>(&mut conn)?;
    assert_eq!(fetched_rows.len(), 1);
    let expected_row = vec![("joe".to_string(), "12".to_string())];
    assert_eq!(fetched_rows, expected_row);

    let fetched_rows = "SELECT small_col::json, large_col::jsonb FROM json_table WHERE id_col = 4"
        .fetch_result::<(Json<JsonValue>, Json<JsonValue>)>(&mut conn)?;
    assert_eq!(fetched_rows.len(), 1);
    let expected_row = vec![(
        Json::from(JsonValue::from(vec![
            "john".to_string(),
            "jane".to_string(),
        ])),
        Json::from(JsonValue::from(vec![
            "john".to_string(),
            "jane".to_string(),
        ])),
    )];
    assert_eq!(fetched_rows, expected_row);

    Ok(())
}
