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

use crate::fixtures::duckdb_conn;
use crate::fixtures::tables::duckdb_types::DuckdbTypesTable;
use crate::fixtures::tables::nyc_trips::NycTripsTable;
use crate::fixtures::{
    conn, tempdir, time_series_record_batch_minutes, time_series_record_batch_years,
};
use anyhow::Result;
use chrono::NaiveDateTime;
use datafusion::parquet::arrow::ArrowWriter;
use fixtures::arrow::primitive_setup_fdw_local_file_listing;
use paradedb_sqllogictest::engine::Query;
use rstest::*;
use sqlx::types::BigDecimal;
use sqlx::PgConnection;
use std::fs::File;
use std::str::FromStr;
use tempfile::TempDir;
use time::macros::datetime;
use time::{Date, Month::January, PrimitiveDateTime};

#[rstest]
async fn test_time_bucket_minutes_duckdb(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = time_series_record_batch_minutes()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(parquet_path.as_path().to_str().unwrap(), "MyTable")
        .execute(&mut conn);

    format!(
        "CREATE FOREIGN TABLE timeseries () SERVER parquet_server OPTIONS (files '{}')",
        parquet_path.to_str().unwrap()
    )
    .execute(&mut conn);

    match "SELECT time_bucket(INTERVAL '2 DAY') AS bucket, AVG(value) as avg_value FROM timeseries GROUP BY bucket ORDER BY bucket;".execute_result(&mut conn) {
        Ok(_) => {
            panic!(
                "should have failed call to time_bucket() for timeseries data with incorrect parameters"
            );
        }
        Err(err) => {
            assert_eq!("error returned from database: function time_bucket(interval) does not exist", err.to_string());
        }
    }

    let data: Vec<(NaiveDateTime, BigDecimal)> = "SELECT time_bucket(INTERVAL '10 MINUTE', timestamp::TIMESTAMP) AS bucket, AVG(value) as avg_value FROM timeseries GROUP BY bucket ORDER BY bucket;"
        .fetch_result(&mut conn).unwrap();

    assert_eq!(2, data.len());

    let expected: Vec<(NaiveDateTime, BigDecimal)> = vec![
        ("1970-01-01T00:00:00".parse()?, BigDecimal::from_str("3")?),
        ("1970-01-01T00:10:00".parse()?, BigDecimal::from_str("8")?),
    ];
    assert_eq!(expected, data);

    let data: Vec<(NaiveDateTime, BigDecimal)> = "SELECT time_bucket(INTERVAL '1 MINUTE', timestamp::TIMESTAMP) AS bucket, AVG(value) as avg_value FROM timeseries GROUP BY bucket ORDER BY bucket;"
        .fetch_result(&mut conn).unwrap();

    assert_eq!(10, data.len());

    let expected: Vec<(NaiveDateTime, BigDecimal)> = vec![
        ("1970-01-01T00:01:00".parse()?, BigDecimal::from_str("1")?),
        ("1970-01-01T00:02:00".parse()?, BigDecimal::from_str("-1")?),
        ("1970-01-01T00:03:00".parse()?, BigDecimal::from_str("0")?),
        ("1970-01-01T00:04:00".parse()?, BigDecimal::from_str("2")?),
        ("1970-01-01T00:05:00".parse()?, BigDecimal::from_str("3")?),
        ("1970-01-01T00:06:00".parse()?, BigDecimal::from_str("4")?),
        ("1970-01-01T00:07:00".parse()?, BigDecimal::from_str("5")?),
        ("1970-01-01T00:08:00".parse()?, BigDecimal::from_str("6")?),
        ("1970-01-01T00:09:00".parse()?, BigDecimal::from_str("7")?),
        ("1970-01-01T00:10:00".parse()?, BigDecimal::from_str("8")?),
    ];
    assert_eq!(expected, data);

    let data: Vec<(NaiveDateTime, BigDecimal)> = "SELECT time_bucket(INTERVAL '10 MINUTE', timestamp::TIMESTAMP, INTERVAL '5 MINUTE') AS bucket, AVG(value) as avg_value FROM timeseries GROUP BY bucket ORDER BY bucket;"
        .fetch_result(&mut conn).unwrap();
    assert_eq!(2, data.len());

    let expected: Vec<(NaiveDateTime, BigDecimal)> = vec![
        (
            "1969-12-31T23:55:00".parse()?,
            BigDecimal::from_str("0.5000")?,
        ),
        (
            "1970-01-01T00:05:00".parse()?,
            BigDecimal::from_str("5.5000")?,
        ),
    ];
    assert_eq!(expected, data);

    Ok(())
}

#[rstest]
async fn test_time_bucket_years_duckdb(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = time_series_record_batch_years()?;
    let parquet_path = tempdir.path().join("test_arrow_types.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_setup_fdw_local_file_listing(parquet_path.as_path().to_str().unwrap(), "MyTable")
        .execute(&mut conn);

    format!(
        "CREATE FOREIGN TABLE timeseries () SERVER parquet_server OPTIONS (files '{}')",
        parquet_path.to_str().unwrap()
    )
    .execute(&mut conn);

    match "SELECT time_bucket(INTERVAL '2 DAY') AS bucket, AVG(value) as avg_value FROM timeseries GROUP BY bucket ORDER BY bucket;".execute_result(&mut conn) {
        Ok(_) => {
            panic!(
                "should have failed call to time_bucket() for timeseries data with incorrect parameters"
            );
        }
        Err(err) => {
            assert_eq!("error returned from database: function time_bucket(interval) does not exist", err.to_string());
        }
    }

    let data: Vec<(Date, BigDecimal)> = "SELECT time_bucket(INTERVAL '1 YEAR', timestamp::DATE) AS bucket, AVG(value) as avg_value FROM timeseries GROUP BY bucket ORDER BY bucket;"
        .fetch_result(&mut conn).unwrap();

    assert_eq!(10, data.len());

    let expected: Vec<(Date, BigDecimal)> = vec![
        (
            Date::from_calendar_date(1970, January, 1)?,
            BigDecimal::from_str("1")?,
        ),
        (
            Date::from_calendar_date(1971, January, 1)?,
            BigDecimal::from_str("-1")?,
        ),
        (
            Date::from_calendar_date(1972, January, 1)?,
            BigDecimal::from_str("0")?,
        ),
        (
            Date::from_calendar_date(1973, January, 1)?,
            BigDecimal::from_str("2")?,
        ),
        (
            Date::from_calendar_date(1974, January, 1)?,
            BigDecimal::from_str("3")?,
        ),
        (
            Date::from_calendar_date(1975, January, 1)?,
            BigDecimal::from_str("4")?,
        ),
        (
            Date::from_calendar_date(1976, January, 1)?,
            BigDecimal::from_str("5")?,
        ),
        (
            Date::from_calendar_date(1977, January, 1)?,
            BigDecimal::from_str("6")?,
        ),
        (
            Date::from_calendar_date(1978, January, 1)?,
            BigDecimal::from_str("7")?,
        ),
        (
            Date::from_calendar_date(1979, January, 1)?,
            BigDecimal::from_str("8")?,
        ),
    ];
    assert_eq!(expected, data);

    let data: Vec<(Date, BigDecimal)> = "SELECT time_bucket(INTERVAL '5 YEAR', timestamp::DATE) AS bucket, AVG(value) as avg_value FROM timeseries GROUP BY bucket ORDER BY bucket;"
        .fetch_result(&mut conn).unwrap();

    assert_eq!(2, data.len());

    let expected: Vec<(Date, BigDecimal)> = vec![
        (
            Date::from_calendar_date(1970, January, 1)?,
            BigDecimal::from_str("1")?,
        ),
        (
            Date::from_calendar_date(1975, January, 1)?,
            BigDecimal::from_str("6")?,
        ),
    ];
    assert_eq!(expected, data);

    let data: Vec<(Date, BigDecimal)> = "SELECT time_bucket(INTERVAL '2 YEAR', timestamp::DATE, DATE '1969-01-01') AS bucket, AVG(value) as avg_value FROM timeseries GROUP BY bucket ORDER BY bucket;"
        .fetch_result(&mut conn).unwrap();

    assert_eq!(6, data.len());

    let expected: Vec<(Date, BigDecimal)> = vec![
        (
            Date::from_calendar_date(1969, January, 1)?,
            BigDecimal::from_str("1")?,
        ),
        (
            Date::from_calendar_date(1971, January, 1)?,
            BigDecimal::from_str("-0.5000")?,
        ),
        (
            Date::from_calendar_date(1973, January, 1)?,
            BigDecimal::from_str("2.5000")?,
        ),
        (
            Date::from_calendar_date(1975, January, 1)?,
            BigDecimal::from_str("4.5000")?,
        ),
        (
            Date::from_calendar_date(1977, January, 1)?,
            BigDecimal::from_str("6.5000")?,
        ),
        (
            Date::from_calendar_date(1979, January, 1)?,
            BigDecimal::from_str("8")?,
        ),
    ];
    assert_eq!(expected, data);

    Ok(())
}

#[rstest]
async fn test_time_bucket_fallback(mut conn: PgConnection) -> Result<()> {
    let error_message = "Function `time_bucket()` must be used with a DuckDB FDW. Native postgres does not support this function. If you believe this function should be implemented natively as a fallback please submit a ticket to https://github.com/paradedb/pg_analytics/issues.";
    let trips_table = NycTripsTable::setup();
    trips_table.execute(&mut conn);

    match "SELECT time_bucket(INTERVAL '2 DAY', tpep_pickup_datetime::DATE) AS bucket, AVG(trip_distance) as avg_value FROM nyc_trips GROUP BY bucket ORDER BY bucket;".execute_result(&mut conn) {
        Ok(_) => {
            panic!("Should have error'ed when calling time_bucket() on non-FDW data.")
        }
        Err(error) => {
            let a = error.to_string().contains(error_message);
            assert!(a);
        }
    }

    Ok(())
}

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
