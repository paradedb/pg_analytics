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

use anyhow::Result;
use datafusion::parquet::arrow::ArrowWriter;
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use fixtures::*;
use rstest::*;
use shared::fixtures::arrow::{
    delta_primitive_record_batch, primitive_record_batch, primitive_setup_fdw_local_file_delta,
    primitive_setup_fdw_local_file_listing, primitive_setup_fdw_s3_delta,
    primitive_setup_fdw_s3_listing,
};
use shared::fixtures::tempfile::TempDir;
use sqlx::postgres::types::PgInterval;
use sqlx::types::{BigDecimal, Json, Uuid};
use sqlx::PgConnection;
use std::collections::HashMap;
use std::str::FromStr;
use time::macros::{date, datetime, time};

use tracing_subscriber::{fmt, EnvFilter};

const S3_TRIPS_BUCKET: &str = "test-trip-setup";
const S3_TRIPS_KEY: &str = "test_trip_setup.parquet";

pub fn init_tracer() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));

    fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .try_init()
        .ok(); // It's okay if this fails, it just means a global subscriber has already been set
}

#[rstest]
async fn test_arrow_types_local_file_listing(
    mut conn: PgConnection,
    tempdir: TempDir,
) -> Result<()> {
    // Initialize the tracer
    init_tracer();

    tracing::debug!("test_arrow_types_local_file_listing Started !!!");

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
