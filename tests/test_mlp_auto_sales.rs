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

mod common;
mod datasets;
mod fixtures;

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use rstest::*;
use sqlx::PgConnection;

use crate::common::{execute_query, fetch_results, init_tracer};
use crate::datasets::auto_sales::{AutoSalesSimulator, AutoSalesTestRunner};
use crate::fixtures::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::logical_expr::col;
use datafusion::prelude::{CsvReadOptions, SessionContext};

#[fixture]
fn parquet_path() -> PathBuf {
    // Use the environment variable to detect the `target` path
    let target_dir = env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let parquet_path = Path::new(&target_dir).join("tmp_dataset/ds_auto_sales.parquet");

    // Check if the file exists; if not, create the necessary directories
    if !parquet_path.exists() {
        if let Some(parent_dir) = parquet_path.parent() {
            fs::create_dir_all(parent_dir).expect("Failed to create directories");
        }
    }

    parquet_path
}

#[rstest]
async fn test_partitioned_automotive_sales_s3_parquet(
    #[future] s3: S3,
    mut conn: PgConnection,
    parquet_path: PathBuf,
) -> Result<()> {
    init_tracer();

    tracing::error!("test_partitioned_automotive_sales_s3_parquet Started !!!");

    tracing::error!("Kom-1.1 !!!");

    // Check for the existence of a parquet file in a predefined path. If absent, generate it.
    if !parquet_path.exists() {
        // Generate and save data
        let sales_data = AutoSalesSimulator::generate_data(10000)?;

        AutoSalesSimulator::save_to_parquet(&sales_data, &parquet_path)
            .map_err(|e| anyhow::anyhow!("Failed to save parquet: {}", e))?;
    }

    tracing::error!("Kom-2.1 !!!");

    // Set up S3
    let s3 = s3.await;
    let s3_bucket = "demo-mlp-auto-sales";
    s3.create_bucket(s3_bucket).await?;

    tracing::error!("Kom-3.1 !!!");

    let ctx = SessionContext::new();
    let df_sales_data = ctx
        .read_parquet(
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

    tracing::error!(
        "DataFrame schema after reading Parquet: {:?}",
        df_sales_data.schema()
    );

    tracing::error!(
        "Column names after reading Parquet: {:?}",
        df_sales_data.schema().field_names()
    );

    tracing::error!("Kom-4.1 !!!");

    // Create partition and upload data to S3
    // AutoSalesTestRunner::create_partition_and_upload_to_s3(&s3, s3_bucket, &df_sales_data).await?;

    // AutoSalesTestRunner::investigate_datafusion_discrepancy(&df_sales_data, &parquet_path).await?;

    AutoSalesTestRunner::create_partition_and_upload_to_s3(
        &s3,
        s3_bucket,
        &df_sales_data,
        &parquet_path,
    )
    .await?;

    tracing::error!("Kom-5.1 !!!");

    // Set up tables
    AutoSalesTestRunner::setup_tables(&mut conn, &s3, s3_bucket).await?;

    tracing::error!("Kom-6.1 !!!");

    // AutoSalesTestRunner::assert_total_sales(&mut conn, &ctx, &df_sales_data).await?;

    // AutoSalesTestRunner::assert_avg_price(&mut conn, &df_sales_data).await?;

    // AutoSalesTestRunner::assert_monthly_sales(&mut conn, &df_sales_data).await?;

    AutoSalesTestRunner::assert_monthly_sales_duckdb(&mut conn, &parquet_path).await?;

    AutoSalesTestRunner::debug_april_sales(&mut conn, &parquet_path).await?;

    Ok(())
}
