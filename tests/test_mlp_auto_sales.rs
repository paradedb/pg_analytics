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

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use rstest::*;
use sqlx::PgConnection;

use crate::fixtures::*;
use crate::tables::auto_sales::{AutoSalesSimulator, AutoSalesTestRunner};
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::prelude::SessionContext;

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
    // Log the start of the test.

    // Check if the Parquet file already exists at the specified path.
    if !parquet_path.exists() {
        // If the file doesn't exist, generate and save sales data in batches.
        AutoSalesSimulator::save_to_parquet_in_batches(10000, 100, &parquet_path)
            .map_err(|e| anyhow::anyhow!("Failed to save parquet: {}", e))?;
    }

    // Create a new DataFusion session context for querying the data.
    let ctx = SessionContext::new();
    // Load the sales data from the Parquet file into a DataFrame.
    let df_sales_data = ctx
        .read_parquet(
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

    // Await the S3 service setup.
    let s3 = s3.await;
    // Define the S3 bucket name for storing sales data.
    let s3_bucket = "demo-mlp-auto-sales";
    // Create the S3 bucket if it doesn't already exist.
    s3.create_bucket(s3_bucket).await?;

    // Partition the data and upload the partitions to the S3 bucket.
    AutoSalesTestRunner::create_partition_and_upload_to_s3(&s3, s3_bucket, &df_sales_data).await?;

    // Set up the necessary tables in the PostgreSQL database using the data from S3.
    AutoSalesTestRunner::setup_tables(&mut conn, &s3, s3_bucket).await?;

    // Assert that the total sales calculation matches the expected result.
    AutoSalesTestRunner::assert_total_sales(&mut conn, &df_sales_data).await?;

    // Assert that the average price calculation matches the expected result.
    AutoSalesTestRunner::assert_avg_price(&mut conn, &df_sales_data).await?;

    // Assert that the monthly sales calculation matches the expected result.
    AutoSalesTestRunner::assert_monthly_sales(&mut conn, &df_sales_data).await?;

    // Return Ok if all assertions pass successfully.
    Ok(())
}

#[rstest]
async fn test_duckdb_object_cache_performance(
    #[future] s3: S3,
    mut conn: PgConnection,
    parquet_path: PathBuf,
) -> Result<()> {
    // Check if the Parquet file already exists at the specified path.
    if !parquet_path.exists() {
        // If the file doesn't exist, generate and save sales data in batches.
        AutoSalesSimulator::save_to_parquet_in_batches(10000, 100, &parquet_path)
            .map_err(|e| anyhow::anyhow!("Failed to save parquet: {}", e))?;
    }

    // Create a new DataFusion session context for querying the data.
    let ctx = SessionContext::new();
    // Load the sales data from the Parquet file into a DataFrame.
    let df_sales_data = ctx
        .read_parquet(
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

    // Set up the test environment
    let s3 = s3.await;
    let s3_bucket = "demo-mlp-auto-sales";

    // Create the S3 bucket if it doesn't already exist.
    s3.create_bucket(s3_bucket).await?;

    // Partition the data and upload the partitions to the S3 bucket.
    AutoSalesTestRunner::create_partition_and_upload_to_s3(&s3, s3_bucket, &df_sales_data).await?;

    // Set up the necessary tables in the PostgreSQL database using the data from S3.
    AutoSalesTestRunner::setup_tables(&mut conn, &s3, s3_bucket).await?;

    // Get the benchmark query
    let benchmark_query = AutoSalesTestRunner::benchmark_query();

    // Run benchmarks
    let warmup_iterations = 5;
    let num_iterations = 10;
    let cache_disabled_times = AutoSalesTestRunner::run_benchmark_iterations(
        &mut conn,
        &benchmark_query,
        num_iterations,
        warmup_iterations,
        false,
        &df_sales_data,
    )
    .await?;
    let cache_enabled_times = AutoSalesTestRunner::run_benchmark_iterations(
        &mut conn,
        &benchmark_query,
        num_iterations,
        warmup_iterations,
        true,
        &df_sales_data,
    )
    .await?;
    let final_disabled_times = AutoSalesTestRunner::run_benchmark_iterations(
        &mut conn,
        &benchmark_query,
        num_iterations,
        warmup_iterations,
        false,
        &df_sales_data,
    )
    .await?;

    // Analyze and report results
    AutoSalesTestRunner::report_benchmark_results(
        cache_disabled_times,
        cache_enabled_times,
        final_disabled_times,
    );

    Ok(())
}
