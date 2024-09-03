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

use crate::fixtures::{db::Query, S3};
use anyhow::{Context, Result};
use approx::assert_relative_eq;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::*;
use rand::prelude::*;
use rand::Rng;
use serde::{Deserialize, Serialize};
use soa_derive::StructOfArray;
use sqlx::FromRow;
use sqlx::PgConnection;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use time::PrimitiveDateTime;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;

use std::fs::File;

const YEARS: [i32; 5] = [2020, 2021, 2022, 2023, 2024];

const MANUFACTURERS: [&str; 10] = [
    "Toyota",
    "Honda",
    "Ford",
    "Chevrolet",
    "Nissan",
    "BMW",
    "Mercedes",
    "Audi",
    "Hyundai",
    "Kia",
];

const MODELS: [&str; 20] = [
    "Sedan",
    "SUV",
    "Truck",
    "Hatchback",
    "Coupe",
    "Convertible",
    "Van",
    "Wagon",
    "Crossover",
    "Luxury",
    "Compact",
    "Midsize",
    "Fullsize",
    "Electric",
    "Hybrid",
    "Sports",
    "Minivan",
    "Pickup",
    "Subcompact",
    "Performance",
];

#[derive(Debug, PartialEq, FromRow, StructOfArray, Default, Serialize, Deserialize)]
pub struct AutoSale {
    pub sale_id: Option<i64>,
    pub sale_date: Option<PrimitiveDateTime>,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub price: Option<f64>,
    pub dealership_id: Option<i32>,
    pub customer_id: Option<i32>,
    pub year: Option<i32>,
    pub month: Option<i32>,
}

pub struct AutoSalesSimulator;

impl AutoSalesSimulator {
    #[allow(unused)]
    pub fn generate_data_chunk(chunk_size: usize) -> impl Iterator<Item = AutoSale> {
        let mut rng = rand::thread_rng();

        (0..chunk_size).map(move |i| {
            let year = *YEARS.choose(&mut rng).unwrap();
            let month = rng.gen_range(1..=12);
            let day = rng.gen_range(1..=28);
            let hour = rng.gen_range(0..24);
            let minute = rng.gen_range(0..60);
            let second = rng.gen_range(0..60);

            let sale_date = PrimitiveDateTime::new(
                time::Date::from_calendar_date(year, month.try_into().unwrap(), day).unwrap(),
                time::Time::from_hms(hour, minute, second).unwrap(),
            );

            AutoSale {
                sale_id: Some(i as i64),
                sale_date: Some(sale_date),
                manufacturer: Some(MANUFACTURERS.choose(&mut rng).unwrap().to_string()),
                model: Some(MODELS.choose(&mut rng).unwrap().to_string()),
                price: Some(rng.gen_range(20000.0..80000.0)),
                dealership_id: Some(rng.gen_range(100..1000)),
                customer_id: Some(rng.gen_range(1000..10000)),
                year: Some(year),
                month: Some(month.into()),
            }
        })
    }

    #[allow(unused)]
    pub fn save_to_parquet_in_batches(
        num_records: usize,
        chunk_size: usize,
        path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Manually define the schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("sale_id", DataType::Int64, true),
            Field::new("sale_date", DataType::Utf8, true),
            Field::new("manufacturer", DataType::Utf8, true),
            Field::new("model", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
            Field::new("dealership_id", DataType::Int32, true),
            Field::new("customer_id", DataType::Int32, true),
            Field::new("year", DataType::Int32, true),
            Field::new("month", DataType::Int32, true),
        ]));

        let file = File::create(path)?;
        let writer_properties = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_properties))?;

        for chunk_start in (0..num_records).step_by(chunk_size) {
            let chunk_end = usize::min(chunk_start + chunk_size, num_records);
            let chunk_size = chunk_end - chunk_start;
            let sales_chunk: Vec<AutoSale> = Self::generate_data_chunk(chunk_size).collect();

            // Convert the sales data chunk to arrays
            let sale_ids: ArrayRef = Arc::new(Int64Array::from(
                sales_chunk.iter().map(|s| s.sale_id).collect::<Vec<_>>(),
            ));
            let sale_dates: ArrayRef = Arc::new(StringArray::from(
                sales_chunk
                    .iter()
                    .map(|s| s.sale_date.map(|d| d.to_string()))
                    .collect::<Vec<_>>(),
            ));
            let manufacturer: ArrayRef = Arc::new(StringArray::from(
                sales_chunk
                    .iter()
                    .map(|s| s.manufacturer.clone())
                    .collect::<Vec<_>>(),
            ));
            let model: ArrayRef = Arc::new(StringArray::from(
                sales_chunk
                    .iter()
                    .map(|s| s.model.clone())
                    .collect::<Vec<_>>(),
            ));
            let price: ArrayRef = Arc::new(Float64Array::from(
                sales_chunk.iter().map(|s| s.price).collect::<Vec<_>>(),
            ));
            let dealership_id: ArrayRef = Arc::new(Int32Array::from(
                sales_chunk
                    .iter()
                    .map(|s| s.dealership_id)
                    .collect::<Vec<_>>(),
            ));
            let customer_id: ArrayRef = Arc::new(Int32Array::from(
                sales_chunk
                    .iter()
                    .map(|s| s.customer_id)
                    .collect::<Vec<_>>(),
            ));
            let year: ArrayRef = Arc::new(Int32Array::from(
                sales_chunk.iter().map(|s| s.year).collect::<Vec<_>>(),
            ));
            let month: ArrayRef = Arc::new(Int32Array::from(
                sales_chunk.iter().map(|s| s.month).collect::<Vec<_>>(),
            ));

            // Create a RecordBatch using the schema and arrays
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    sale_ids,
                    sale_dates,
                    manufacturer,
                    model,
                    price,
                    dealership_id,
                    customer_id,
                    year,
                    month,
                ],
            )?;

            writer.write(&batch)?;
        }

        writer.close()?;

        Ok(())
    }
}

pub struct AutoSalesTestRunner;

impl AutoSalesTestRunner {
    #[allow(unused)]
    pub async fn create_partition_and_upload_to_s3(
        s3: &S3,
        s3_bucket: &str,
        df_sales_data: &DataFrame,
    ) -> Result<()> {
        for year in YEARS {
            for manufacturer in MANUFACTURERS {
                let method_result = df_sales_data
                    .clone()
                    .filter(
                        col("year")
                            .eq(lit(year))
                            .and(col("manufacturer").eq(lit(manufacturer))),
                    )?
                    .sort(vec![
                        col("month").sort(true, false),
                        col("sale_id").sort(true, false),
                    ])?;

                let partitioned_batches: Vec<RecordBatch> = method_result.collect().await?;

                // Upload each batch to S3 with the appropriate key format
                for (i, batch) in partitioned_batches.iter().enumerate() {
                    // Use Hive-style partitioning in the S3 key
                    let key = format!(
                        "year={}/manufacturer={}/data_{}.parquet",
                        year, manufacturer, i
                    );

                    // Upload the batch to the specified S3 bucket
                    s3.put_batch(s3_bucket, &key, batch)
                        .await
                        .with_context(|| {
                            format!("Failed to upload batch {} to S3 with key {}", i, key)
                        })?;
                }
            }
        }

        Ok(())
    }

    #[allow(unused)]
    pub async fn teardown_tables(conn: &mut PgConnection) -> Result<()> {
        // Drop the partitioned table (this will also drop all its partitions)
        let drop_partitioned_table = r#"
            DROP TABLE IF EXISTS auto_sales CASCADE;
        "#;
        drop_partitioned_table.execute_result(conn)?;

        // Drop the foreign data wrapper and server
        let drop_fdw_and_server = r#"
            DROP SERVER IF EXISTS auto_sales_server CASCADE;
        "#;
        drop_fdw_and_server.execute_result(conn)?;

        let drop_parquet_wrapper = r#"
            DROP FOREIGN DATA WRAPPER IF EXISTS parquet_wrapper CASCADE;
        "#;
        drop_parquet_wrapper.execute_result(conn)?;

        // Drop the user mapping
        let drop_user_mapping = r#"
            DROP USER MAPPING IF EXISTS FOR public SERVER auto_sales_server;
        "#;
        drop_user_mapping.execute_result(conn)?;

        Ok(())
    }

    #[allow(unused)]
    pub async fn setup_tables(conn: &mut PgConnection, s3: &S3, s3_bucket: &str) -> Result<()> {
        // First, tear down any existing tables
        Self::teardown_tables(conn).await?;

        // Setup S3 Foreign Data Wrapper commands
        let s3_fdw_setup = Self::setup_s3_fdw(&s3.url);
        for command in s3_fdw_setup.split(';') {
            let trimmed_command = command.trim();
            if !trimmed_command.is_empty() {
                trimmed_command.execute_result(conn)?;
            }
        }

        Self::create_partitioned_foreign_table(s3_bucket).execute_result(conn)?;

        Ok(())
    }

    fn setup_s3_fdw(s3_endpoint: &str) -> String {
        format!(
            r#"
            CREATE FOREIGN DATA WRAPPER parquet_wrapper
                HANDLER parquet_fdw_handler
                VALIDATOR parquet_fdw_validator;
    
            CREATE SERVER auto_sales_server
                FOREIGN DATA WRAPPER parquet_wrapper;
    
            CREATE USER MAPPING FOR public
                SERVER auto_sales_server
                OPTIONS (
                    type 'S3',
                    region 'us-east-1',
                    endpoint '{s3_endpoint}',
                    use_ssl 'false',
                    url_style 'path'
                );
            "#
        )
    }

    fn create_partitioned_foreign_table(s3_bucket: &str) -> String {
        // Construct the SQL statement for creating a partitioned foreign table
        format!(
            r#"
            CREATE FOREIGN TABLE auto_sales (
                sale_id                 BIGINT,
                sale_date               DATE,
                manufacturer            TEXT,
                model                   TEXT,
                price                   DOUBLE PRECISION,
                dealership_id           INT,
                customer_id             INT,
                year                    INT,
                month                   INT
            )
            SERVER auto_sales_server
            OPTIONS (
                files 's3://{s3_bucket}/year=*/manufacturer=*/data_*.parquet',
                hive_partitioning '1'
            );
            "#
        )
    }
}

impl AutoSalesTestRunner {
    /// Asserts that the total sales calculated from `pg_analytics`
    /// match the expected results from the DataFrame.
    #[allow(unused)]
    pub async fn assert_total_sales(
        conn: &mut PgConnection,
        df_sales_data: &DataFrame,
    ) -> Result<()> {
        // SQL query to calculate total sales grouped by year and manufacturer.
        let total_sales_query = r#"
            SELECT year, manufacturer, ROUND(SUM(price)::numeric, 4)::float8 as total_sales
            FROM auto_sales
            WHERE year BETWEEN 2020 AND 2024
            GROUP BY year, manufacturer
            ORDER BY year, total_sales DESC;
        "#;

        // Execute the SQL query and fetch results from PostgreSQL.
        let total_sales_results: Vec<(i32, String, f64)> = total_sales_query.fetch(conn);

        // Perform the same calculations on the DataFrame.
        let df_result = df_sales_data
            .clone()
            .filter(col("year").between(lit(2020), lit(2024)))? // Filter by year range.
            .aggregate(
                vec![col("year"), col("manufacturer")],
                vec![sum(col("price")).alias("total_sales")],
            )? // Group by year and manufacturer, summing prices.
            .select(vec![
                col("year"),
                col("manufacturer"),
                round(vec![col("total_sales"), lit(4)]).alias("total_sales"),
            ])? // Round the total sales to 4 decimal places.
            .sort(vec![
                col("year").sort(true, false),
                col("total_sales").sort(false, false),
            ])?; // Sort by year and descending total sales.

        // Collect DataFrame results and transform them into a comparable format.
        let expected_results: Vec<(i32, String, f64)> = df_result
            .collect()
            .await?
            .into_iter()
            .flat_map(|batch| {
                let year_column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let manufacturer_column = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let total_sales_column = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();

                (0..batch.num_rows())
                    .map(move |i| {
                        (
                            year_column.value(i),
                            manufacturer_column.value(i).to_owned(),
                            total_sales_column.value(i),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        // Compare the results with a small epsilon for floating-point precision.
        for ((pg_year, pg_manufacturer, pg_total), (df_year, df_manufacturer, df_total)) in
            total_sales_results.iter().zip(expected_results.iter())
        {
            assert_eq!(pg_year, df_year, "Year mismatch");
            assert_eq!(pg_manufacturer, df_manufacturer, "Manufacturer mismatch");
            assert_relative_eq!(pg_total, df_total, epsilon = 0.001);
        }

        Ok(())
    }

    /// Asserts that the average price calculated from `pg_analytics`
    /// matches the expected results from the DataFrame.
    #[allow(unused)]
    pub async fn assert_avg_price(
        conn: &mut PgConnection,
        df_sales_data: &DataFrame,
    ) -> Result<()> {
        // SQL query to calculate the average price by manufacturer for 2023.
        let avg_price_query = r#"
            SELECT manufacturer, ROUND(AVG(price)::numeric, 4)::float8 as avg_price
            FROM auto_sales
            WHERE year = 2023
            GROUP BY manufacturer
            ORDER BY avg_price DESC;
        "#;

        // Execute the SQL query and fetch results from PostgreSQL.
        let avg_price_results: Vec<(String, f64)> = avg_price_query.fetch(conn);

        // Perform the same calculations on the DataFrame.
        let df_result = df_sales_data
            .clone()
            .filter(col("year").eq(lit(2023)))? // Filter by year 2023.
            .aggregate(
                vec![col("manufacturer")],
                vec![avg(col("price")).alias("avg_price")],
            )? // Group by manufacturer, calculating the average price.
            .select(vec![
                col("manufacturer"),
                round(vec![col("avg_price"), lit(4)]).alias("avg_price"),
            ])? // Round the average price to 4 decimal places.
            .sort(vec![col("avg_price").sort(false, false)])?; // Sort by descending average price.

        // Collect DataFrame results and transform them into a comparable format.
        let expected_results: Vec<(String, f64)> = df_result
            .collect()
            .await?
            .into_iter()
            .flat_map(|batch| {
                let manufacturer_column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let avg_price_column = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();

                (0..batch.num_rows())
                    .map(move |i| {
                        (
                            manufacturer_column.value(i).to_owned(),
                            avg_price_column.value(i),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        // Compare the results using assert_relative_eq for floating-point precision.
        for ((pg_manufacturer, pg_price), (df_manufacturer, df_price)) in
            avg_price_results.iter().zip(expected_results.iter())
        {
            assert_eq!(pg_manufacturer, df_manufacturer, "Manufacturer mismatch");
            assert_relative_eq!(pg_price, df_price, epsilon = 0.001);
        }

        Ok(())
    }

    /// Asserts that the monthly sales calculated from `pg_analytics`
    /// match the expected results from the DataFrame.
    #[allow(unused)]
    pub async fn assert_monthly_sales(
        conn: &mut PgConnection,
        df_sales_data: &DataFrame,
    ) -> Result<()> {
        // SQL query to calculate monthly sales and collect sale IDs for 2024.
        let monthly_sales_query = r#"
            SELECT year, month, COUNT(*) as sales_count, 
                   array_agg(sale_id) as sale_ids
            FROM auto_sales
            WHERE manufacturer = 'Toyota' AND year = 2024
            GROUP BY year, month
            ORDER BY month;
        "#;

        // Execute the SQL query and fetch results from PostgreSQL.
        let monthly_sales_results: Vec<(i32, i32, i64, Vec<i64>)> = monthly_sales_query.fetch(conn);

        // Perform the same calculations on the DataFrame.
        let df_result = df_sales_data
            .clone()
            .filter(
                col("manufacturer")
                    .eq(lit("Toyota"))
                    .and(col("year").eq(lit(2024))),
            )? // Filter by manufacturer (Toyota) and year (2024).
            .aggregate(
                vec![col("year"), col("month")],
                vec![
                    count(lit(1)).alias("sales_count"),
                    array_agg(col("sale_id")).alias("sale_ids"),
                ],
            )? // Group by year and month, counting sales and aggregating sale IDs.
            .sort(vec![col("month").sort(true, false)])?; // Sort by month.

        // Collect DataFrame results, sort sale IDs, and transform into a comparable format.
        let expected_results: Vec<(i32, i32, i64, Vec<i64>)> = df_result
            .collect()
            .await?
            .into_iter()
            .flat_map(|batch| {
                let year = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let month = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let sales_count = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let sale_ids = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();

                (0..batch.num_rows())
                    .map(|i| {
                        let mut sale_ids_vec: Vec<i64> = sale_ids
                            .value(i)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values()
                            .to_vec();
                        sale_ids_vec.sort(); // Sort the sale IDs to match PostgreSQL result.

                        (
                            year.value(i),
                            month.value(i),
                            sales_count.value(i),
                            sale_ids_vec,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        // Assert that the results from PostgreSQL match the DataFrame results.
        assert_eq!(
            monthly_sales_results, expected_results,
            "Monthly sales results do not match"
        );

        Ok(())
    }
}

// Define a type alias for the complex type
type QueryResult = Vec<(Option<i32>, Option<String>, Option<f64>, i64)>;

impl AutoSalesTestRunner {
    #[allow(unused)]
    pub fn benchmark_query() -> String {
        // This is a placeholder query. Replace with a more complex query that would benefit from caching.
        r#"
        SELECT year, manufacturer, AVG(price) as avg_price, COUNT(*) as sale_count
        FROM auto_sales
        WHERE year BETWEEN 2020 AND 2024
        GROUP BY year, manufacturer
        ORDER BY year, avg_price DESC
        "#
        .to_string()
    }

    #[allow(unused)]
    async fn verify_benchmark_query(
        df_sales_data: &DataFrame,
        duckdb_results: QueryResult,
    ) -> Result<()> {
        // Execute the equivalent query on the DataFrame
        let df_result = df_sales_data
            .clone()
            .filter(col("year").between(lit(2020), lit(2024)))?
            .aggregate(
                vec![col("year"), col("manufacturer")],
                vec![
                    avg(col("price")).alias("avg_price"),
                    count(lit(1)).alias("sale_count"),
                ],
            )?
            .sort(vec![
                col("year").sort(true, false),
                col("avg_price").sort(false, false),
            ])?;

        let df_results: QueryResult = df_result
            .collect()
            .await?
            .into_iter()
            .flat_map(|batch| {
                let year = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let manufacturer = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let avg_price = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let sale_count = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();

                (0..batch.num_rows())
                    .map(move |i| {
                        (
                            Some(year.value(i)),
                            Some(manufacturer.value(i).to_string()),
                            Some(avg_price.value(i)),
                            sale_count.value(i),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        // Compare results
        assert_eq!(
            duckdb_results.len(),
            df_results.len(),
            "Result set sizes do not match"
        );

        for (
            (duck_year, duck_manufacturer, duck_avg_price, duck_count),
            (df_year, df_manufacturer, df_avg_price, df_count),
        ) in duckdb_results.iter().zip(df_results.iter())
        {
            assert_eq!(duck_year, df_year, "Year mismatch");
            assert_eq!(duck_manufacturer, df_manufacturer, "Manufacturer mismatch");
            assert_relative_eq!(
                duck_avg_price.unwrap(),
                df_avg_price.unwrap(),
                epsilon = 0.01,
                max_relative = 0.01
            );
            assert_eq!(duck_count, df_count, "Sale count mismatch");
        }

        Ok(())
    }

    #[allow(unused)]
    pub async fn run_benchmark_iterations(
        conn: &mut PgConnection,
        query: &str,
        iterations: usize,
        warmup_iterations: usize,
        enable_cache: bool,
        df_sales_data: &DataFrame,
    ) -> Result<Vec<Duration>> {
        let cache_setting = if enable_cache { "true" } else { "false" };
        format!(
            "SELECT duckdb_execute($$SET enable_object_cache={}$$)",
            cache_setting
        )
        .execute(conn);

        // Warm-up phase
        for _ in 0..warmup_iterations {
            let _: QueryResult = query.fetch(conn);
        }

        let mut execution_times = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let start = Instant::now();
            let query_val: QueryResult = query.fetch(conn);
            let execution_time = start.elapsed();

            let _ = Self::verify_benchmark_query(df_sales_data, query_val.clone()).await;

            execution_times.push(execution_time);
        }

        Ok(execution_times)
    }

    #[allow(unused)]
    fn average_duration(durations: &[Duration]) -> Duration {
        durations.iter().sum::<Duration>() / durations.len() as u32
    }

    #[allow(unused)]
    pub fn report_benchmark_results(
        cache_disabled: Vec<Duration>,
        cache_enabled: Vec<Duration>,
        final_disabled: Vec<Duration>,
    ) {
        let calculate_metrics =
            |durations: &[Duration]| -> (Duration, Duration, Duration, Duration, Duration, f64) {
                let avg = Self::average_duration(durations);
                let min = *durations.iter().min().unwrap_or(&Duration::ZERO);
                let max = *durations.iter().max().unwrap_or(&Duration::ZERO);

                let variance = durations
                    .iter()
                    .map(|&d| {
                        let diff = d.as_secs_f64() - avg.as_secs_f64();
                        diff * diff
                    })
                    .sum::<f64>()
                    / durations.len() as f64;
                let std_dev = variance.sqrt();

                let mut sorted_durations = durations.to_vec();
                sorted_durations.sort_unstable();
                let percentile_95 = sorted_durations
                    [((durations.len() as f64 * 0.95) as usize).min(durations.len() - 1)];

                (
                    avg,
                    min,
                    max,
                    percentile_95,
                    Duration::from_secs_f64(std_dev),
                    std_dev,
                )
            };

        let (
            avg_disabled,
            min_disabled,
            max_disabled,
            p95_disabled,
            std_dev_disabled,
            std_dev_disabled_secs,
        ) = calculate_metrics(&cache_disabled);
        let (
            avg_enabled,
            min_enabled,
            max_enabled,
            p95_enabled,
            std_dev_enabled,
            std_dev_enabled_secs,
        ) = calculate_metrics(&cache_enabled);
        let (
            avg_final_disabled,
            min_final_disabled,
            max_final_disabled,
            p95_final_disabled,
            std_dev_final_disabled,
            std_dev_final_disabled_secs,
        ) = calculate_metrics(&final_disabled);

        let improvement = (avg_final_disabled.as_secs_f64() - avg_enabled.as_secs_f64())
            / avg_final_disabled.as_secs_f64()
            * 100.0;

        tracing::info!("Benchmark Results:");
        tracing::info!("Cache Disabled:");
        tracing::info!("  Average: {:?}", avg_disabled);
        tracing::info!("  Minimum: {:?}", min_disabled);
        tracing::info!("  Maximum: {:?}", max_disabled);
        tracing::info!("  95th Percentile: {:?}", p95_disabled);
        tracing::info!(
            "  Standard Deviation: {:?} ({:.6} seconds)",
            std_dev_disabled,
            std_dev_disabled_secs
        );

        tracing::info!("Cache Enabled:");
        tracing::info!("  Average: {:?}", avg_enabled);
        tracing::info!("  Minimum: {:?}", min_enabled);
        tracing::info!("  Maximum: {:?}", max_enabled);
        tracing::info!("  95th Percentile: {:?}", p95_enabled);
        tracing::info!(
            "  Standard Deviation: {:?} ({:.6} seconds)",
            std_dev_enabled,
            std_dev_enabled_secs
        );

        tracing::info!("Final Cache Disabled:");
        tracing::info!("  Average: {:?}", avg_final_disabled);
        tracing::info!("  Minimum: {:?}", min_final_disabled);
        tracing::info!("  Maximum: {:?}", max_final_disabled);
        tracing::info!("  95th Percentile: {:?}", p95_final_disabled);
        tracing::info!(
            "  Standard Deviation: {:?} ({:.6} seconds)",
            std_dev_final_disabled,
            std_dev_final_disabled_secs
        );

        tracing::info!("Performance improvement with cache: {:.2}%", improvement);

        // Add assertions
        assert!(
            avg_enabled < avg_disabled,
            "Expected performance improvement with cache enabled"
        );
        assert!(
            avg_enabled < avg_final_disabled,
            "Expected performance improvement with cache enabled compared to final disabled state"
        );
    }
}
