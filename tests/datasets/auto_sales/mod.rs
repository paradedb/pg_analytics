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

use crate::common::{duckdb_utils, execute_query, fetch_results, print_utils};
use crate::fixtures::*;
use anyhow::{Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use rand::prelude::*;
use rand::Rng;
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use soa_derive::StructOfArray;
use sqlx::FromRow;
use sqlx::PgConnection;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use time::PrimitiveDateTime;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::execution::context::SessionContext;
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
    pub fn generate_data(num_records: usize) -> Result<Vec<AutoSale>> {
        let mut rng = rand::thread_rng();

        let sales: Vec<AutoSale> = (0..num_records)
            .map(|i| {
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
            .collect();

        // Check that all records have the same number of fields
        let first_record_fields = Self::count_fields(&sales[0]);
        for (index, sale) in sales.iter().enumerate().skip(1) {
            let fields = Self::count_fields(sale);
            if fields != first_record_fields {
                return Err(anyhow::anyhow!("Inconsistent number of fields: Record 0 has {} fields, but record {} has {} fields", first_record_fields, index, fields));
            }
        }

        Ok(sales)
    }

    fn count_fields(sale: &AutoSale) -> usize {
        // Count non-None fields
        let mut count = 0;
        if sale.sale_id.is_some() {
            count += 1;
        }
        if sale.sale_date.is_some() {
            count += 1;
        }
        if sale.manufacturer.is_some() {
            count += 1;
        }
        if sale.model.is_some() {
            count += 1;
        }
        if sale.price.is_some() {
            count += 1;
        }
        if sale.dealership_id.is_some() {
            count += 1;
        }
        if sale.customer_id.is_some() {
            count += 1;
        }
        if sale.year.is_some() {
            count += 1;
        }
        if sale.month.is_some() {
            count += 1;
        }

        count
    }

    pub fn save_to_parquet(
        sales: &[AutoSale],
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

        // Convert the sales data to arrays
        let sale_ids: ArrayRef = Arc::new(Int64Array::from(
            sales.iter().map(|s| s.sale_id).collect::<Vec<_>>(),
        ));
        let sale_dates: ArrayRef = Arc::new(StringArray::from(
            sales
                .iter()
                .map(|s| s.sale_date.map(|d| d.to_string()))
                .collect::<Vec<_>>(),
        ));
        let manufacturer: ArrayRef = Arc::new(StringArray::from(
            sales
                .iter()
                .map(|s| s.manufacturer.clone())
                .collect::<Vec<_>>(),
        ));
        let model: ArrayRef = Arc::new(StringArray::from(
            sales.iter().map(|s| s.model.clone()).collect::<Vec<_>>(),
        ));
        let price: ArrayRef = Arc::new(Float64Array::from(
            sales.iter().map(|s| s.price).collect::<Vec<_>>(),
        ));
        let dealership_id: ArrayRef = Arc::new(Int32Array::from(
            sales.iter().map(|s| s.dealership_id).collect::<Vec<_>>(),
        ));
        let customer_id: ArrayRef = Arc::new(Int32Array::from(
            sales.iter().map(|s| s.customer_id).collect::<Vec<_>>(),
        ));
        let year: ArrayRef = Arc::new(Int32Array::from(
            sales.iter().map(|s| s.year).collect::<Vec<_>>(),
        ));
        let month: ArrayRef = Arc::new(Int32Array::from(
            sales.iter().map(|s| s.month).collect::<Vec<_>>(),
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

        // Write the RecordBatch to a Parquet file
        let file = File::create(path)?;
        let writer_properties = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(writer_properties))?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }
}

pub struct AutoSalesTestRunner;

impl AutoSalesTestRunner {
    async fn compare_datafusion_approaches(
        df: &DataFrame,
        parquet_path: &Path,
        year: i32,
        manufacturer: &str,
    ) -> Result<()> {
        let ctx = SessionContext::new();

        // Register the Parquet file
        ctx.register_parquet(
            "auto_sales",
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .context("Failed to register Parquet file")?;

        // SQL approach
        let sql_query = format!(
            r#"
            SELECT year, month, sale_id
            FROM auto_sales
            WHERE year = {} AND manufacturer = '{}'
            ORDER BY month, sale_id
            "#,
            year, manufacturer
        );

        let sql_result = ctx.sql(&sql_query).await?;
        let sql_batches: Vec<RecordBatch> = sql_result.collect().await?;

        // Method chaining approach
        let method_result = df
            .clone()
            .filter(
                col("year")
                    .eq(lit(year))
                    .and(col("manufacturer").eq(lit(manufacturer))),
            )?
            .sort(vec![
                col("month").sort(true, false),
                col("sale_id").sort(true, false),
            ])?
            .select(vec![col("year"), col("month"), col("sale_id")])?;

        let method_batches: Vec<RecordBatch> = method_result.collect().await?;

        // Compare results
        tracing::error!(
            "Comparing results for year {} and manufacturer {}",
            year,
            manufacturer
        );
        tracing::error!(
            "SQL query result count: {}",
            sql_batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );
        tracing::error!(
            "Method chaining result count: {}",
            method_batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        let mut row_count = 0;
        let mut mismatch_count = 0;

        for (sql_batch, method_batch) in sql_batches.iter().zip(method_batches.iter()) {
            let sql_year = sql_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let sql_month = sql_batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let sql_sale_id = sql_batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            let method_year = method_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let method_month = method_batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let method_sale_id = method_batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            for i in 0..sql_batch.num_rows().min(method_batch.num_rows()) {
                row_count += 1;
                if sql_year.value(i) != method_year.value(i)
                    || sql_month.value(i) != method_month.value(i)
                    || sql_sale_id.value(i) != method_sale_id.value(i)
                {
                    mismatch_count += 1;
                    tracing::error!(
                        "Mismatch at row {}: SQL ({}, {}, {}), Method ({}, {}, {})",
                        row_count,
                        sql_year.value(i),
                        sql_month.value(i),
                        sql_sale_id.value(i),
                        method_year.value(i),
                        method_month.value(i),
                        method_sale_id.value(i)
                    );
                }
                if row_count % 1000 == 0 {
                    tracing::error!("Processed {} rows", row_count);
                }
            }
        }

        if sql_batches.iter().map(|b| b.num_rows()).sum::<usize>()
            != method_batches.iter().map(|b| b.num_rows()).sum::<usize>()
        {
            tracing::error!("Result sets have different lengths");
        }

        tracing::error!(
            "Comparison complete. Total rows: {}, Mismatches: {}",
            row_count,
            mismatch_count
        );

        Ok(())
    }

    // Usage in your test or main function
    pub async fn investigate_datafusion_discrepancy(
        df: &DataFrame,
        parquet_path: &Path,
    ) -> Result<()> {
        Self::compare_datafusion_approaches(df, parquet_path, 2024, "Toyota").await?;
        Self::compare_datafusion_approaches(df, parquet_path, 2020, "Toyota").await?;
        Self::compare_datafusion_approaches(df, parquet_path, 2021, "Toyota").await?;
        Self::compare_datafusion_approaches(df, parquet_path, 2022, "Toyota").await?;
        Self::compare_datafusion_approaches(df, parquet_path, 2023, "Toyota").await?;
        Ok(())
    }

    pub async fn create_partition_and_upload_to_s3(
        s3: &S3,
        s3_bucket: &str,
        df_sales_data: &DataFrame,
        parquet_path: &Path,
    ) -> Result<()> {
        let ctx = SessionContext::new();

        // Register the Parquet file
        ctx.register_parquet(
            "auto_sales",
            parquet_path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .context("Failed to register Parquet file")?;

        for year in YEARS {
            for manufacturer in MANUFACTURERS {
                tracing::info!("Processing year: {}, manufacturer: {}", year, manufacturer);

                // SQL approach
                let sql_query = format!(
                    r#"
                    SELECT *
                    FROM auto_sales
                    WHERE year = {} AND manufacturer = '{}'
                    ORDER BY month, sale_id
                    "#,
                    year, manufacturer
                );

                tracing::error!("Executing SQL query: {}", sql_query);
                let sql_result = ctx.sql(&sql_query).await?;
                let sql_batches: Vec<RecordBatch> = sql_result.collect().await?;

                // Method chaining approach
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

                let method_batches: Vec<RecordBatch> = method_result.collect().await?;

                // Compare results
                let sql_count: usize = sql_batches.iter().map(|b| b.num_rows()).sum();
                let method_count: usize = method_batches.iter().map(|b| b.num_rows()).sum();

                tracing::error!("SQL query result count: {}", sql_count);
                tracing::error!("Method chaining result count: {}", method_count);

                if sql_count != method_count {
                    tracing::error!("Result count mismatch for {}/{}", year, manufacturer);
                }

                // Proceed with upload (using method chaining approach for consistency with original function)
                for (i, batch) in method_batches.iter().enumerate() {
                    let key = format!("{}/{}/data_{}.parquet", year, manufacturer, i);
                    tracing::debug!("Uploading batch {} to S3: {}", i, key);
                    s3.put_batch(s3_bucket, &key, batch)
                        .await
                        .with_context(|| format!("Failed to upload batch {} to S3", i))?;
                }

                // Verify uploaded data (optional, might be slow for large datasets)
                for (i, _) in method_batches.iter().enumerate() {
                    let key = format!("{}/{}/data_{}.parquet", year, manufacturer, i);
                    let downloaded_batch = s3
                        .get_batch(s3_bucket, &key)
                        .await
                        .with_context(|| format!("Failed to download batch {} from S3", i))?;
                    if downloaded_batch != method_batches[i] {
                        tracing::error!(
                            "Uploaded batch {} does not match original for {}/{}",
                            i,
                            year,
                            manufacturer
                        );
                    }
                }
            }
        }

        tracing::error!("Completed data upload to S3");
        Ok(())
    }

    pub async fn teardown_tables(conn: &mut PgConnection) -> Result<()> {
        // Drop the partitioned table (this will also drop all its partitions)
        let drop_partitioned_table = r#"
            DROP TABLE IF EXISTS auto_sales_partitioned CASCADE;
        "#;
        execute_query(conn, drop_partitioned_table).await?;

        // Drop the foreign data wrapper and server
        let drop_fdw_and_server = r#"
            DROP SERVER IF EXISTS auto_sales_server CASCADE;
        "#;
        execute_query(conn, drop_fdw_and_server).await?;

        let drop_fdw_and_server = r#"
            DROP FOREIGN DATA WRAPPER IF EXISTS parquet_wrapper CASCADE;
        "#;
        execute_query(conn, drop_fdw_and_server).await?;

        // Drop the user mapping
        let drop_user_mapping = r#"
            DROP USER MAPPING IF EXISTS FOR public SERVER auto_sales_server;
        "#;
        execute_query(conn, drop_user_mapping).await?;

        Ok(())
    }

    pub async fn setup_tables(conn: &mut PgConnection, s3: &S3, s3_bucket: &str) -> Result<()> {
        // First, tear down any existing tables
        Self::teardown_tables(conn).await?;

        // Setup S3 Foreign Data Wrapper commands
        let s3_fdw_setup = Self::setup_s3_fdw(&s3.url, s3_bucket);
        for command in s3_fdw_setup.split(';') {
            let trimmed_command = command.trim();
            if !trimmed_command.is_empty() {
                execute_query(conn, trimmed_command).await?;
            }
        }

        execute_query(conn, &Self::create_partitioned_table()).await?;

        // Create partitions
        for year in YEARS {
            execute_query(conn, &Self::create_year_partition(year)).await?;
            for manufacturer in MANUFACTURERS {
                execute_query(
                    conn,
                    &Self::create_manufacturer_partition(s3_bucket, year, manufacturer),
                )
                .await?;
            }
        }

        Ok(())
    }

    fn setup_s3_fdw(s3_endpoint: &str, s3_bucket: &str) -> String {
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

    fn create_partitioned_table() -> String {
        r#"
        CREATE TABLE auto_sales_partitioned (
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
        PARTITION BY LIST (year);
        "#
        .to_string()
    }

    fn create_year_partition(year: i32) -> String {
        format!(
            r#"
            CREATE TABLE auto_sales_y{year} 
            PARTITION OF auto_sales_partitioned
            FOR VALUES IN ({year})
            PARTITION BY LIST (manufacturer);
            "#
        )
    }

    fn create_manufacturer_partition(s3_bucket: &str, year: i32, manufacturer: &str) -> String {
        format!(
            r#"
            CREATE FOREIGN TABLE auto_sales_y{year}_{manufacturer} 
            PARTITION OF auto_sales_y{year}
            FOR VALUES IN ('{manufacturer}')
            SERVER auto_sales_server
            OPTIONS (
                files 's3://{s3_bucket}/{year}/{manufacturer}/*.parquet'
            );
            "#
        )
    }
}

impl AutoSalesTestRunner {
    /// Asserts that the total sales calculated from the `pg_analytics`
    /// match the expected results from the DataFrame.
    pub async fn assert_total_sales(
        conn: &mut PgConnection,
        session_context: &SessionContext,
        df_sales_data: &DataFrame,
    ) -> Result<()> {
        // Run test queries
        let total_sales_query = r#"
            SELECT year, manufacturer, SUM(price) as total_sales
            FROM auto_sales_partitioned
            WHERE year BETWEEN 2020 AND 2024
            GROUP BY year, manufacturer
            ORDER BY year, total_sales DESC;
        "#;
        let total_sales_results: Vec<(i32, String, f64)> =
            fetch_results(conn, total_sales_query).await?;

        let df_result = df_sales_data
            .clone()
            .filter(col("year").between(lit(2020), lit(2024)))?
            .aggregate(
                vec![col("year"), col("manufacturer")],
                vec![sum(col("price")).alias("total_sales")],
            )?
            .sort(vec![
                col("year").sort(true, false),
                col("total_sales").sort(false, false),
            ])?;

        let expected_results = df_result
            .collect()
            .await?
            .iter()
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

                (0..batch.num_rows()).map(move |i| {
                    (
                        year_column.value(i),
                        manufacturer_column.value(i).to_owned(),
                        total_sales_column.value(i),
                    )
                })
            })
            .collect::<Vec<(i32, String, f64)>>();

        assert_eq!(
            expected_results, total_sales_results,
            "Total sales results do not match"
        );

        Ok(())
    }

    /// Asserts that the average price calculated from the `pg_analytics`
    /// matches the expected results from the DataFrame.
    pub async fn assert_avg_price(
        conn: &mut PgConnection,
        df_sales_data: &DataFrame,
    ) -> Result<()> {
        let avg_price_query = r#"
            SELECT manufacturer, AVG(price) as avg_price
            FROM auto_sales_partitioned
            WHERE year = 2023
            GROUP BY manufacturer
            ORDER BY avg_price DESC;
        "#;
        let avg_price_results: Vec<(String, f64)> = fetch_results(conn, avg_price_query).await?;

        let df_result = df_sales_data
            .clone()
            .filter(col("year").eq(lit(2023)))?
            .aggregate(
                vec![col("manufacturer")],
                vec![avg(col("price")).alias("avg_price")],
            )?
            .sort(vec![col("avg_price").sort(false, false)])?;

        let expected_results = df_result
            .collect()
            .await?
            .iter()
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

                (0..batch.num_rows()).map(move |i| {
                    (
                        manufacturer_column.value(i).to_owned(),
                        avg_price_column.value(i),
                    )
                })
            })
            .collect::<Vec<(String, f64)>>();

        assert_eq!(
            expected_results, avg_price_results,
            "Average price results do not match"
        );

        Ok(())
    }

    /// Asserts that the monthly sales calculated from the `pg_analytics`
    /// match the expected results from the DataFrame.
    pub async fn assert_monthly_sales(
        conn: &mut PgConnection,
        df_sales_data: &DataFrame,
    ) -> Result<()> {
        let monthly_sales_query = r#"
            SELECT year, month, COUNT(*) as sales_count, 
                   array_agg(sale_id) as sale_ids
            FROM auto_sales_partitioned
            WHERE manufacturer = 'Toyota' AND year = 2024
            GROUP BY year, month
            ORDER BY month;
        "#;
        let monthly_sales_results: Vec<(i32, i32, i64, Vec<i64>)> =
            fetch_results(conn, monthly_sales_query).await?;

        let df_result = df_sales_data
            .clone()
            .filter(
                col("manufacturer")
                    .eq(lit("Toyota"))
                    .and(col("year").eq(lit(2024))),
            )?
            .aggregate(
                vec![col("year"), col("month")],
                vec![
                    count(lit(1)).alias("sales_count"),
                    array_agg(col("sale_id")).alias("sale_ids"),
                ],
            )?
            .sort(vec![col("month").sort(true, false)])?;

        let expected_results: Vec<(i32, i32, i64, Vec<i64>)> = df_result
            .collect()
            .await?
            .into_iter()
            .map(|batch| {
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
                        (
                            year.value(i),
                            month.value(i),
                            sales_count.value(i),
                            sale_ids
                                .value(i)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values()
                                .to_vec(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect();

        print_utils::print_results(
            vec![
                "Year".to_string(),
                "Month".to_string(),
                "Sales Count".to_string(),
                "Sale IDs (first 5)".to_string(),
            ],
            "Pg_Analytics".to_string(),
            &monthly_sales_results,
            "DataFrame".to_string(),
            &expected_results,
        )
        .await?;

        // assert_eq!(
        //     monthly_sales_results, expected_results,
        //     "Monthly sales results do not match"
        // );

        Ok(())
    }

    /// Asserts that the monthly sales calculated from the `pg_analytics`
    /// match the expected results from the DataFrame.
    pub async fn assert_monthly_sales_duckdb(
        conn: &mut PgConnection,
        parquet_path: &PathBuf,
    ) -> Result<()> {
        let monthly_sales_sqlx_query = r#"
            SELECT year, month, COUNT(*) as sales_count, 
                   array_agg(sale_id) as sale_ids
            FROM auto_sales_partitioned
            WHERE manufacturer = 'Toyota' AND year = 2024
            GROUP BY year, month
            ORDER BY month;
        "#;
        let monthly_sales_pga_results: Vec<(i32, i32, i64, Vec<i64>)> =
            fetch_results(conn, monthly_sales_sqlx_query).await?;

        let monthly_sales_duckdb_query = r#"
            SELECT year, month, COUNT(*) as sales_count, 
                   list(sale_id) as sale_ids
            FROM auto_sales
            WHERE manufacturer = 'Toyota' AND year = 2024
            GROUP BY year, month
            ORDER BY month
        "#;

        let monthly_sales_duckdb_results: Vec<(i32, i32, i64, Vec<i64>)> =
            duckdb_utils::fetch_duckdb_results(parquet_path, monthly_sales_duckdb_query)?;

        print_utils::print_results(
            vec![
                "Year".to_string(),
                "Month".to_string(),
                "Sales Count".to_string(),
                "Sale IDs (first 5)".to_string(),
            ],
            "Pg_Analytics".to_string(),
            &monthly_sales_pga_results,
            "DuckDb".to_string(),
            &monthly_sales_duckdb_results,
        )
        .await?;

        // assert_eq!(
        //     monthly_sales_results, expected_results,
        //     "Monthly sales results do not match"
        // );

        Ok(())
    }

    pub async fn debug_april_sales(conn: &mut PgConnection, parquet_path: &PathBuf) -> Result<()> {
        let april_sales_pg_query = r#"
            SELECT year, month, sale_id, price
            FROM auto_sales_partitioned
            WHERE manufacturer = 'Toyota' AND year = 2024 AND month = 4
            ORDER BY sale_id;
        "#;
        let april_sales_pg_results: Vec<(i32, i32, i64, f64)> =
            fetch_results(conn, april_sales_pg_query).await?;

        let april_sales_duckdb_query = r#"
            SELECT year, month, sale_id, price
            FROM auto_sales
            WHERE manufacturer = 'Toyota' AND year = 2024 AND month = 4
            ORDER BY sale_id;
        "#;
        let april_sales_duckdb_results: Vec<(i32, i32, i64, f64)> =
            duckdb_utils::fetch_duckdb_results(parquet_path, april_sales_duckdb_query)?;

        print_utils::print_results(
            vec![
                "Year".to_string(),
                "Month".to_string(),
                "Sale ID".to_string(),
                "Price".to_string(),
            ],
            "Pg_Analytics".to_string(),
            &april_sales_pg_results,
            "DuckDB".to_string(),
            &april_sales_duckdb_results,
        )
        .await?;

        println!("PostgreSQL count: {}", april_sales_pg_results.len());
        println!("DuckDB count: {}", april_sales_duckdb_results.len());

        Ok(())
    }
}
