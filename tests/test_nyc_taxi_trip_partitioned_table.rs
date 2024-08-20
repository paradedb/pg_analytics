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

use anyhow::Result;
use fixtures::*;
use rstest::*;
use sqlx::PgConnection;
use std::collections::HashMap;

use tracing_subscriber::{fmt, EnvFilter};

pub fn init_tracer() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));

    fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .try_init()
        .ok(); // It's okay if this fails, it just means a global subscriber has already been set
}

impl TestPartitionTable for NycTripsTable {}

trait TestPartitionTable {
    fn setup_s3_parquet_fdw(s3_endpoint: &str, s3_bucket: &str) -> String {
        let create_fdw = "CREATE FOREIGN DATA WRAPPER parquet_wrapper HANDLER parquet_fdw_handler VALIDATOR parquet_fdw_validator";
        let create_server = "CREATE SERVER parquet_server FOREIGN DATA WRAPPER parquet_wrapper";
        let create_user_mapping = "CREATE USER MAPPING FOR public SERVER parquet_server";
        let create_table = Self::create_partitioned_table(s3_bucket);

        format!(
            r#"
                {create_fdw};
                {create_server};
                {create_user_mapping} OPTIONS (type 'S3', region 'us-east-1', endpoint '{s3_endpoint}', use_ssl 'false', url_style 'path');
                {create_table};
            "#
        )
    }

    fn create_partitioned_table(s3_bucket: &str) -> String {
        format!(
            r#"
            CREATE TABLE nyc_trips_main (
                "VendorID"              INT,
                "tpep_pickup_datetime"  TIMESTAMP,
                "tpep_dropoff_datetime" TIMESTAMP,
                "passenger_count"       BIGINT,
                "trip_distance"         DOUBLE PRECISION,
                "RatecodeID"            DOUBLE PRECISION,
                "store_and_fwd_flag"    TEXT,
                "PULocationID"          REAL,
                "DOLocationID"          REAL,
                "payment_type"          DOUBLE PRECISION,
                "fare_amount"           DOUBLE PRECISION,
                "extra"                 DOUBLE PRECISION,
                "mta_tax"               DOUBLE PRECISION,
                "tip_amount"            DOUBLE PRECISION,
                "tolls_amount"          DOUBLE PRECISION,
                "improvement_surcharge" DOUBLE PRECISION,
                "total_amount"          DOUBLE PRECISION
            )
            PARTITION BY LIST ("VendorID");

            -- First-level partitions by VendorID
            CREATE TABLE nyc_trips_vendor_1 PARTITION OF nyc_trips_main
                FOR VALUES IN (1)
                PARTITION BY RANGE ("PULocationID");

            CREATE TABLE nyc_trips_vendor_2 PARTITION OF nyc_trips_main
                FOR VALUES IN (2)
                PARTITION BY RANGE ("PULocationID");

            -- Second-level partitions for vendor 1 by PULocationID ranges
            CREATE FOREIGN TABLE nyc_trips_vendor_1_loc_0_100 PARTITION OF nyc_trips_vendor_1
                FOR VALUES FROM (0) TO (100)
                SERVER parquet_server
                OPTIONS (files 's3://{s3_bucket}/nyc_trips/vendor_1/loc_0_100/*.parquet');

            CREATE FOREIGN TABLE nyc_trips_vendor_1_loc_100_200 PARTITION OF nyc_trips_vendor_1
                FOR VALUES FROM (100) TO (200)
                SERVER parquet_server
                OPTIONS (files 's3://{s3_bucket}/nyc_trips/vendor_1/loc_100_200/*.parquet');

            -- Second-level partitions for vendor 2 by PULocationID ranges
            CREATE FOREIGN TABLE nyc_trips_vendor_2_loc_0_100 PARTITION OF nyc_trips_vendor_2
                FOR VALUES FROM (0) TO (100)
                SERVER parquet_server
                OPTIONS (files 's3://{s3_bucket}/nyc_trips/vendor_2/loc_0_100/*.parquet');

            CREATE FOREIGN TABLE nyc_trips_vendor_2_loc_100_200 PARTITION OF nyc_trips_vendor_2
                FOR VALUES FROM (100) TO (200)
                SERVER parquet_server
                OPTIONS (files 's3://{s3_bucket}/nyc_trips/vendor_2/loc_100_200/*.parquet');
            "#
        )
    }
}

// Helper function to determine the location range
fn get_location_range(pu_location_id: f32) -> u32 {
    if pu_location_id < 100.0 {
        0
    } else if pu_location_id < 200.0 {
        100
    } else {
        200
    }
}

#[rstest]
async fn test_partitioned_nyctaxi_trip_s3_parquet(
    #[future(awt)] s3: S3,
    mut conn: PgConnection,
) -> Result<()> {
    // Initialize the tracer
    init_tracer();

    tracing::error!("test_partitioned_nyctaxi_trip_s3_parquet Started !!!");

    // Set up S3 buckets and sample data
    let s3_bucket = "test-nyctaxi-trip-setup";
    let s3_endpoint = s3.url.clone();

    // Set up the nyc_trips table and insert sample data
    NycTripsTable::setup().execute(&mut conn);

    // Fetch the sample data
    let rows: Vec<NycTripsTable> = "SELECT * FROM nyc_trips".fetch(&mut conn);

    // Create S3 bucket and upload data
    s3.create_bucket(s3_bucket).await?;

    // Group rows by VendorID and PULocationID range
    let mut grouped_rows: HashMap<(i32, u32), Vec<NycTripsTable>> = HashMap::new();
    for row in rows {
        let vendor_id = row.vendor_id.expect("Invalid VendorID !!!");
        let pu_location_id = row.pu_location_id.expect("Invalid PULocationID !!!");
        let location_range = get_location_range(pu_location_id);
        let key = (vendor_id, location_range);
        grouped_rows.entry(key).or_default().push(row);
    }

    // Upload data to S3
    for ((vendor_id, location_range), rows) in grouped_rows {
        let s3_key = format!(
            "nyc_trips/vendor_{vendor_id}/loc_{location_range}_{}/data.parquet",
            location_range + 100
        );
        s3.put_rows(s3_bucket, &s3_key, &rows).await?;
    }

    tracing::error!("Kom-1.1 !!!");

    // Set up Foreign Data Wrapper for S3
    NycTripsTable::setup_s3_parquet_fdw(&s3_endpoint, s3_bucket).execute(&mut conn);

    tracing::error!("Kom-2.1 !!!");

    // Run test queries
    let query =
        r#"SELECT * FROM nyc_trips WHERE "VendorID" = 1 AND "PULocationID" BETWEEN 0 AND 99.99"#;
    let results: Vec<NycTripsTable> = query.fetch(&mut conn);

    // Assert results
    assert!(!results.is_empty(), "Query should return results");

    tracing::error!("Kom-3.1 !!!");

    tracing::error!("{:#?} !!!", &results.len());

    for row in results {
        assert_eq!(
            row.vendor_id,
            Some(1),
            "All results should be from vendor 1"
        );
        assert!(
            row.pu_location_id.unwrap() >= 0.0 && row.pu_location_id.unwrap() < 100.0,
            "All results should have PULocationID between 0 and 100"
        );
    }

    Ok(())
}
