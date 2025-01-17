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

use crate::fixtures::db::Query;
use crate::fixtures::{conn, s3, S3};
use anyhow::Result;
use rstest::*;
use sqlx::PgConnection;

use crate::fixtures::tables::nyc_trips::NycTripsTable;

const S3_BUCKET: &str = "test-trip-setup";
const S3_KEY: &str = "test_trip_setup.parquet";

#[rstest]
#[ignore = "EXPLAIN not fully working"]
async fn test_explain_fdw(#[future(awt)] s3: S3, mut conn: PgConnection) -> Result<()> {
    NycTripsTable::setup().execute(&mut conn);
    let rows: Vec<NycTripsTable> = "SELECT * FROM nyc_trips".fetch(&mut conn);
    s3.client.create_bucket().bucket(S3_BUCKET).send().await?;
    s3.create_bucket(S3_BUCKET).await?;
    s3.put_rows(S3_BUCKET, S3_KEY, &rows).await?;

    NycTripsTable::setup_s3_listing_fdw(&s3.url.clone(), &format!("s3://{S3_BUCKET}/{S3_KEY}"))
        .execute(&mut conn);

    let explain: Vec<(String,)> =
        "EXPLAIN SELECT COUNT(*) FROM trips WHERE tip_amount <> 0".fetch(&mut conn);

    assert!(explain[0].0.contains("DataFusionScan: LogicalPlan"));
    assert!(explain[1].0.contains("Projection: COUNT(*)"));
    assert!(explain[2]
        .0
        .contains("Aggregate: groupBy=[[]], aggr=[[COUNT(*)]]"));
    assert!(explain[3]
        .0
        .contains("Filter: trips.tip_amount != Int64(0)"));
    assert!(explain[4].0.contains("TableScan: trips"));

    Ok(())
}

#[rstest]
async fn test_explain_heap(mut conn: PgConnection) -> Result<()> {
    NycTripsTable::setup().execute(&mut conn);

    let explain: Vec<(String,)> =
        "EXPLAIN SELECT COUNT(*) FROM nyc_trips WHERE tip_amount <> 0".fetch(&mut conn);

    assert!(explain[0].0.contains("Aggregate"));
    assert!(explain[1].0.contains("Seq Scan on nyc_trips"));
    assert!(explain[2].0.contains("Filter"));

    Ok(())
}

#[rstest]
#[ignore = "EXPLAIN not fully working"]
async fn test_explain_federated(#[future(awt)] s3: S3, mut conn: PgConnection) -> Result<()> {
    NycTripsTable::setup().execute(&mut conn);
    let rows: Vec<NycTripsTable> = "SELECT * FROM nyc_trips".fetch(&mut conn);
    s3.client.create_bucket().bucket(S3_BUCKET).send().await?;
    s3.create_bucket(S3_BUCKET).await?;
    s3.put_rows(S3_BUCKET, S3_KEY, &rows).await?;

    NycTripsTable::setup_s3_listing_fdw(&s3.url.clone(), &format!("s3://{S3_BUCKET}/{S3_KEY}"))
        .execute(&mut conn);

    let explain: Vec<(String,)> =
        "EXPLAIN SELECT COUNT(*) FROM trips LEFT JOIN nyc_trips ON trips.\"VendorID\" = nyc_trips.\"VendorID\"".fetch(&mut conn);

    assert!(explain[0].0.contains("Aggregate"));
    assert!(explain[1].0.contains("Hash Right Join"));
    assert!(explain[2]
        .0
        .contains("Hash Cond: (nyc_trips.\"VendorID\" = trips.\"VendorID\")"));
    assert!(explain[3].0.contains("Seq Scan on nyc_trips"));
    assert!(explain[4].0.contains("Hash"));
    assert!(explain[5].0.contains("Foreign Scan on trips"));

    Ok(())
}

#[rstest]
#[ignore = "EXPLAIN not fully working"]
async fn test_explain_analyze_fdw(#[future(awt)] s3: S3, mut conn: PgConnection) -> Result<()> {
    NycTripsTable::setup().execute(&mut conn);
    let rows: Vec<NycTripsTable> = "SELECT * FROM nyc_trips".fetch(&mut conn);
    s3.client.create_bucket().bucket(S3_BUCKET).send().await?;
    s3.create_bucket(S3_BUCKET).await?;
    s3.put_rows(S3_BUCKET, S3_KEY, &rows).await?;

    NycTripsTable::setup_s3_listing_fdw(&s3.url.clone(), &format!("s3://{S3_BUCKET}/{S3_KEY}"))
        .execute(&mut conn);

    let explain: Vec<(String,)> =
        "EXPLAIN ANALYZE SELECT COUNT(*) FROM trips WHERE tip_amount <> 0".fetch(&mut conn);

    assert!(explain[0].0.contains("AggregateExec"));
    assert!(explain[1].0.contains("CoalescePartitionsExec"));
    assert!(explain[2].0.contains("AggregateExec"));
    assert!(explain[3].0.contains("RepartitionExec"));
    assert!(explain[4].0.contains("ProjectionExec"));
    assert!(explain[5].0.contains("CoalesceBatchesExec"));
    assert!(explain[6].0.contains("FilterExec"));
    assert!(explain[7].0.contains("ParquetExec"));

    Ok(())
}

#[rstest]
async fn test_explain_analyze_heap(mut conn: PgConnection) -> Result<()> {
    NycTripsTable::setup().execute(&mut conn);

    let explain: Vec<(String,)> =
        "EXPLAIN ANALYZE SELECT COUNT(*) FROM nyc_trips WHERE tip_amount <> 0".fetch(&mut conn);

    assert!(explain[0].0.contains("Aggregate"));
    assert!(explain[1].0.contains("Seq Scan on nyc_trips"));
    assert!(explain[2].0.contains("Filter"));
    assert!(explain[3].0.contains("Rows Removed by Filter"));
    assert!(explain[4].0.contains("Planning Time"));
    assert!(explain[5].0.contains("Execution Time"));

    Ok(())
}

#[rstest]
#[ignore = "not passing... 'postgres FFI may not not be called from multiple threads'"]
async fn test_explain_analyze_federated(
    #[future(awt)] s3: S3,
    mut conn: PgConnection,
) -> Result<()> {
    NycTripsTable::setup().execute(&mut conn);
    let rows: Vec<NycTripsTable> = "SELECT * FROM nyc_trips".fetch(&mut conn);
    s3.client.create_bucket().bucket(S3_BUCKET).send().await?;
    s3.create_bucket(S3_BUCKET).await?;
    s3.put_rows(S3_BUCKET, S3_KEY, &rows).await?;

    NycTripsTable::setup_s3_listing_fdw(&s3.url.clone(), &format!("s3://{S3_BUCKET}/{S3_KEY}"))
        .execute(&mut conn);

    let explain: Vec<(String,)> =
        "EXPLAIN ANALYZE SELECT COUNT(*) FROM trips LEFT JOIN nyc_trips ON trips.\"VendorID\" = nyc_trips.\"VendorID\"".fetch(&mut conn);

    assert!(explain[0].0.contains("Aggregate"));
    assert!(explain[1].0.contains("Hash Right Join"));
    assert!(explain[2]
        .0
        .contains("Hash Cond: (nyc_trips.\"VendorID\" = trips.\"VendorID\")"));
    assert!(explain[3].0.contains("Seq Scan on nyc_trips"));
    assert!(explain[4].0.contains("Hash"));
    assert!(explain[5].0.contains("Buckets"));
    assert!(explain[6].0.contains("Foreign Scan on trips"));

    Ok(())
}

#[rstest]
async fn test_explain_foreign_table(#[future(awt)] s3: S3, mut conn: PgConnection) -> Result<()> {
    NycTripsTable::setup().execute(&mut conn);

    let rows: Vec<NycTripsTable> = "SELECT * FROM nyc_trips".fetch(&mut conn);
    s3.client.create_bucket().bucket(S3_BUCKET).send().await?;
    s3.create_bucket(S3_BUCKET).await?;
    s3.put_rows(S3_BUCKET, S3_KEY, &rows).await?;

    NycTripsTable::setup_s3_listing_fdw(&s3.url.clone(), &format!("s3://{S3_BUCKET}/{S3_KEY}"))
        .execute(&mut conn);

    let explain: Vec<(String,)> = "EXPLAIN SELECT COUNT(*) FROM trips".fetch(&mut conn);
    assert_eq!(explain[0].0, "DuckDB Scan: SELECT COUNT(*) FROM trips");

    let explain: Result<Vec<(String,)>, sqlx::Error> =
        "EXPLAIN verbose SELECT COUNT(*) FROM trips".fetch_result(&mut conn);
    assert!(explain.is_err());

    Ok(())
}

#[rstest]
async fn test_explain_foreign_table_duckdb_style(
    #[future(awt)] s3: S3,
    mut conn: PgConnection,
) -> Result<()> {
    NycTripsTable::setup().execute(&mut conn);

    let rows: Vec<NycTripsTable> = "SELECT * FROM nyc_trips".fetch(&mut conn);
    s3.client.create_bucket().bucket(S3_BUCKET).send().await?;
    s3.create_bucket(S3_BUCKET).await?;
    s3.put_rows(S3_BUCKET, S3_KEY, &rows).await?;

    NycTripsTable::setup_s3_listing_fdw(&s3.url.clone(), &format!("s3://{S3_BUCKET}/{S3_KEY}"))
        .execute(&mut conn);

    let explain: Vec<(String,)> = "EXPLAIN SELECT COUNT(*) FROM trips".fetch(&mut conn);
    assert_eq!(explain[0].0, "DuckDB Scan: SELECT COUNT(*) FROM trips");

    let explain: Result<Vec<(String,)>, sqlx::Error> =
        "EXPLAIN (style duckdb) SELECT COUNT(*) FROM trips".fetch_result(&mut conn);

    let expected_plan = vec![
        "┌───────────────────────────┐",
        "│    UNGROUPED_AGGREGATE    │",
        "│    ────────────────────   │",
        "│        Aggregates:        │",
        "│        count_star()       │",
        "└─────────────┬─────────────┘",
        "┌─────────────┴─────────────┐",
        "│         PROJECTION        │",
        "│    ────────────────────   │",
        "│             42            │",
        "│                           │",
        "│         ~100 Rows         │",
        "└─────────────┬─────────────┘",
        "┌─────────────┴─────────────┐",
        "│       READ_PARQUET        │",
        "│    ────────────────────   │",
        "│         Function:         │",
        "│        READ_PARQUET       │",
        "│                           │",
        "│         ~100 Rows         │",
        "└───────────────────────────┘",
    ];

    assert!(explain.is_ok());
    if let Ok(plan) = explain {
        assert_eq!(plan.len(), expected_plan.len());
        for ((row,), expect_row) in plan.iter().zip(expected_plan) {
            assert_eq!(row, expect_row);
        }
    }

    // test (style duckdb, analyze)
    let explain: Result<Vec<(String,)>, sqlx::Error> =
        "EXPLAIN (style duckdb, analyze) SELECT COUNT(*) FROM trips".fetch_result(&mut conn);

    let expected_plan = vec![
        "┌─────────────────────────────────────┐",
        "│┌───────────────────────────────────┐│",
        "││    Query Profiling Information    ││",
        "│└───────────────────────────────────┘│",
        "└─────────────────────────────────────┘",
        "EXPLAIN ANALYZE SELECT COUNT(*) FROM trips",
        "┌─────────────────────────────────────┐",
        "│┌───────────────────────────────────┐│",
        "││         HTTPFS HTTP Stats         ││",
        "││                                   ││",
        "││            in: 3.0 KiB            ││",
        "││            out: 0 bytes           ││",
        "││              #HEAD: 1             ││",
        "││              #GET: 2              ││",
        "││              #PUT: 0              ││",
        "││              #POST: 0             ││",
        "│└───────────────────────────────────┘│",
        "└─────────────────────────────────────┘",
        "┌────────────────────────────────────────────────┐",
        "│┌──────────────────────────────────────────────┐│",
        "││              Total Time: 0.0007s             ││",
        "│└──────────────────────────────────────────────┘│",
        "└────────────────────────────────────────────────┘",
        "┌───────────────────────────┐",
        "│           QUERY           │",
        "│    ────────────────────   │",
        "│           0 Rows          │",
        "│          (0.00s)          │",
        "└─────────────┬─────────────┘",
        "┌─────────────┴─────────────┐",
        "│      EXPLAIN_ANALYZE      │",
        "│    ────────────────────   │",
        "│           0 Rows          │",
        "│          (0.00s)          │",
        "└─────────────┬─────────────┘",
        "┌─────────────┴─────────────┐",
        "│    UNGROUPED_AGGREGATE    │",
        "│    ────────────────────   │",
        "│        Aggregates:        │",
        "│        count_star()       │",
        "│                           │",
        "│           1 Rows          │",
        "│          (0.00s)          │",
        "└─────────────┬─────────────┘",
        "┌─────────────┴─────────────┐",
        "│         PROJECTION        │",
        "│    ────────────────────   │",
        "│             42            │",
        "│                           │",
        "│          100 Rows         │",
        "│          (0.00s)          │",
        "└─────────────┬─────────────┘",
        "┌─────────────┴─────────────┐",
        "│         TABLE_SCAN        │",
        "│    ────────────────────   │",
        "│         Function:         │",
        "│        READ_PARQUET       │",
        "│                           │",
        "│          100 Rows         │",
        "│          (0.00s)          │",
        "└───────────────────────────┘",
    ];
    assert!(explain.is_ok());
    if let Ok(plan) = explain {
        assert_eq!(plan.len(), expected_plan.len());
        for ((row,), expect_row) in plan.iter().zip(expected_plan) {
            if expect_row.contains("Time") {
                continue;
            }
            assert_eq!(row, expect_row);
        }
    }
    Ok(())
}
