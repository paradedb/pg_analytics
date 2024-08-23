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

//! Tests for DuckDB Geospatial Extension

mod fixtures;

use crate::fixtures::{arrow::primitive_setup_fdw_local_file_spatial, conn, db::Query};
use anyhow::Result;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_binary_array;
use rstest::rstest;
use sqlx::PgConnection;
use std::sync::Arc;

// TODO: Currently, arrow-rs lacks support for geometry types, restricting this test to non-geometry data.
// Once geometry support is available or a suitable workaround is found, expand this test to include geometry types.
#[rstest]
async fn test_arrow_types_local_file_sptail(mut conn: PgConnection) -> Result<()> {
    let current_path = std::env::current_dir()?;
    let file_path = current_path.join("tests/data/test.geojson");

    let field = Field::new("geom", DataType::Binary, false);
    let schema = Arc::new(Schema::new(vec![field]));

    let data: Vec<u8> = vec![2, 4];
    let data: Vec<&[u8]> = data.chunks(2).collect();

    let batch = RecordBatch::try_new(schema, vec![Arc::new(BinaryArray::from(data))])?;

    primitive_setup_fdw_local_file_spatial(
        file_path.to_string_lossy().as_ref(),
        "spatial_primitive",
    )
    .execute(&mut conn);

    let retrieved_batch =
        "SELECT * FROM spatial_primitive".fetch_recordbatch(&mut conn, batch.schema_ref());

    assert_eq!(batch.num_columns(), retrieved_batch.num_columns());
    let array = as_binary_array(retrieved_batch.column(0))?
        .value(0)
        .get(0..2)
        .unwrap();
    let expected_array = as_binary_array(batch.column_by_name("geom").unwrap())?.value(0);
    assert_eq!(array, expected_array);

    Ok(())
}
