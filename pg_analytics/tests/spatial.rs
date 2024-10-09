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

use crate::fixtures::{arrow::primitive_setup_fdw_local_file_spatial, conn, db::Query, tempdir};
use anyhow::Result;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_binary_array;
use geojson::{Feature, GeoJson, Geometry, Value};
use rstest::rstest;
use sqlx::PgConnection;

use std::sync::Arc;
use tempfile::TempDir;

// TODO: Currently, arrow-rs lacks support for geometry types, restricting this test to non-geometry data.
// Once geometry support is available or a suitable workaround is found, expand this test to include geometry types.
#[rstest]
async fn test_arrow_types_local_file_spatial(
    mut conn: PgConnection,
    tempdir: TempDir,
) -> Result<()> {
    let temp_path = tempdir.path().join("test_spatial.geojson");
    let geometry = Geometry::new(Value::Point(vec![-120.66029, 35.2812]));
    let geojson = GeoJson::Feature(Feature {
        bbox: None,
        geometry: Some(geometry),
        id: None,
        properties: None,
        foreign_members: None,
    });
    let geojson_string = geojson.to_string();
    std::fs::write(&temp_path, &geojson_string)?;

    let field = Field::new("geom", DataType::Binary, false);
    let schema = Arc::new(Schema::new(vec![field]));
    // arrow-rs cannot parse the geojson string directly, so we need to hardcode the binary data
    let data: Vec<&[u8]> = vec![&[
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 14, 248, 252, 48, 66, 42, 94, 192, 78, 209,
        145, 92, 254, 163, 65, 64,
    ]];
    let batch = RecordBatch::try_new(schema, vec![Arc::new(BinaryArray::from(data.clone()))])?;

    primitive_setup_fdw_local_file_spatial(
        temp_path.to_string_lossy().as_ref(),
        "spatial_primitive",
    )
    .execute(&mut conn);

    let retrieved_batch =
        "SELECT * FROM spatial_primitive".fetch_recordbatch(&mut conn, batch.schema_ref());

    assert_eq!(batch.num_columns(), retrieved_batch.num_columns());
    let array = as_binary_array(retrieved_batch.column(0))?.value(0);
    assert_eq!(array, data[0]);

    Ok(())
}
