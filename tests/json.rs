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
use datafusion::arrow::array::{LargeStringArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::{arrow::record_batch::RecordBatch, parquet::arrow::ArrowWriter};
use rstest::*;
use serde_json::json;
use sqlx::types::{Json, JsonValue};
use sqlx::PgConnection;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

use crate::fixtures::arrow::{primitive_create_foreign_data_wrapper, primitive_create_server};
use crate::fixtures::db::Query;
use crate::fixtures::{conn, tempdir};

pub fn json_string_record_batch() -> Result<RecordBatch> {
    let fields = vec![
        Field::new("string_col", DataType::Utf8, false),
        Field::new("large_string_col", DataType::LargeUtf8, false),
    ];

    let schema = Arc::new(Schema::new(fields));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![r#"{ "name": "joe", "age": 12 }"#])),
            Arc::new(LargeStringArray::from(vec![
                r#"{ "name": "joe", "age": 12 }"#,
            ])),
        ],
    )?)
}

#[rstest]
async fn test_json_cast_from_string(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = json_string_record_batch()?;
    let parquet_path = tempdir.path().join("test_json_cast_from_string.parquet");
    let parquet_file = File::create(&parquet_path)?;

    let mut writer = ArrowWriter::try_new(parquet_file, stored_batch.schema(), None).unwrap();
    writer.write(&stored_batch)?;
    writer.close()?;

    primitive_create_foreign_data_wrapper(
        "parquet_wrapper",
        "parquet_fdw_handler",
        "parquet_fdw_validator",
    )
    .execute(&mut conn);
    primitive_create_server("parquet_server", "parquet_wrapper").execute(&mut conn);
    format!(
        "CREATE FOREIGN TABLE json_table () SERVER parquet_server OPTIONS (files '{}')",
        parquet_path.to_str().unwrap()
    )
    .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT string_col::json->>'name', large_string_col::json->>'age' FROM json_table"
            .fetch(&mut conn);
    assert_eq!(rows, vec![("joe".to_string(), "12".to_string())]);

    let rows: Vec<(String, String)> =
        "SELECT string_col::jsonb->>'name', large_string_col::jsonb->>'age' FROM json_table"
            .fetch(&mut conn);
    assert_eq!(rows, vec![("joe".to_string(), "12".to_string())]);

    let rows: Vec<(Json<JsonValue>, Json<JsonValue>)> =
        "SELECT string_col::json, large_string_col::jsonb FROM json_table".fetch(&mut conn);
    assert_eq!(
        rows,
        vec![(
            Json::from(json!({ "name": "joe", "age": 12 })),
            Json::from(json!({ "name": "joe", "age": 12 }))
        )]
    );

    Ok(())
}
