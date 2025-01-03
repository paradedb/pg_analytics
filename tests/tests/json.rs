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
use datafusion::arrow::array::{
    ArrayBuilder, ArrowPrimitiveType, BooleanBuilder, LargeStringArray, LargeStringBuilder,
    ListArray, ListBuilder, PrimitiveBuilder, StringArray, StringBuilder, StructBuilder,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Int16Type, Int32Type, Int64Type, Int8Type, Schema,
};
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

fn boolean_list_array(boolean_values: Vec<Vec<Option<bool>>>) -> ListArray {
    let boolean_builder = BooleanBuilder::new();
    let mut list_builder = ListBuilder::new(boolean_builder);

    for values in boolean_values {
        for value in values {
            list_builder.values().append_option(value);
        }
        list_builder.append(true);
    }

    list_builder.finish()
}

fn primitive_list_array<T: ArrowPrimitiveType<Native: From<V>>, V>(
    values: Vec<Vec<Option<V>>>,
) -> ListArray {
    let builder = PrimitiveBuilder::<T>::new();
    let mut list_builder = ListBuilder::new(builder);

    for sublist in values {
        for value in sublist {
            list_builder
                .values()
                .append_option(value.map(|v| T::Native::from(v)));
        }
        list_builder.append(true);
    }

    list_builder.finish()
}

fn string_list_array(values: Vec<Vec<Option<&str>>>) -> ListArray {
    let builder = StringBuilder::new();
    let mut list_builder = ListBuilder::new(builder);

    for sublist in values {
        for value in sublist {
            list_builder.values().append_option(value);
        }
        list_builder.append(true);
    }

    list_builder.finish()
}

fn large_string_list_array(values: Vec<Vec<Option<&str>>>) -> ListArray {
    let builder = LargeStringBuilder::new();
    let mut list_builder = ListBuilder::new(builder);

    for sublist in values {
        for value in sublist {
            list_builder.values().append_option(value);
        }
        list_builder.append(true);
    }

    list_builder.finish()
}

pub fn json_list_record_batch() -> Result<RecordBatch> {
    let fields = vec![
        Field::new(
            "boolean_array",
            DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
            false,
        ),
        Field::new(
            "int8_array",
            DataType::List(Arc::new(Field::new("item", DataType::Int8, true))),
            false,
        ),
        Field::new(
            "int16_array",
            DataType::List(Arc::new(Field::new("item", DataType::Int16, true))),
            false,
        ),
        Field::new(
            "int32_array",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        ),
        Field::new(
            "int64_array",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        ),
        Field::new(
            "string_array",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "large_string_array",
            DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
            false,
        ),
    ];

    let schema = Arc::new(Schema::new(fields));

    let boolean_values = vec![
        vec![None, Some(false), Some(true)],
        vec![None, Some(true)],
        vec![Some(true), None, Some(false), Some(false)],
    ];
    let int_values = vec![
        vec![None, Some(1), Some(2)],
        vec![None, Some(3)],
        vec![Some(4), Some(5), None, Some(6)],
    ];
    let string_values = vec![
        vec![Some("abc"), None, Some("b")],
        vec![None, Some("ce")],
        vec![Some("d"), Some("e"), None, Some("f")],
    ];

    let boolean_array = Arc::new(boolean_list_array(boolean_values));
    let int8_array = Arc::new(primitive_list_array::<Int8Type, i8>(int_values.clone()));
    let int16_array = Arc::new(primitive_list_array::<Int16Type, _>(int_values.clone()));
    let int32_array = Arc::new(primitive_list_array::<Int32Type, _>(int_values.clone()));
    let int64_array = Arc::new(primitive_list_array::<Int64Type, _>(int_values.clone()));
    let string_array = Arc::new(string_list_array(string_values.clone()));
    let large_string_array = Arc::new(large_string_list_array(string_values.clone()));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            boolean_array,
            int8_array,
            int16_array,
            int32_array,
            int64_array,
            string_array,
            large_string_array,
        ],
    )?)
}

pub fn struct_list_record_batch() -> Result<RecordBatch> {
    let struct_fileds = vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ];
    let fields = vec![Field::new(
        "struct_array",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(struct_fileds.clone())),
            true,
        ))),
        false,
    )];

    let schema = Arc::new(Schema::new(fields));

    let struct_values = vec![
        vec![
            Some(("joe", 12)),
            None,
            Some(("jane", 13)),
            Some(("jim", 14)),
        ],
        vec![Some(("joe", 12))],
    ];

    let struct_array = {
        let mut struct_list_builder = ListBuilder::new(StructBuilder::new(
            struct_fileds,
            vec![
                Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
                Box::new(PrimitiveBuilder::<Int32Type>::new()) as Box<dyn ArrayBuilder>,
            ],
        ));

        for sublist in struct_values {
            for value in sublist {
                if let Some((name, age)) = value {
                    struct_list_builder.values().append(true);
                    struct_list_builder
                        .values()
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(name);
                    struct_list_builder
                        .values()
                        .field_builder::<PrimitiveBuilder<Int32Type>>(1)
                        .unwrap()
                        .append_value(age);
                } else {
                    struct_list_builder.values().append(false);
                    struct_list_builder
                        .values()
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_null();
                    struct_list_builder
                        .values()
                        .field_builder::<PrimitiveBuilder<Int32Type>>(1)
                        .unwrap()
                        .append_null();
                }
            }
            struct_list_builder.append(true);
        }
        struct_list_builder.finish()
    };

    Ok(RecordBatch::try_new(schema, vec![Arc::new(struct_array)])?)
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

#[rstest]
fn test_json_cast_from_list(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = json_list_record_batch()?;
    let parquet_path = tempdir.path().join("test_json_cast_from_list.parquet");
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
        "CREATE FOREIGN TABLE json_table (
            boolean_array jsonb,
            int8_array jsonb,
            int16_array jsonb,
            int32_array jsonb,
            int64_array jsonb,
            string_array jsonb,
            large_string_array jsonb
        ) SERVER parquet_server OPTIONS (files '{}')",
        parquet_path.to_str().unwrap()
    )
    .execute(&mut conn);

    let r = "SELECT * FROM json_table".execute_result(&mut conn);
    assert!(r.is_ok(), "error in query:'{}'", r.unwrap_err());

    let row: (Json<JsonValue>,) =
        "SELECT int8_array FROM json_table where int8_array = '[null, 3]'".fetch_one(&mut conn);
    assert_eq!(row.0, Json::from(json!([null, 3])));

    Ok(())
}

#[rstest]
fn test_json_cast_from_struct_list(mut conn: PgConnection, tempdir: TempDir) -> Result<()> {
    let stored_batch = struct_list_record_batch()?;
    let parquet_path = tempdir
        .path()
        .join("test_json_cast_from_struct_list.parquet");
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
        "CREATE FOREIGN TABLE json_table ()
         SERVER parquet_server OPTIONS (files '{}')",
        parquet_path.to_str().unwrap()
    )
    .execute(&mut conn);

    let r = "SELECT * FROM json_table".execute_result(&mut conn);
    assert!(r.is_ok(), "error in query:'{}'", r.unwrap_err());

    let row: (Json<JsonValue>,) =
        "SELECT struct_array FROM json_table where struct_array = '[{\"name\": \"joe\", \"age\": 12}]'"
            .fetch_one(&mut conn);
    assert_eq!(row.0, Json::from(json!([{"name": "joe", "age": 12}])));

    Ok(())
}
