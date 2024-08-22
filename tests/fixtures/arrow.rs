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

use std::sync::Arc;

use anyhow::{bail, Result};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::arrow::array::*;
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use pgrx::pg_sys::InvalidOid;
use pgrx::PgBuiltInOids;
use sqlx::postgres::PgRow;
use sqlx::{Postgres, Row, TypeInfo, ValueRef};

fn array_data() -> ArrayData {
    let values: [u8; 12] = *b"helloparquet";
    let offsets: [i32; 4] = [0, 5, 5, 12]; // Note: Correct the offsets to accurately reflect boundaries

    ArrayData::builder(DataType::Binary)
        .len(3) // Set length to 3 to match other arrays
        .add_buffer(Buffer::from_slice_ref(&offsets[..]))
        .add_buffer(Buffer::from_slice_ref(&values[..]))
        .build()
        .unwrap()
}

// Fixed size binary is not supported yet, but this will be useful for test data when we do support.
#[allow(unused)]
fn fixed_size_array_data() -> ArrayData {
    let values: [u8; 15] = *b"hellotherearrow"; // Ensure length is consistent

    ArrayData::builder(DataType::FixedSizeBinary(5))
        .len(3)
        .add_buffer(Buffer::from(&values[..]))
        .build()
        .unwrap()
}

fn binary_array_data() -> ArrayData {
    let values: [u8; 12] = *b"helloparquet";
    let offsets: [i64; 4] = [0, 5, 5, 12];

    ArrayData::builder(DataType::LargeBinary)
        .len(3) // Ensure length is consistent
        .add_buffer(Buffer::from_slice_ref(&offsets[..]))
        .add_buffer(Buffer::from_slice_ref(&values[..]))
        .build()
        .unwrap()
}

#[derive(Debug)]
struct FieldDesc {
    name: String,
    data_type: DataType,
    nullable: bool,
    pg_type: String,
}

impl<T> From<(T, DataType, bool, T)> for FieldDesc
where
    T: ToString,
{
    fn from(value: (T, DataType, bool, T)) -> Self {
        Self {
            name: value.0.to_string(),
            data_type: value.1,
            nullable: value.2,
            pg_type: value.3.to_string(),
        }
    }
}

#[derive(Debug, Default)]
pub struct FieldSpec(Vec<FieldDesc>);

impl FieldSpec {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn arrow_schema(&self) -> Schema {
        let fields = self
            .0
            .iter()
            .map(|fd| Field::new(&fd.name, fd.data_type.clone(), fd.nullable))
            .collect::<Vec<_>>();
        Schema::new(fields)
    }

    pub fn postgres_schema(&self) -> Vec<(&str, &str)> {
        self.0
            .iter()
            .map(|fd| (fd.name.as_str(), fd.pg_type.as_str()))
            .collect::<Vec<_>>()
    }
}

impl<T> From<Vec<(T, DataType, bool, T)>> for FieldSpec
where
    T: ToString,
{
    fn from(value: Vec<(T, DataType, bool, T)>) -> Self {
        Self(value.into_iter().map(FieldDesc::from).collect())
    }
}

/// A separate version of the primitive_record_batch fixture,
/// narrowed to only the types that Delta Lake supports.
pub fn delta_primitive_record_batch() -> Result<RecordBatch> {
    let fields = vec![
        Field::new("boolean_col", DataType::Boolean, false),
        Field::new("int8_col", DataType::Int8, false),
        Field::new("int16_col", DataType::Int16, false),
        Field::new("int32_col", DataType::Int32, false),
        Field::new("int64_col", DataType::Int64, false),
        Field::new("float32_col", DataType::Float32, false),
        Field::new("float64_col", DataType::Float64, false),
        Field::new("date32_col", DataType::Date32, false),
        Field::new("binary_col", DataType::Binary, false),
        Field::new("utf8_col", DataType::Utf8, false),
    ];

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BooleanArray::from(vec![true, true, false])),
            Arc::new(Int8Array::from(vec![1, -1, 0])),
            Arc::new(Int16Array::from(vec![1, -1, 0])),
            Arc::new(Int32Array::from(vec![1, -1, 0])),
            Arc::new(Int64Array::from(vec![1, -1, 0])),
            Arc::new(Float32Array::from(vec![1.0, -1.0, 0.0])),
            Arc::new(Float64Array::from(vec![1.0, -1.0, 0.0])),
            Arc::new(Date32Array::from(vec![18262, 18263, 18264])),
            Arc::new(BinaryArray::from(array_data())),
            Arc::new(StringArray::from(vec![
                Some("Hello"),
                Some("There"),
                Some("World"),
            ])),
        ],
    )?;

    Ok(batch)
}

// Used to test case sensitivity in column names
pub fn record_batch_with_casing() -> Result<RecordBatch> {
    let fields = vec![Field::new("Boolean_Col", DataType::Boolean, false)];

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(BooleanArray::from(vec![true, true, false]))],
    )?;

    Ok(batch)
}

// Blows up deltalake, so comment out for now.
pub fn primitive_record_batch() -> Result<RecordBatch> {
    // Define the fields for each datatype
    let fields = vec![
        Field::new("boolean_col", DataType::Boolean, true),
        Field::new("int8_col", DataType::Int8, false),
        Field::new("int16_col", DataType::Int16, false),
        Field::new("int32_col", DataType::Int32, false),
        Field::new("int64_col", DataType::Int64, false),
        Field::new("uint8_col", DataType::UInt8, false),
        Field::new("uint16_col", DataType::UInt16, false),
        Field::new("uint32_col", DataType::UInt32, false),
        Field::new("uint64_col", DataType::UInt64, false),
        Field::new("float32_col", DataType::Float32, false),
        Field::new("float64_col", DataType::Float64, false),
        Field::new("date32_col", DataType::Date32, false),
        Field::new("date64_col", DataType::Date64, false),
        Field::new("binary_col", DataType::Binary, false),
        Field::new("large_binary_col", DataType::LargeBinary, false),
        Field::new("utf8_col", DataType::Utf8, false),
        Field::new("large_utf8_col", DataType::LargeUtf8, false),
    ];

    // Create a schema from the fields
    let schema = Arc::new(Schema::new(fields));

    // Create a RecordBatch
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
            ])),
            Arc::new(Int8Array::from(vec![1, -1, 0])),
            Arc::new(Int16Array::from(vec![1, -1, 0])),
            Arc::new(Int32Array::from(vec![1, -1, 0])),
            Arc::new(Int64Array::from(vec![1, -1, 0])),
            Arc::new(UInt8Array::from(vec![1, 2, 0])),
            Arc::new(UInt16Array::from(vec![1, 2, 0])),
            Arc::new(UInt32Array::from(vec![1, 2, 0])),
            Arc::new(UInt64Array::from(vec![1, 2, 0])),
            Arc::new(Float32Array::from(vec![1.0, -1.0, 0.0])),
            Arc::new(Float64Array::from(vec![1.0, -1.0, 0.0])),
            Arc::new(Date32Array::from(vec![18262, 18263, 18264])),
            Arc::new(Date64Array::from(vec![
                1609459200000,
                1609545600000,
                1609632000000,
            ])),
            Arc::new(BinaryArray::from(array_data())),
            Arc::new(LargeBinaryArray::from(binary_array_data())),
            Arc::new(StringArray::from(vec![
                Some("Hello"),
                Some("There"),
                Some("World"),
            ])),
            Arc::new(LargeStringArray::from(vec![
                Some("Hello"),
                Some("There"),
                Some("World"),
            ])),
        ],
    )?)
}

pub fn primitive_create_foreign_data_wrapper(
    wrapper: &str,
    handler: &str,
    validator: &str,
) -> String {
    format!("CREATE FOREIGN DATA WRAPPER {wrapper} HANDLER {handler} VALIDATOR {validator}")
}

pub fn primitive_create_server(server: &str, wrapper: &str) -> String {
    format!("CREATE SERVER {server} FOREIGN DATA WRAPPER {wrapper}")
}

pub fn primitive_create_user_mapping_options(user: &str, server: &str) -> String {
    format!("CREATE USER MAPPING FOR {user} SERVER {server}",)
}

pub fn auto_create_table(server: &str, table: &str) -> String {
    format!("CREATE FOREIGN TABLE {table} () SERVER {server}")
}

fn create_field_definition(fields: &[(&str, &str)]) -> String {
    fields
        .iter()
        .map(|(field_name, field_type)| format!("{field_name} {field_type}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub fn create_foreign_table(server: &str, table: &str, fields: &[(&str, &str)]) -> String {
    let fields_definition = create_field_definition(fields);
    format!("CREATE FOREIGN TABLE {table} ({fields_definition}) SERVER {server}")
}

pub fn setup_fdw_local_parquet_file_listing(
    local_file_path: &str,
    table: &str,
    fields: &[(&str, &str)],
) -> String {
    let create_foreign_data_wrapper = primitive_create_foreign_data_wrapper(
        "parquet_wrapper",
        "parquet_fdw_handler",
        "parquet_fdw_validator",
    );
    let create_server = primitive_create_server("parquet_server", "parquet_wrapper");
    let create_table = create_foreign_table("parquet_server", table, fields);

    format!(
        r#"
        {create_foreign_data_wrapper};
        {create_server};
        {create_table} OPTIONS (files '{local_file_path}'); 
    "#
    )
}

// Some fields have been commented out to get tests to pass
// See https://github.com/paradedb/paradedb/issues/1299
fn primitive_table_columns() -> Vec<(&'static str, &'static str)> {
    vec![
        ("boolean_col", "boolean"),
        ("int8_col", "smallint"),
        ("int16_col", "smallint"),
        ("int32_col", "integer"),
        ("int64_col", "bigint"),
        ("uint8_col", "smallint"),
        ("uint16_col", "integer"),
        ("uint32_col", "bigint"),
        ("uint64_col", "numeric(20)"),
        ("float32_col", "real"),
        ("float64_col", "double precision"),
        ("date32_col", "date"),
        ("date64_col", "date"),
        ("binary_col", "bytea"),
        ("large_binary_col", "bytea"),
        ("utf8_col", "text"),
        ("large_utf8_col", "text"),
    ]
}

pub fn primitive_create_table(server: &str, table: &str) -> String {
    create_foreign_table(server, table, &primitive_table_columns())
}

fn primitive_delta_table_columns() -> Vec<(&'static str, &'static str)> {
    vec![
        ("boolean_col", "boolean"),
        ("int8_col", "smallint"),
        ("int16_col", "smallint"),
        ("int32_col", "integer"),
        ("int64_col", "bigint"),
        ("float32_col", "real"),
        ("float64_col", "double precision"),
        ("date32_col", "date"),
        ("binary_col", "bytea"),
        ("utf8_col", "text"),
    ]
}

pub fn primitive_create_delta_table(server: &str, table: &str) -> String {
    create_foreign_table(server, table, &primitive_delta_table_columns())
}

pub fn primitive_setup_fdw_s3_listing(
    s3_endpoint: &str,
    s3_object_path: &str,
    table: &str,
) -> String {
    let create_foreign_data_wrapper = primitive_create_foreign_data_wrapper(
        "parquet_wrapper",
        "parquet_fdw_handler",
        "parquet_fdw_validator",
    );
    let create_user_mapping_options =
        primitive_create_user_mapping_options("public", "parquet_server");
    let create_server = primitive_create_server("parquet_server", "parquet_wrapper");
    let create_table = primitive_create_table("parquet_server", table);

    format!(
        r#"
        {create_foreign_data_wrapper};
        {create_server};       
        {create_user_mapping_options} OPTIONS (type 'S3', region 'us-east-1', endpoint '{s3_endpoint}', use_ssl 'false', url_style 'path');
        {create_table} OPTIONS (files '{s3_object_path}'); 
    "#
    )
}

pub fn primitive_setup_fdw_s3_delta(
    s3_endpoint: &str,
    s3_object_path: &str,
    table: &str,
) -> String {
    let create_foreign_data_wrapper = primitive_create_foreign_data_wrapper(
        "delta_wrapper",
        "delta_fdw_handler",
        "delta_fdw_validator",
    );
    let create_user_mapping_options =
        primitive_create_user_mapping_options("public", "delta_server");
    let create_server = primitive_create_server("delta_server", "delta_wrapper");
    let create_table = primitive_create_delta_table("delta_server", table);

    format!(
        r#"
        {create_foreign_data_wrapper};
        {create_server};   
        {create_user_mapping_options} OPTIONS (type 'S3', region 'us-east-1', endpoint '{s3_endpoint}', use_ssl 'false', url_style 'path');   
        {create_table} OPTIONS (files '{s3_object_path}'); 
    "#
    )
}

pub fn primitive_setup_fdw_local_file_listing(local_file_path: &str, table: &str) -> String {
    setup_fdw_local_parquet_file_listing(local_file_path, table, &primitive_table_columns())
}

pub fn primitive_setup_fdw_local_file_delta(local_file_path: &str, table: &str) -> String {
    let create_foreign_data_wrapper = primitive_create_foreign_data_wrapper(
        "delta_wrapper",
        "delta_fdw_handler",
        "delta_fdw_validator",
    );
    let create_server = primitive_create_server("delta_server", "delta_wrapper");
    let create_table = primitive_create_delta_table("delta_server", table);

    format!(
        r#"
        {create_foreign_data_wrapper};
        {create_server};
        {create_table} OPTIONS (files '{local_file_path}'); 
    "#
    )
}

pub fn setup_local_file_listing_with_casing(local_file_path: &str, table: &str) -> String {
    let create_foreign_data_wrapper = primitive_create_foreign_data_wrapper(
        "parquet_wrapper",
        "parquet_fdw_handler",
        "parquet_fdw_validator",
    );
    let create_server = primitive_create_server("parquet_server", "parquet_wrapper");
    let create_table = auto_create_table("parquet_server", table);

    format!(
        r#"
        {create_foreign_data_wrapper};
        {create_server};
        {create_table} OPTIONS (files '{local_file_path}', preserve_casing 'true'); 
    "#
    )
}

fn valid(data_type: &DataType, oid: u32) -> bool {
    let oid = match PgBuiltInOids::from_u32(oid) {
        Ok(oid) => oid,
        _ => return false,
    };
    match data_type {
        DataType::Null => false,
        DataType::Boolean => matches!(oid, PgBuiltInOids::BOOLOID),
        DataType::Int8 => matches!(oid, PgBuiltInOids::INT2OID),
        DataType::Int16 => matches!(oid, PgBuiltInOids::INT2OID),
        DataType::Int32 => matches!(oid, PgBuiltInOids::INT4OID),
        DataType::Int64 => matches!(oid, PgBuiltInOids::INT8OID),
        DataType::UInt8 => matches!(oid, PgBuiltInOids::INT2OID),
        DataType::UInt16 => matches!(oid, PgBuiltInOids::INT4OID),
        DataType::UInt32 => matches!(oid, PgBuiltInOids::INT8OID),
        DataType::UInt64 => matches!(oid, PgBuiltInOids::NUMERICOID),
        DataType::Float16 => false, // Not supported yet.
        DataType::Float32 => matches!(oid, PgBuiltInOids::FLOAT4OID),
        DataType::Float64 => matches!(oid, PgBuiltInOids::FLOAT8OID),
        DataType::Timestamp(_, _) => matches!(oid, PgBuiltInOids::TIMESTAMPOID),
        DataType::Date32 => matches!(oid, PgBuiltInOids::DATEOID),
        DataType::Date64 => matches!(oid, PgBuiltInOids::DATEOID),
        DataType::Time32(_) => matches!(oid, PgBuiltInOids::TIMEOID),
        DataType::Time64(_) => matches!(oid, PgBuiltInOids::TIMEOID),
        DataType::Duration(_) => false, // Not supported yet.
        DataType::Interval(_) => false, // Not supported yet.
        DataType::Binary => matches!(oid, PgBuiltInOids::BYTEAOID),
        DataType::FixedSizeBinary(_) => false, // Not supported yet.
        DataType::LargeBinary => matches!(oid, PgBuiltInOids::BYTEAOID),
        DataType::BinaryView => matches!(oid, PgBuiltInOids::BYTEAOID),
        DataType::Utf8 => matches!(oid, PgBuiltInOids::TEXTOID),
        DataType::LargeUtf8 => matches!(oid, PgBuiltInOids::TEXTOID),
        // Remaining types are not supported yet.
        DataType::Utf8View => false,
        DataType::List(_) => false,
        DataType::ListView(_) => false,
        DataType::FixedSizeList(_, _) => false,
        DataType::LargeList(_) => false,
        DataType::LargeListView(_) => false,
        DataType::Struct(_) => false,
        DataType::Union(_, _) => false,
        DataType::Dictionary(_, _) => false,
        DataType::Decimal128(_, _) => false,
        DataType::Decimal256(_, _) => false,
        DataType::Map(_, _) => false,
        DataType::RunEndEncoded(_, _) => false,
    }
}

fn decode<'r, T: sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>>(
    field: &Field,
    row: &'r PgRow,
) -> Result<T> {
    let field_name = field.name();
    let field_type = field.data_type();

    let col = row.try_get_raw(field.name().as_str())?;
    let info = col.type_info();
    let oid = info.oid().map(|o| o.0).unwrap_or(InvalidOid.into());
    if !valid(field_type, oid) {
        bail!(
            "field '{}' has arrow type '{}', which cannot be read from postgres type '{}'",
            field.name(),
            field.data_type(),
            info.name()
        )
    }

    Ok(row.try_get(field_name.as_str())?)
}

pub fn schema_to_batch(schema: &SchemaRef, rows: &[PgRow]) -> Result<RecordBatch> {
    let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let arrays = schema
        .fields()
        .into_iter()
        .map(|field| {
            Ok(match field.data_type() {
                DataType::Boolean => Arc::new(BooleanArray::from(
                    rows.iter()
                        .map(|row| decode::<Option<bool>>(field, row))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::Int8 => Arc::new(Int8Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<i16>>(field, row))
                        .map(|row| row.map(|o| o.map(|n| n as i8)))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::Int16 => Arc::new(Int16Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<i16>>(field, row))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::Int32 => Arc::new(Int32Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<i32>>(field, row))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::Int64 => Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<i64>>(field, row))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::UInt8 => Arc::new(UInt8Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<i16>>(field, row))
                        .map(|row| row.map(|o| o.map(|n| n as u8)))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::UInt16 => Arc::new(UInt16Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<i32>>(field, row))
                        .map(|row| row.map(|o| o.map(|n| n as u16)))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::UInt32 => Arc::new(UInt32Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<i64>>(field, row))
                        .map(|row| row.map(|o| o.map(|n| n as u32)))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::UInt64 => Arc::new(UInt64Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<BigDecimal>>(field, row))
                        .map(|row| row.map(|o| o.and_then(|n| n.to_u64())))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::Float32 => Arc::new(Float32Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<f32>>(field, row))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::Float64 => Arc::new(Float64Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<f64>>(field, row))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::Timestamp(unit, _) => match unit {
                    TimeUnit::Second => Arc::new(TimestampSecondArray::from(
                        rows.iter()
                            .map(|row| decode::<Option<NaiveDateTime>>(field, row))
                            .map(|row| row.map(|o| o.map(|n| n.and_utc().timestamp())))
                            .collect::<Result<Vec<_>>>()?,
                    )) as ArrayRef,
                    TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(
                        rows.iter()
                            .map(|row| decode::<Option<NaiveDateTime>>(field, row))
                            .map(|row| row.map(|o| o.map(|n| n.and_utc().timestamp_millis())))
                            .collect::<Result<Vec<_>>>()?,
                    )) as ArrayRef,
                    TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(
                        rows.iter()
                            .map(|row| decode::<Option<NaiveDateTime>>(field, row))
                            .map(|row| row.map(|o| o.map(|n| n.and_utc().timestamp_micros())))
                            .collect::<Result<Vec<_>>>()?,
                    )) as ArrayRef,
                    TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(
                        rows.iter()
                            .map(|row| decode::<Option<NaiveDateTime>>(field, row))
                            .map(|row| {
                                row.map(|o| o.and_then(|n| n.and_utc().timestamp_nanos_opt()))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )) as ArrayRef,
                },
                DataType::Date32 => Arc::new(Date32Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<NaiveDate>>(field, row))
                        .map(|row| {
                            row.map(|o| {
                                o.map(|n| n.signed_duration_since(unix_epoch).num_days() as i32)
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::Date64 => Arc::new(Date64Array::from(
                    rows.iter()
                        .map(|row| decode::<Option<NaiveDate>>(field, row))
                        .map(|row| {
                            row.map(|o| {
                                o.map(|n| n.signed_duration_since(unix_epoch).num_milliseconds())
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::Time32(unit) => match unit {
                    TimeUnit::Second => Arc::new(Time32SecondArray::from(
                        rows.iter()
                            .map(|row| decode::<Option<NaiveTime>>(field, row))
                            .map(|row| row.map(|o| o.map(|n| n.num_seconds_from_midnight() as i32)))
                            .collect::<Result<Vec<_>>>()?,
                    )) as ArrayRef,
                    TimeUnit::Millisecond => Arc::new(Time32MillisecondArray::from(
                        rows.iter()
                            .map(|row| decode::<Option<NaiveTime>>(field, row))
                            .map(|row| {
                                row.map(|o| {
                                    o.map(|n| {
                                        (n.num_seconds_from_midnight() * 1000
                                            + (n.nanosecond() / 1_000_000))
                                            as i32
                                    })
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )) as ArrayRef,
                    TimeUnit::Microsecond => bail!("arrow time32 does not support microseconds"),
                    TimeUnit::Nanosecond => bail!("arrow time32 does not support nanoseconds"),
                },
                DataType::Time64(unit) => match unit {
                    TimeUnit::Second => bail!("arrow time64i does not support seconds"),
                    TimeUnit::Millisecond => bail!("arrow time64 does not support millseconds"),
                    TimeUnit::Microsecond => Arc::new(Time64MicrosecondArray::from(
                        rows.iter()
                            .map(|row| decode::<Option<NaiveTime>>(field, row))
                            .map(|row| {
                                row.map(|o| {
                                    o.map(|n| {
                                        (n.num_seconds_from_midnight() * 1_000_000
                                            + (n.nanosecond() / 1_000))
                                            as i64
                                    })
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )) as ArrayRef,
                    TimeUnit::Nanosecond => Arc::new(Time64NanosecondArray::from(
                        rows.iter()
                            .map(|row| decode::<Option<NaiveTime>>(field, row))
                            .map(|row| {
                                row.map(|o| {
                                    o.map(|n| {
                                        (n.num_seconds_from_midnight() as u64 * 1_000_000_000
                                            + (n.nanosecond() as u64))
                                            .try_into()
                                            .ok()
                                            .unwrap_or(i64::MAX)
                                    })
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )) as ArrayRef,
                },
                DataType::Binary => Arc::new(BinaryArray::from(
                    rows.iter()
                        .map(|row| decode::<Option<&[u8]>>(field, row))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::LargeBinary => Arc::new(LargeBinaryArray::from(
                    rows.iter()
                        .map(|row| decode::<Option<&[u8]>>(field, row))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::Utf8 => Arc::new(StringArray::from(
                    rows.iter()
                        .map(|row| decode::<Option<&str>>(field, row))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                DataType::LargeUtf8 => Arc::new(LargeStringArray::from(
                    rows.iter()
                        .map(|row| decode::<Option<&str>>(field, row))
                        .collect::<Result<Vec<_>>>()?,
                )) as ArrayRef,
                _ => bail!("cannot read into arrow type '{}'", field.data_type()),
            })
        })
        .collect::<Result<Vec<ArrayRef>>>()?;

    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}
