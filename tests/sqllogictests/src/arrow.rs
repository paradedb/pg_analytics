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

// TECH DEBT: This file is a copy of the `db.rs` file from https://github.com/paradedb/paradedb/blob/dev/shared/src/fixtures/db.rs
// We duplicated because the paradedb repo may use a different version of pgrx than pg_analytics, but eventually we should
// move this into a separate crate without any dependencies on pgrx.

use std::sync::Arc;

use anyhow::{bail, Result};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use pgrx::pg_sys::InvalidOid;
use pgrx::PgBuiltInOids;
use sqlx::postgres::PgRow;
use sqlx::{Postgres, Row, TypeInfo, ValueRef};

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
                    TimeUnit::Millisecond => bail!("arrow time64 does not support milliseconds"),
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
