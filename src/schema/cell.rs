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

use anyhow::{anyhow, bail, Result};
use duckdb::arrow::array::types::{
    ArrowTemporalType, Date32Type, Date64Type, Decimal128Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalYearMonthType, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use duckdb::arrow::array::{
    timezone::Tz, Array, ArrayAccessor, ArrayRef, ArrowPrimitiveType, AsArray, BinaryArray,
    BooleanArray, Decimal128Array, Float16Array, Float32Array, Float64Array, GenericByteArray,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, StringArray,
};
use duckdb::arrow::datatypes::{DataType, DecimalType, GenericStringType, IntervalUnit, TimeUnit};
use pgrx::*;
use serde_json::{value::Number, Map, Value};
use std::any::type_name;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use supabase_wrappers::interface::Cell;

use super::datetime::*;

type LargeStringArray = GenericByteArray<GenericStringType<i64>>;

pub trait GetBinaryValue
where
    Self: Array + AsArray,
{
    fn get_binary_value<A>(&self, index: usize) -> Result<Option<String>>
    where
        A: Array + Debug + 'static,
        for<'a> &'a A: ArrayAccessor,
        for<'a> <&'a A as ArrayAccessor>::Item: AsRef<[u8]>,
    {
        let downcast_array = self
            .as_any()
            .downcast_ref::<A>()
            .ok_or_else(|| anyhow!("failed to downcast binary array"))?;

        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                let value = downcast_array.value(index);
                let bytes: &[u8] = value.as_ref();
                let rust_bytes = varlena::rust_byte_slice_to_bytea(bytes);
                let rust_str = unsafe { varlena::text_to_rust_str_unchecked(rust_bytes.into_pg()) };
                Ok(Some(rust_str.to_string()))
            }
            true => Ok(None),
        }
    }
}

pub trait GetByteValue
where
    Self: Array + AsArray,
{
    fn get_byte_value<A>(&self, index: usize) -> Result<Option<PgBox<pg_sys::varlena>>>
    where
        A: Array + Debug + 'static,
        for<'a> &'a A: ArrayAccessor,
        for<'a> <&'a A as ArrayAccessor>::Item: AsRef<[u8]>,
    {
        let downcast_array = self
            .as_any()
            .downcast_ref::<A>()
            .ok_or_else(|| anyhow!("failed to downcast byte array"))?;

        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                let value = downcast_array.value(index);
                let bytes: &[u8] = value.as_ref();
                Ok(Some(varlena::rust_byte_slice_to_bytea(bytes)))
            }
            true => Ok(None),
        }
    }
}

pub trait GetDateValue
where
    Self: Array + AsArray,
{
    fn get_date_value<N, T>(&self, index: usize) -> Result<Option<datum::Date>>
    where
        N: std::marker::Send + std::marker::Sync,
        i64: From<N>,
        T: ArrowPrimitiveType<Native = N> + ArrowTemporalType,
    {
        let downcast_array = self.as_primitive::<T>();

        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                let date = downcast_array
                    .value_as_date(index)
                    .ok_or_else(|| anyhow!("failed to convert date to NaiveDate"))?;

                Ok(Some(datum::Date::try_from(Date(date))?))
            }
            true => Ok(None),
        }
    }
}

pub trait GetPrimitiveValue
where
    Self: Array + AsArray,
{
    fn get_primitive_value<A>(&self, index: usize) -> Result<Option<<&A as ArrayAccessor>::Item>>
    where
        A: Array + Debug + 'static,
        for<'a> &'a A: ArrayAccessor,
    {
        let downcast_array = self
            .as_any()
            .downcast_ref::<A>()
            .ok_or_else(|| anyhow!("failed to downcast primitive array {:?}", type_name::<A>()))?;
        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => Ok(Some(downcast_array.value(index))),
            true => Ok(None),
        }
    }
}

pub trait GetPrimitiveListValue
where
    Self: Array + AsArray,
{
    fn get_primitive_list_value<A, T>(&self, index: usize) -> Result<Option<Vec<T>>>
    where
        A: Array + Debug + 'static,
        for<'a> &'a A: IntoIterator,
        for<'a> <&'a A as IntoIterator>::Item: IntoDatum + Clone,
        for<'a> Vec<T>: FromIterator<<&'a A as IntoIterator>::Item>,
    {
        let downcast_array = self.as_list::<i32>();

        if downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            return Ok(None);
        }

        let binding = downcast_array.value(index);
        let value = binding
            .as_any()
            .downcast_ref::<A>()
            .ok_or_else(|| anyhow!("failed to downcast list array"))?;

        Ok(Some(value.into_iter().collect::<Vec<T>>()))
    }
}

pub trait GetStringListValue
where
    Self: Array + AsArray,
{
    fn get_string_list_value(&self, index: usize) -> Result<Option<Vec<Option<String>>>> {
        let downcast_array = self.as_list::<i32>();

        if downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            return Ok(None);
        }

        let binding = downcast_array.value(index);
        let value = binding
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("failed to downcast list array"))?;

        Ok(Some(
            value
                .iter()
                .map(|opt| opt.map(|s| s.to_string()))
                .collect::<Vec<Option<String>>>(),
        ))
    }
}

pub trait GetStructValue
where
    Self: Array + AsArray,
{
    fn get_struct_value(&self, index: usize) -> Result<Option<datum::JsonB>> {
        let downcast_array = self.as_struct();

        if downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            return Ok(None);
        }

        let column_names = downcast_array.column_names();
        let fields = downcast_array.fields();
        let mut map = Map::new();

        for column_name in column_names {
            if let Some((column_index, field)) = fields.find(column_name) {
                match field.data_type() {
                    DataType::Boolean => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_primitive_value::<BooleanArray>(index)? {
                            map.insert(column_name.to_string(), Value::Bool(value));
                        }
                    }
                    DataType::Int8 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_primitive_value::<Int8Array>(index)? {
                            map.insert(column_name.to_string(), Value::Number(Number::from(value)));
                        }
                    }
                    DataType::Int16 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_primitive_value::<Int16Array>(index)? {
                            map.insert(column_name.to_string(), Value::Number(Number::from(value)));
                        }
                    }
                    DataType::Int32 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_primitive_value::<Int32Array>(index)? {
                            map.insert(column_name.to_string(), Value::Number(Number::from(value)));
                        }
                    }
                    DataType::Int64 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_primitive_value::<Int64Array>(index)? {
                            map.insert(column_name.to_string(), Value::Number(Number::from(value)));
                        }
                    }
                    DataType::UInt8 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_uint_value::<UInt8Type>(index)? {
                            map.insert(column_name.to_string(), Value::Number(Number::from(value)));
                        }
                    }
                    DataType::UInt16 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_uint_value::<UInt16Type>(index)? {
                            map.insert(column_name.to_string(), Value::Number(Number::from(value)));
                        }
                    }
                    DataType::UInt32 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_uint_value::<UInt32Type>(index)? {
                            map.insert(column_name.to_string(), Value::Number(Number::from(value)));
                        }
                    }
                    DataType::UInt64 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_uint_value::<UInt64Type>(index)? {
                            map.insert(column_name.to_string(), Value::Number(Number::from(value)));
                        }
                    }
                    DataType::Float16 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_primitive_value::<Float16Array>(index)? {
                            map.insert(
                                column_name.to_string(),
                                Value::Number(Number::from_f64(value.to_f32() as f64).ok_or_else(
                                    || anyhow!("failed to convert {:?} to f64", value),
                                )?),
                            );
                        }
                    }
                    DataType::Float32 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_primitive_value::<Float32Array>(index)? {
                            map.insert(
                                column_name.to_string(),
                                Value::Number(Number::from_f64(value as f64).ok_or_else(|| {
                                    anyhow!("failed to convert {:?} to f64", value)
                                })?),
                            );
                        }
                    }
                    DataType::Float64 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_primitive_value::<Float64Array>(index)? {
                            map.insert(
                                column_name.to_string(),
                                Value::Number(Number::from_f64(value).ok_or_else(|| {
                                    anyhow!("failed to convert {:?} to f64", value)
                                })?),
                            );
                        }
                    }
                    DataType::Decimal128(p, s) => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_decimal_value::<f64>(index, *p, *s)? {
                            map.insert(
                                column_name.to_string(),
                                Value::Number(Number::from_f64(value).ok_or_else(|| {
                                    anyhow!("failed to convert {:?} to f64", value)
                                })?),
                            );
                        }
                    }
                    DataType::Utf8 => {
                        let column = downcast_array.column(column_index);
                        if let Some(value) = column.get_primitive_value::<StringArray>(index)? {
                            map.insert(column_name.to_string(), Value::String(value.to_string()));
                        }
                    }
                    unsupported => bail!(
                        "Structs with {:?} field types are not yet supported",
                        unsupported
                    ),
                }
            }
        }

        Ok(Some(datum::JsonB(Value::Object(map))))
    }
}

pub trait GetDecimalValue
where
    Self: Array + AsArray,
{
    fn get_decimal_value<N>(&self, index: usize, precision: u8, scale: i8) -> Result<Option<N>>
    where
        N: std::marker::Send + std::marker::Sync + TryFrom<AnyNumeric>,
        <N as TryFrom<pgrx::AnyNumeric>>::Error: Sync + Send + std::error::Error + 'static,
    {
        let downcast_array = self
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| anyhow!("failed to downcast Decimal128 array"))?;
        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                let value = downcast_array.value(index);
                let numeric =
                    AnyNumeric::from_str(&Decimal128Type::format_decimal(value, precision, scale))?;
                Ok(Some(N::try_from(numeric)?))
            }
            true => Ok(None),
        }
    }
}

pub trait GetIntervalDayTimeValue
where
    Self: Array + AsArray,
{
    fn get_interval_day_time_value(&self, index: usize) -> Result<Option<datum::Interval>> {
        let downcast_array = self.as_primitive::<IntervalDayTimeType>();

        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                const MICROSECONDS_IN_MILLISECOND: i32 = 1_000;
                let interval = downcast_array.value(index);

                Ok(Some(datum::Interval::new(
                    0,
                    interval.days,
                    (interval.milliseconds * MICROSECONDS_IN_MILLISECOND) as i64,
                )?))
            }
            true => Ok(None),
        }
    }
}

pub trait GetIntervalMonthDayNanoValue
where
    Self: Array + AsArray,
{
    fn get_interval_month_day_nano_value(&self, index: usize) -> Result<Option<datum::Interval>> {
        let downcast_array = self.as_primitive::<IntervalMonthDayNanoType>();

        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                const NANOSECONDS_IN_MICROSECOND: i64 = 1_000;
                let interval = downcast_array.value(index);

                Ok(Some(datum::Interval::new(
                    interval.months,
                    interval.days,
                    interval.nanoseconds / NANOSECONDS_IN_MICROSECOND,
                )?))
            }
            true => Ok(None),
        }
    }
}

pub trait GetIntervalYearMonthValue
where
    Self: Array + AsArray,
{
    fn get_interval_year_month_value(&self, index: usize) -> Result<Option<datum::Interval>> {
        let downcast_array = self.as_primitive::<IntervalYearMonthType>();

        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                let months = downcast_array.value(index);
                Ok(Some(datum::Interval::from_months(months)))
            }
            true => Ok(None),
        }
    }
}

pub trait GetTimeValue
where
    Self: Array + AsArray,
{
    fn get_time_value<N, T>(&self, index: usize) -> Result<Option<datum::Time>>
    where
        N: std::marker::Send + std::marker::Sync,
        i64: From<N>,
        T: ArrowPrimitiveType<Native = N> + ArrowTemporalType,
    {
        let downcast_array = self.as_primitive::<T>();

        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                let time = downcast_array
                    .value_as_time(index)
                    .ok_or_else(|| anyhow!("failed to convert timestamp to NaiveDateTime"))?;

                Ok(Some(datum::Time::try_from(Time(time))?))
            }
            true => Ok(None),
        }
    }
}

pub trait GetTimestampValue
where
    Self: Array + AsArray,
{
    fn get_timestamp_value<T>(&self, index: usize) -> Result<Option<datum::Timestamp>>
    where
        T: ArrowPrimitiveType<Native = i64> + ArrowTemporalType,
    {
        let downcast_array = self.as_primitive::<T>();

        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                let datetime = downcast_array
                    .value_as_datetime(index)
                    .ok_or_else(|| anyhow!("failed to convert timestamp to NaiveDateTime"))?;

                Ok(Some(datum::Timestamp::try_from(DateTimeNoTz(datetime))?))
            }
            true => Ok(None),
        }
    }
}

pub trait GetTimestampTzValue
where
    Self: Array + AsArray,
{
    fn get_timestamptz_value<T>(
        &self,
        index: usize,
        tz: Option<Arc<str>>,
    ) -> Result<Option<datum::TimestampWithTimeZone>>
    where
        T: ArrowPrimitiveType<Native = i64> + ArrowTemporalType,
    {
        let downcast_array = self.as_primitive::<T>();
        if downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            return Ok(None);
        }

        match tz {
            Some(tz) => {
                let datetime = downcast_array
                    .value_as_datetime_with_tz(index, Tz::from_str(&tz)?)
                    .ok_or_else(|| anyhow!("failed to convert timestamp to NaiveDateTime"))?;

                Ok(Some(datum::TimestampWithTimeZone::try_from(
                    DateTimeTz::new(datetime, &tz),
                )?))
            }
            None => {
                let datetime = downcast_array
                    .value_as_datetime(index)
                    .ok_or_else(|| anyhow!("failed to convert timestamp to NaiveDateTime"))?;

                Ok(Some(datum::TimestampWithTimeZone::try_from(DateTimeNoTz(
                    datetime,
                ))?))
            }
        }
    }
}

pub trait GetUIntValue
where
    Self: Array + AsArray,
{
    fn get_uint_value<A>(&self, index: usize) -> Result<Option<u64>>
    where
        A: ArrowPrimitiveType,
        u64: TryFrom<A::Native>,
        <u64 as TryFrom<<A as duckdb::arrow::array::ArrowPrimitiveType>::Native>>::Error:
            Send + Sync + std::error::Error,
    {
        let downcast_array = self.as_primitive::<A>();
        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                let value: A::Native = downcast_array.value(index);
                Ok(Some(u64::try_from(value)?))
            }
            true => Ok(None),
        }
    }
}

pub trait GetUuidValue
where
    Self: Array + AsArray,
{
    fn get_uuid_value(&self, index: usize) -> Result<Option<datum::Uuid>> {
        let downcast_array = self
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("failed to downcast uuid array"))?;

        match downcast_array.nulls().is_some() && downcast_array.is_null(index) {
            false => {
                let value = downcast_array.value(index);
                let uuid = uuid::Uuid::parse_str(value)?;
                Ok(Some(
                    datum::Uuid::from_slice(uuid.as_bytes()).map_err(|err| anyhow!(err))?,
                ))
            }
            true => Ok(None),
        }
    }
}

pub trait GetCell
where
    Self: Array
        + AsArray
        + GetBinaryValue
        + GetByteValue
        + GetDateValue
        + GetDecimalValue
        + GetIntervalDayTimeValue
        + GetIntervalMonthDayNanoValue
        + GetIntervalYearMonthValue
        + GetPrimitiveValue
        + GetPrimitiveListValue
        + GetStringListValue
        + GetStructValue
        + GetTimeValue
        + GetTimestampValue
        + GetTimestampTzValue
        + GetUIntValue
        + GetUuidValue,
{
    fn get_cell(&self, index: usize, oid: pg_sys::Oid, name: &str) -> Result<Option<Cell>> {
        match oid {
            pg_sys::BOOLOID => match self.get_primitive_value::<BooleanArray>(index)? {
                Some(value) => Ok(Some(Cell::Bool(value))),
                None => Ok(None),
            },
            pg_sys::BYTEAOID => match self.data_type() {
                DataType::Binary => match self.get_byte_value::<BinaryArray>(index)? {
                    Some(value) => Ok(Some(Cell::Bytea(value.into_pg()))),
                    None => Ok(None),
                },
                DataType::LargeBinary => match self.get_byte_value::<LargeBinaryArray>(index)? {
                    Some(value) => Ok(Some(Cell::Bytea(value.into_pg()))),
                    None => Ok(None),
                },
                DataType::Utf8 => match self.get_primitive_value::<StringArray>(index)? {
                    Some(value) => Ok(Some(Cell::Bytea(
                        varlena::rust_str_to_text_p(value).into_pg(),
                    ))),
                    None => Ok(None),
                },
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::INT2OID => match self.data_type() {
                DataType::Int8 => match self.get_primitive_value::<Int8Array>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value as i16))),
                    None => Ok(None),
                },
                DataType::Int16 => match self.get_primitive_value::<Int16Array>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value))),
                    None => Ok(None),
                },
                DataType::Int32 => match self.get_primitive_value::<Int32Array>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value as i16))),
                    None => Ok(None),
                },
                DataType::Int64 => match self.get_uint_value::<UInt8Type>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value as i16))),
                    None => Ok(None),
                },
                DataType::UInt8 => match self.get_uint_value::<UInt8Type>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value as i16))),
                    None => Ok(None),
                },
                DataType::UInt16 => match self.get_uint_value::<UInt16Type>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value as i16))),
                    None => Ok(None),
                },
                DataType::UInt32 => match self.get_uint_value::<UInt32Type>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value as i16))),
                    None => Ok(None),
                },
                DataType::UInt64 => match self.get_uint_value::<UInt64Type>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value as i16))),
                    None => Ok(None),
                },
                DataType::Float16 => match self.get_primitive_value::<Float16Array>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value.to_f32() as i16))),
                    None => Ok(None),
                },
                DataType::Float32 => match self.get_primitive_value::<Float32Array>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value as i16))),
                    None => Ok(None),
                },
                DataType::Float64 => match self.get_primitive_value::<Float64Array>(index)? {
                    Some(value) => Ok(Some(Cell::I16(value as i16))),
                    None => Ok(None),
                },
                DataType::Decimal128(p, s) => {
                    match self.get_decimal_value::<i16>(index, *p, *s)? {
                        Some(value) => Ok(Some(Cell::I16(value))),
                        None => Ok(None),
                    }
                }
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::INT4OID => match self.data_type() {
                DataType::Int8 => match self.get_primitive_value::<Int8Array>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value as i32))),
                    None => Ok(None),
                },
                DataType::Int16 => match self.get_primitive_value::<Int16Array>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value as i32))),
                    None => Ok(None),
                },
                DataType::Int32 => match self.get_primitive_value::<Int32Array>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value))),
                    None => Ok(None),
                },
                DataType::Int64 => match self.get_primitive_value::<Int64Array>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value as i32))),
                    None => Ok(None),
                },
                DataType::UInt8 => match self.get_uint_value::<UInt8Type>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value as i32))),
                    None => Ok(None),
                },
                DataType::UInt16 => match self.get_uint_value::<UInt16Type>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value as i32))),
                    None => Ok(None),
                },
                DataType::UInt32 => match self.get_uint_value::<UInt32Type>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value as i32))),
                    None => Ok(None),
                },
                DataType::UInt64 => match self.get_uint_value::<UInt64Type>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value as i32))),
                    None => Ok(None),
                },
                DataType::Float16 => match self.get_primitive_value::<Float16Array>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value.to_f32() as i32))),
                    None => Ok(None),
                },
                DataType::Float32 => match self.get_primitive_value::<Float32Array>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value as i32))),
                    None => Ok(None),
                },
                DataType::Float64 => match self.get_primitive_value::<Float64Array>(index)? {
                    Some(value) => Ok(Some(Cell::I32(value as i32))),
                    None => Ok(None),
                },
                DataType::Decimal128(p, s) => {
                    match self.get_decimal_value::<i32>(index, *p, *s)? {
                        Some(value) => Ok(Some(Cell::I32(value))),
                        None => Ok(None),
                    }
                }
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::INT8OID => match self.data_type() {
                DataType::Int8 => match self.get_primitive_value::<Int8Array>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value as i64))),
                    None => Ok(None),
                },
                DataType::Int16 => match self.get_primitive_value::<Int16Array>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value as i64))),
                    None => Ok(None),
                },
                DataType::Int32 => match self.get_primitive_value::<Int32Array>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value as i64))),
                    None => Ok(None),
                },
                DataType::Int64 => match self.get_primitive_value::<Int64Array>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value))),
                    None => Ok(None),
                },
                DataType::UInt8 => match self.get_uint_value::<UInt8Type>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value as i64))),
                    None => Ok(None),
                },
                DataType::UInt16 => match self.get_uint_value::<UInt16Type>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value as i64))),
                    None => Ok(None),
                },
                DataType::UInt32 => match self.get_uint_value::<UInt32Type>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value as i64))),
                    None => Ok(None),
                },
                DataType::UInt64 => match self.get_uint_value::<UInt64Type>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value as i64))),
                    None => Ok(None),
                },
                DataType::Float16 => match self.get_primitive_value::<Float16Array>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value.to_f32() as i64))),
                    None => Ok(None),
                },
                DataType::Float32 => match self.get_primitive_value::<Float32Array>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value as i64))),
                    None => Ok(None),
                },
                DataType::Float64 => match self.get_primitive_value::<Float64Array>(index)? {
                    Some(value) => Ok(Some(Cell::I64(value as i64))),
                    None => Ok(None),
                },
                DataType::Decimal128(p, s) => {
                    match self.get_decimal_value::<i64>(index, *p, *s)? {
                        Some(value) => Ok(Some(Cell::I64(value))),
                        None => Ok(None),
                    }
                }
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::FLOAT4OID => match self.data_type() {
                DataType::Int8 => match self.get_primitive_value::<Int8Array>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value as f32))),
                    None => Ok(None),
                },
                DataType::Int16 => match self.get_primitive_value::<Int16Array>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value as f32))),
                    None => Ok(None),
                },
                DataType::Int32 => match self.get_primitive_value::<Int32Array>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value as f32))),
                    None => Ok(None),
                },
                DataType::Int64 => match self.get_primitive_value::<Int64Array>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value as f32))),
                    None => Ok(None),
                },
                DataType::UInt8 => match self.get_uint_value::<UInt8Type>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value as f32))),
                    None => Ok(None),
                },
                DataType::UInt16 => match self.get_uint_value::<UInt16Type>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value as f32))),
                    None => Ok(None),
                },
                DataType::UInt32 => match self.get_uint_value::<UInt32Type>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value as f32))),
                    None => Ok(None),
                },
                DataType::UInt64 => match self.get_uint_value::<UInt64Type>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value as f32))),
                    None => Ok(None),
                },
                DataType::Float16 => match self.get_primitive_value::<Float16Array>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value.to_f32()))),
                    None => Ok(None),
                },
                DataType::Float32 => match self.get_primitive_value::<Float32Array>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value))),
                    None => Ok(None),
                },
                DataType::Float64 => match self.get_primitive_value::<Float64Array>(index)? {
                    Some(value) => Ok(Some(Cell::F32(value as f32))),
                    None => Ok(None),
                },
                DataType::Decimal128(p, s) => {
                    match self.get_decimal_value::<f32>(index, *p, *s)? {
                        Some(value) => Ok(Some(Cell::F32(value))),
                        None => Ok(None),
                    }
                }
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::FLOAT8OID => match self.data_type() {
                DataType::Int8 => match self.get_primitive_value::<Int8Array>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value as f64))),
                    None => Ok(None),
                },
                DataType::Int16 => match self.get_primitive_value::<Int16Array>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value as f64))),
                    None => Ok(None),
                },
                DataType::Int32 => match self.get_primitive_value::<Int32Array>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value as f64))),
                    None => Ok(None),
                },
                DataType::Int64 => match self.get_primitive_value::<Int64Array>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value as f64))),
                    None => Ok(None),
                },
                DataType::UInt8 => match self.get_uint_value::<UInt8Type>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value as f64))),
                    None => Ok(None),
                },
                DataType::UInt16 => match self.get_uint_value::<UInt16Type>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value as f64))),
                    None => Ok(None),
                },
                DataType::UInt32 => match self.get_uint_value::<UInt32Type>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value as f64))),
                    None => Ok(None),
                },
                DataType::UInt64 => match self.get_uint_value::<UInt64Type>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value as f64))),
                    None => Ok(None),
                },
                DataType::Float16 => match self.get_primitive_value::<Float16Array>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value.to_f64()))),
                    None => Ok(None),
                },
                DataType::Float32 => match self.get_primitive_value::<Float32Array>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value as f64))),
                    None => Ok(None),
                },
                DataType::Float64 => match self.get_primitive_value::<Float64Array>(index)? {
                    Some(value) => Ok(Some(Cell::F64(value))),
                    None => Ok(None),
                },
                DataType::Decimal128(p, s) => {
                    match self.get_decimal_value::<f64>(index, *p, *s)? {
                        Some(value) => Ok(Some(Cell::F64(value))),
                        None => Ok(None),
                    }
                }
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::NUMERICOID => match self.data_type() {
                DataType::Int8 => match self.get_primitive_value::<Int8Array>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::from(value as i64)))),
                    None => Ok(None),
                },
                DataType::Int16 => match self.get_primitive_value::<Int16Array>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::from(value as i64)))),
                    None => Ok(None),
                },
                DataType::Int32 => match self.get_primitive_value::<Int32Array>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::from(value as i64)))),
                    None => Ok(None),
                },
                DataType::Int64 => match self.get_primitive_value::<Int64Array>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::from(value)))),
                    None => Ok(None),
                },
                DataType::UInt8 => match self.get_uint_value::<UInt8Type>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::from(value)))),
                    None => Ok(None),
                },
                DataType::UInt16 => match self.get_uint_value::<UInt16Type>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::from(value)))),
                    None => Ok(None),
                },
                DataType::UInt32 => match self.get_uint_value::<UInt32Type>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::from(value)))),
                    None => Ok(None),
                },
                DataType::UInt64 => match self.get_uint_value::<UInt64Type>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::from(value)))),
                    None => Ok(None),
                },
                DataType::Float16 => match self.get_primitive_value::<Float16Array>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::try_from(value.to_f32())?))),
                    None => Ok(None),
                },
                DataType::Float32 => match self.get_primitive_value::<Float32Array>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::try_from(value)?))),
                    None => Ok(None),
                },
                DataType::Float64 => match self.get_primitive_value::<Float64Array>(index)? {
                    Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::try_from(value)?))),
                    None => Ok(None),
                },
                DataType::Decimal128(p, s) => {
                    match self.get_primitive_value::<Decimal128Array>(index)? {
                        Some(value) => Ok(Some(Cell::Numeric(AnyNumeric::from_str(
                            &Decimal128Type::format_decimal(value, *p, *s),
                        )?))),
                        None => Ok(None),
                    }
                }
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID | pg_sys::NAMEOID => {
                match self.data_type() {
                    DataType::Utf8 => match self.get_primitive_value::<StringArray>(index)? {
                        Some(value) => Ok(Some(Cell::String(value.to_string()))),
                        None => Ok(None),
                    },
                    DataType::LargeUtf8 => {
                        match self.get_primitive_value::<LargeStringArray>(index)? {
                            Some(value) => Ok(Some(Cell::String(value.to_string()))),
                            None => Ok(None),
                        }
                    }
                    DataType::Binary => match self.get_binary_value::<BinaryArray>(index)? {
                        Some(value) => Ok(Some(Cell::String(value))),
                        None => Ok(None),
                    },
                    DataType::LargeBinary => {
                        match self.get_binary_value::<LargeBinaryArray>(index)? {
                            Some(value) => Ok(Some(Cell::String(value))),
                            None => Ok(None),
                        }
                    }
                    unsupported => Err(DataTypeError::DataTypeMismatch(
                        name.to_string(),
                        unsupported.clone(),
                        PgOid::from(oid),
                    )
                    .into()),
                }
            }
            pg_sys::DATEOID => match self.data_type() {
                DataType::Date32 => match self.get_date_value::<i32, Date32Type>(index)? {
                    Some(value) => Ok(Some(Cell::Date(value))),
                    None => Ok(None),
                },
                DataType::Date64 => match self.get_date_value::<i64, Date64Type>(index)? {
                    Some(value) => Ok(Some(Cell::Date(value))),
                    None => Ok(None),
                },
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::INTERVALOID => match self.data_type() {
                DataType::Interval(IntervalUnit::DayTime) => {
                    match self.get_interval_day_time_value(index)? {
                        Some(value) => Ok(Some(Cell::Interval(value))),
                        None => Ok(None),
                    }
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    match self.get_interval_month_day_nano_value(index)? {
                        Some(value) => Ok(Some(Cell::Interval(value))),
                        None => Ok(None),
                    }
                }
                DataType::Interval(IntervalUnit::YearMonth) => {
                    match self.get_interval_year_month_value(index)? {
                        Some(value) => Ok(Some(Cell::Interval(value))),
                        None => Ok(None),
                    }
                }
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::JSONBOID => match self.data_type() {
                DataType::Struct(_) => match self.get_struct_value(index)? {
                    Some(value) => Ok(Some(Cell::Json(value))),
                    None => Ok(None),
                },
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::TIMEOID => match self.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => {
                    match self.get_time_value::<i64, Time64NanosecondType>(index)? {
                        Some(value) => Ok(Some(Cell::Time(value))),
                        None => Ok(None),
                    }
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    match self.get_time_value::<i64, Time64MicrosecondType>(index)? {
                        Some(value) => Ok(Some(Cell::Time(value))),
                        None => Ok(None),
                    }
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    match self.get_time_value::<i32, Time32MillisecondType>(index)? {
                        Some(value) => Ok(Some(Cell::Time(value))),
                        None => Ok(None),
                    }
                }
                DataType::Time32(TimeUnit::Second) => {
                    match self.get_time_value::<i32, Time32SecondType>(index)? {
                        Some(value) => Ok(Some(Cell::Time(value))),
                        None => Ok(None),
                    }
                }
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::TIMESTAMPOID => match self.data_type() {
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    match self.get_timestamp_value::<TimestampNanosecondType>(index)? {
                        Some(value) => Ok(Some(Cell::Timestamp(value))),
                        None => Ok(None),
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    match self.get_timestamp_value::<TimestampMicrosecondType>(index)? {
                        Some(value) => Ok(Some(Cell::Timestamp(value))),
                        None => Ok(None),
                    }
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    match self.get_timestamp_value::<TimestampMillisecondType>(index)? {
                        Some(value) => Ok(Some(Cell::Timestamp(value))),
                        None => Ok(None),
                    }
                }
                DataType::Timestamp(TimeUnit::Second, _) => {
                    match self.get_timestamp_value::<TimestampSecondType>(index)? {
                        Some(value) => Ok(Some(Cell::Timestamp(value))),
                        None => Ok(None),
                    }
                }
                DataType::Date32 => match self.get_date_value::<i32, Date32Type>(index)? {
                    Some(value) => Ok(Some(Cell::Timestamp(value.into()))),
                    None => Ok(None),
                },
                DataType::Date64 => match self.get_date_value::<i64, Date64Type>(index)? {
                    Some(value) => Ok(Some(Cell::Timestamp(value.into()))),
                    None => Ok(None),
                },
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::TIMESTAMPTZOID => match self.data_type() {
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                    match self
                        .get_timestamptz_value::<TimestampNanosecondType>(index, tz.clone())?
                    {
                        Some(value) => Ok(Some(Cell::Timestamptz(value))),
                        None => Ok(None),
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                    match self
                        .get_timestamptz_value::<TimestampMicrosecondType>(index, tz.clone())?
                    {
                        Some(value) => Ok(Some(Cell::Timestamptz(value))),
                        None => Ok(None),
                    }
                }
                DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                    match self
                        .get_timestamptz_value::<TimestampMillisecondType>(index, tz.clone())?
                    {
                        Some(value) => Ok(Some(Cell::Timestamptz(value))),
                        None => Ok(None),
                    }
                }
                DataType::Timestamp(TimeUnit::Second, tz) => {
                    match self.get_timestamptz_value::<TimestampSecondType>(index, tz.clone())? {
                        Some(value) => Ok(Some(Cell::Timestamptz(value))),
                        None => Ok(None),
                    }
                }
                DataType::Date32 => match self.get_date_value::<i32, Date32Type>(index)? {
                    Some(value) => Ok(Some(Cell::Timestamptz(value.into()))),
                    None => Ok(None),
                },
                DataType::Date64 => match self.get_date_value::<i64, Date64Type>(index)? {
                    Some(value) => Ok(Some(Cell::Timestamptz(value.into()))),
                    None => Ok(None),
                },
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::VOIDOID => match self.data_type() {
                DataType::Null => Ok(None),
                unsupported => Err(DataTypeError::DataTypeMismatch(
                    name.to_string(),
                    unsupported.clone(),
                    PgOid::from(oid),
                )
                .into()),
            },
            pg_sys::UUIDOID => match self.get_uuid_value(index)? {
                Some(value) => Ok(Some(Cell::Uuid(value))),
                None => Ok(None),
            },
            pg_sys::BOOLARRAYOID => {
                match self.get_primitive_list_value::<BooleanArray, Option<bool>>(index)? {
                    Some(value) => Ok(Some(Cell::BoolArray(value))),
                    None => Ok(None),
                }
            }
            pg_sys::TEXTARRAYOID | pg_sys::VARCHARARRAYOID | pg_sys::BPCHARARRAYOID => {
                match self.get_string_list_value(index)? {
                    Some(value) => Ok(Some(Cell::StringArray(value))),
                    None => Ok(None),
                }
            }
            pg_sys::INT2ARRAYOID => {
                match self.get_primitive_list_value::<Int16Array, Option<i16>>(index)? {
                    Some(value) => Ok(Some(Cell::I16Array(value))),
                    None => Ok(None),
                }
            }
            pg_sys::INT4ARRAYOID => {
                match self.get_primitive_list_value::<Int32Array, Option<i32>>(index)? {
                    Some(value) => Ok(Some(Cell::I32Array(value))),
                    None => Ok(None),
                }
            }
            pg_sys::INT8ARRAYOID => {
                match self.get_primitive_list_value::<Int64Array, Option<i64>>(index)? {
                    Some(value) => Ok(Some(Cell::I64Array(value))),
                    None => Ok(None),
                }
            }
            pg_sys::FLOAT4ARRAYOID => {
                match self.get_primitive_list_value::<Float32Array, Option<f32>>(index)? {
                    Some(value) => Ok(Some(Cell::F32Array(value))),
                    None => Ok(None),
                }
            }
            pg_sys::FLOAT8ARRAYOID => {
                match self.get_primitive_list_value::<Float64Array, Option<f64>>(index)? {
                    Some(value) => Ok(Some(Cell::F64Array(value))),
                    None => Ok(None),
                }
            }
            unsupported => Err(DataTypeError::DataTypeMismatch(
                name.to_string(),
                self.data_type().clone(),
                PgOid::from(unsupported),
            )
            .into()),
        }
    }
}

impl GetBinaryValue for ArrayRef {}
impl GetByteValue for ArrayRef {}
impl GetCell for ArrayRef {}
impl GetDateValue for ArrayRef {}
impl GetDecimalValue for ArrayRef {}
impl GetIntervalDayTimeValue for ArrayRef {}
impl GetIntervalMonthDayNanoValue for ArrayRef {}
impl GetIntervalYearMonthValue for ArrayRef {}
impl GetPrimitiveValue for ArrayRef {}
impl GetPrimitiveListValue for ArrayRef {}
impl GetStringListValue for ArrayRef {}
impl GetStructValue for ArrayRef {}
impl GetTimeValue for ArrayRef {}
impl GetTimestampValue for ArrayRef {}
impl GetTimestampTzValue for ArrayRef {}
impl GetUIntValue for ArrayRef {}
impl GetUuidValue for ArrayRef {}

#[derive(Debug)]
pub enum DataTypeError {
    DataTypeMismatch(String, DataType, PgOid),
}

impl std::fmt::Display for DataTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataTypeError::DataTypeMismatch(arg1, arg2, arg3) => write!(f, "Column {} has Arrow data type {:?} but is mapped to the {:?} type in Postgres, which are incompatible. If you believe this conversion should be supported, please submit a request at https://github.com/paradedb/paradedb/issues.", arg1, arg2, arg3),
        }
    }
}

impl std::error::Error for DataTypeError {}
