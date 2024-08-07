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

use chrono::{DateTime, Datelike, NaiveDateTime, Timelike};
use duckdb::arrow::temporal_conversions::SECONDS_IN_DAY;
use pgrx::iter::TableIterator;
use pgrx::*;

// Origin epoch for months/years intervals.
// The origin date is 2000-01-01.
// Please see: https://duckdb.org/docs/sql/functions/date.html#time_bucketbucket_width-date-origin
const ORIGIN_UNIX_EPOCH: i128 = 946684800;
// Origin epoch for days/minutes/seconds intervals.
// Date is set to 2000-01-03.
// Please see: https://duckdb.org/docs/sql/functions/date.html#time_bucketbucket_width-date-origin
const DAYS_ORIGIN_UNIX_EPOCH: i128 = 946857600;
const MICROS_PER_SECOND: i128 = 1000000;

fn set_date(year: i32, month: u32, day: u32) -> Date {
    Date::from(
        Timestamp::new(year, month as u8, day as u8, 0, 0, 0f64)
            .unwrap_or_else(|error| panic!("There was an error in date creation: {}", error)),
    )
}

fn set_timestamp(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: f64) -> Timestamp {
    Timestamp::new(year, month, day, hour, minute, second)
        .unwrap_or_else(|error| panic!("There was an error in timestamp creation: {}", error))
}

fn calculate_time_bucket(
    bucket_width_seconds: i128,
    input_unix_epoch: i128,
    months: i32,
    override_origin_epoch: Option<i128>,
) -> i128 {
    if let Some(new_origin_epoch) = override_origin_epoch {
        let truncated_input_unix_epoch =
            ((input_unix_epoch - new_origin_epoch) / bucket_width_seconds) * bucket_width_seconds;
        return new_origin_epoch + truncated_input_unix_epoch;
    }

    // Please see: https://duckdb.org/docs/sql/functions/date.html#time_bucketbucket_width-date-origin
    // DuckDB will change which origin it uses based on whether months are set in the INTERVAL.
    if months != 0 {
        let truncated_input_unix_epoch =
            ((input_unix_epoch - ORIGIN_UNIX_EPOCH) / bucket_width_seconds) * bucket_width_seconds;
        ORIGIN_UNIX_EPOCH + truncated_input_unix_epoch
    } else {
        let truncated_input_unix_epoch = ((input_unix_epoch - DAYS_ORIGIN_UNIX_EPOCH)
            / bucket_width_seconds)
            * bucket_width_seconds;
        DAYS_ORIGIN_UNIX_EPOCH + truncated_input_unix_epoch
    }
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date(bucket_width: Interval, input: Date) -> Date {
    let bucket_width_seconds = bucket_width.as_micros() / MICROS_PER_SECOND;
    let input_unix_epoch = (input.to_unix_epoch_days() as i64 * SECONDS_IN_DAY) as i128;

    let bucket_date = calculate_time_bucket(
        bucket_width_seconds,
        input_unix_epoch,
        bucket_width.months(),
        None,
    );

    if let Some(dt) = DateTime::from_timestamp(bucket_date as i64, 0) {
        set_date(dt.year(), dt.month(), dt.day())
    } else {
        panic!("There was a problem setting the native datetime from provided unix epoch.")
    }
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_origin(bucket_width: Interval, input: Date, origin: Date) -> Date {
    let new_origin_epoch = (origin.to_unix_epoch_days() as i64 * SECONDS_IN_DAY) as i128;

    let bucket_width_seconds = bucket_width.as_micros() / MICROS_PER_SECOND;
    let input_unix_epoch = (input.to_unix_epoch_days() as i64 * SECONDS_IN_DAY) as i128;

    let bucket_date = calculate_time_bucket(
        bucket_width_seconds,
        input_unix_epoch,
        bucket_width.months(),
        Some(new_origin_epoch),
    );

    if let Some(dt) = DateTime::from_timestamp(bucket_date as i64, 0) {
        set_date(dt.year(), dt.month(), dt.day())
    } else {
        panic!("There was a problem setting the native datetime from provided unix epoch.")
    }
}

// TODO: Need to implement offset for pg
#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_offset(
    _bucket_width: Interval,
    _input: Date,
    _offset: Interval,
) -> TableIterator<'static, (name!(time_bucket, Date),)> {
    TableIterator::once((""
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp(bucket_width: Interval, input: Timestamp) -> Timestamp {
    let bucket_width_seconds = bucket_width.as_micros() / MICROS_PER_SECOND;
    let input_string = input.to_iso_string();

    let input_unix_epoch = NaiveDateTime::parse_from_str(&input_string, "%Y-%m-%dT%H:%M:%S")
        .unwrap_or_else(|error| {
            panic!(
                "there was an error parsing the set TIMESTAMP value as a string: {}",
                error
            )
        });

    let bucket_date = calculate_time_bucket(
        bucket_width_seconds,
        input_unix_epoch.and_utc().timestamp() as i128,
        bucket_width.months(),
        None,
    );

    if let Some(dt) = DateTime::from_timestamp(bucket_date as i64, 0) {
        set_timestamp(
            dt.year(),
            dt.month() as u8,
            dt.day() as u8,
            dt.hour() as u8,
            dt.minute() as u8,
            dt.second() as f64,
        )
    } else {
        panic!("There was a problem setting the native datetime from provided unix epoch.")
    }
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp_offset_date(
    bucket_width: Interval,
    input: Timestamp,
    origin: Date,
) -> Timestamp {
    let new_origin_epoch = (origin.to_unix_epoch_days() as i64 * SECONDS_IN_DAY) as i128;

    let bucket_width_seconds = bucket_width.as_micros() / MICROS_PER_SECOND;
    let input_string = input.to_iso_string();

    let input_unix_epoch = NaiveDateTime::parse_from_str(&input_string, "%Y-%m-%dT%H:%M:%S")
        .unwrap_or_else(|error| {
            panic!(
                "there was an error parsing the set TIMESTAMP value as a string: {}",
                error
            )
        });

    let bucket_date = calculate_time_bucket(
        bucket_width_seconds,
        input_unix_epoch.and_utc().timestamp() as i128,
        bucket_width.months(),
        Some(new_origin_epoch),
    );

    if let Some(dt) = DateTime::from_timestamp(bucket_date as i64, 0) {
        set_timestamp(
            dt.year(),
            dt.month() as u8,
            dt.day() as u8,
            dt.hour() as u8,
            dt.minute() as u8,
            dt.second() as f64,
        )
    } else {
        panic!("There was a problem setting the native datetime from provided unix epoch.")
    }
}

// TODO: Need to implement offset for pg
#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp_offset_interval(
    _bucket_width: Interval,
    _input: Timestamp,
    _offset: Interval,
) -> TableIterator<'static, (name!(time_bucket, Timestamp),)> {
    TableIterator::once((""
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}
