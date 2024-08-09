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

use pgrx::*;

fn set_date(year: i32, month: u8, day: u8) -> Date {
    Date::from(
        Timestamp::new(year, month, day, 0, 0, 0f64)
            .unwrap_or_else(|error| panic!("There was an error in date creation: {}", error)),
    )
}

fn set_timestamp(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: f64) -> Timestamp {
    Timestamp::new(year, month, day, hour, minute, second)
        .unwrap_or_else(|error| panic!("There was an error in timestamp creation: {}", error))
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date(_bucket_width: Interval, input: Date) -> Date {
    set_date(input.year(), input.day(), input.month())
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_origin(_bucket_width: Interval, input: Date, _origin: Date) -> Date {
    set_date(input.year(), input.day(), input.month())
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_offset(_bucket_width: Interval, input: Date, _offset: Interval) -> Date {
    set_date(input.year(), input.day(), input.month())
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp(_bucket_width: Interval, input: Timestamp) -> Timestamp {
    set_timestamp(
        input.year(),
        input.month(),
        input.day(),
        input.hour(),
        input.minute(),
        input.second(),
    )
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp_offset_date(
    _bucket_width: Interval,
    input: Timestamp,
    _origin: Date,
) -> Timestamp {
    set_timestamp(
        input.year(),
        input.month(),
        input.day(),
        input.hour(),
        input.minute(),
        input.second(),
    )
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp_offset_interval(
    _bucket_width: Interval,
    input: Timestamp,
    _offset: Interval,
) -> Timestamp {
    set_timestamp(
        input.year(),
        input.month(),
        input.day(),
        input.hour(),
        input.minute(),
        input.second(),
    )
}
