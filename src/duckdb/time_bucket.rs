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

use pgrx::iter::TableIterator;
use pgrx::*;

const HOURS_PER_MICRO: i64 = 3600000000;
const MINUTES_PER_MICRO: i64 = 60000000;

fn set_date(year: i32, month: u8, day: u8) -> Date {
    return Date::from(
        Timestamp::new(year, month, day, 0, 0, 0f64)
            .unwrap_or_else(|error| panic!("There was an error in date creation: {}", error)),
    );
}

fn set_timestamp(year: i32, month: u8, day: u8, hour: u8, minute: u8) -> Timestamp {
    return Timestamp::new(year, month, day, hour, minute, 0f64)
        .unwrap_or_else(|error| panic!("There was an error in timestamp creation: {}", error));
}

fn get_micros_delta(micros: i64, input: u8, divisor: i64) -> u8 {
    let micros_quotient = (micros / divisor) as u8;
    if micros_quotient == 0 {
        return 0;
    }
    input % micros_quotient
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_no_offset(bucket_width: Interval, input: Date) -> Date {
    let years = bucket_width.months() / 12;
    if years != 0 {
        let delta = input.year() as i32 % years;
        return set_date(input.year() - delta, input.month(), input.day());
    } else if bucket_width.months() != 0 {
        let delta = input.month() as i32 % bucket_width.months();
        return set_date(input.year(), input.month() - delta as u8, input.day());
    } else if bucket_width.days() != 0 {
        let delta = input.day() as i32 % bucket_width.days();
        return set_date(input.year(), input.month(), input.day() - delta as u8);
    }

    Date::from(Timestamp::new(input.year(), input.month(), input.day(), 0, 0, 0f64).unwrap())
}

// TODO: Need to implement offset for pg
#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_offset_date(
    _bucket_width: Interval,
    _input: Date,
    _offset: Date,
) -> TableIterator<'static, (name!(time_bucket, Date),)> {
    TableIterator::once((""
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}

// TODO: Need to implement offset for pg
#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_offset_interval(
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
    let years = bucket_width.months() / 12;
    if years != 0 {
        let delta = input.year() as i32 % years;
        return set_timestamp(
            input.year() - delta,
            input.month(),
            input.day(),
            input.hour(),
            input.minute(),
        );
    } else if bucket_width.months() != 0 {
        let delta = input.month() as i32 % bucket_width.months();
        return set_timestamp(
            input.year(),
            input.month() - delta as u8,
            input.day(),
            input.hour(),
            input.minute(),
        );
    } else if bucket_width.days() != 0 {
        let delta = input.day() as i32 % bucket_width.days();
        return set_timestamp(
            input.year(),
            input.month(),
            input.day() - delta as u8,
            input.hour(),
            input.minute(),
        );
    } else if bucket_width.micros() != 0 {
        let hours_delta = get_micros_delta(bucket_width.micros(), input.hour(), HOURS_PER_MICRO);
        let minutes_delta =
            get_micros_delta(bucket_width.micros(), input.minute(), MINUTES_PER_MICRO);
        return set_timestamp(
            input.year(),
            input.month(),
            input.day(),
            input.hour() - hours_delta,
            input.minute() - minutes_delta,
        );
    }

    Timestamp::new(
        input.year(),
        input.month(),
        input.day(),
        input.hour(),
        input.minute(),
        0f64,
    )
    .unwrap()
}

// TODO: Need to implement offset for pg
#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp_offset_date(
    _bucket_width: Interval,
    _input: Timestamp,
    _offset: Date,
) -> TableIterator<'static, (name!(time_bucket, Timestamp),)> {
    TableIterator::once((""
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
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
