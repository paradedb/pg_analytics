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

use chrono::{
    DateTime, Datelike, Days, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Timelike,
};
use pgrx::*;
use std::fmt::Debug;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::str::FromStr;

const NANOSECONDS_IN_SECOND: u32 = 1_000_000_000;

const MILLISECONDS_IN_SECOND: i64 = 1_000;

const SECONDS_IN_DAY: i64 = 86_400;

// Number of days between Apache Arrow / UNIX epoch (1970-01-01)
// and PostgreSQL epoch (2000-01-01).
const POSTGRES_BASE_DATE_OFFSET: Days = Days::new(10_957);

/// Converts an [`i32`] stored in [`arrow::array::types::Date32Type`] to PostgresSQL TimestampWithTimeZone
///
/// Takes into account [`arrow::array::types::Date32Type`] stores the number of days
/// elapsed since UNIX epoch (1970-01-01).
/// Postgres [`datum::TimestampWithTimeZone`] type takes timestamp in microseconds
/// with epoch (2000-01-01)
#[inline(always)]
pub(crate) fn arrow_date32_to_postgres_timestamps(
    timestamp_in_days: i32,
) -> Result<Option<TimestampWithTimeZone>, FromTimeError> {
    arrow_date64_to_postgres_timestamps(
        timestamp_in_days as i64 * SECONDS_IN_DAY * MILLISECONDS_IN_SECOND,
    )
}

/// Converts an [`i64`] stored in [`arrow::array::types::Date64Type`] to PostgresSQL TimestampWithTimeZone
///
/// Takes into account [`arrow::array::types::Date64Type`] stores the number of milliseconds
/// elapsed since UNIX epoch (1970-01-01).
/// Postgres [`datum::TimestampWithTimeZone`] type takes timestamp in microseconds
/// with epoch (2000-01-01)
#[inline(always)]
pub(crate) fn arrow_date64_to_postgres_timestamps(
    timestamp_in_milliseconds: i64,
) -> Result<Option<TimestampWithTimeZone>, FromTimeError> {
    DateTime::from_timestamp_millis(timestamp_in_milliseconds)
        .map(|date_time| date_time.naive_utc())
        .and_then(|naive_date_time| naive_date_time.checked_sub_days(POSTGRES_BASE_DATE_OFFSET))
        .map(|shifted_naive_date_time| shifted_naive_date_time.and_utc().timestamp_micros())
        .map(TimestampWithTimeZone::try_from)
        .transpose()
}

#[derive(Clone, Debug)]
pub struct Date(pub NaiveDate);

#[derive(Clone, Debug)]
pub struct DateTimeNoTz(pub NaiveDateTime);

#[derive(Clone, Debug)]
pub struct Time(pub NaiveTime);

#[derive(Clone, Debug)]
pub struct Interval(pub TimeDelta);

#[derive(Clone, Debug)]
pub struct DateTimeTz<Tz: TimeZone> {
    datetime: DateTime<Tz>,
    tz: String,
}

impl<Tz: TimeZone> DateTimeTz<Tz> {
    pub fn new(datetime: DateTime<Tz>, tz: &str) -> Self {
        Self {
            datetime,
            tz: tz.to_string(),
        }
    }

    pub fn datetime(&self) -> DateTime<Tz> {
        self.datetime.clone()
    }

    pub fn tz(&self) -> String {
        self.tz.clone()
    }
}

impl TryFrom<DateTimeNoTz> for datum::Timestamp {
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(datetime: DateTimeNoTz) -> Result<Self, Self::Error> {
        let DateTimeNoTz(datetime) = datetime;

        datum::Timestamp::new(
            datetime.year(),
            datetime.month() as u8,
            datetime.day() as u8,
            datetime.hour() as u8,
            datetime.minute() as u8,
            (datetime.second() + datetime.nanosecond() / NANOSECONDS_IN_SECOND).into(),
        )
    }
}

impl TryFrom<DateTimeNoTz> for datum::TimestampWithTimeZone {
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(datetime: DateTimeNoTz) -> Result<Self, Self::Error> {
        let DateTimeNoTz(datetime) = datetime;

        datum::TimestampWithTimeZone::new(
            datetime.year(),
            datetime.month() as u8,
            datetime.day() as u8,
            datetime.hour() as u8,
            datetime.minute() as u8,
            (datetime.second() + datetime.nanosecond() / NANOSECONDS_IN_SECOND).into(),
        )
    }
}

impl<Tz> TryFrom<DateTimeTz<Tz>> for datum::TimestampWithTimeZone
where
    Tz: TimeZone + FromStr + RefUnwindSafe + UnwindSafe + Debug,
{
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(datetimetz: DateTimeTz<Tz>) -> Result<Self, Self::Error> {
        let datetime = datetimetz.datetime();
        let tz = datetimetz.tz();

        datum::TimestampWithTimeZone::with_timezone(
            datetime.year(),
            datetime.month() as u8,
            datetime.day() as u8,
            datetime.hour() as u8,
            datetime.minute() as u8,
            (datetime.second() + datetime.nanosecond() / NANOSECONDS_IN_SECOND).into(),
            tz,
        )
    }
}

impl TryFrom<Date> for datum::Date {
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(date: Date) -> Result<Self, Self::Error> {
        let Date(date) = date;
        datum::Date::new(date.year(), date.month() as u8, date.day() as u8)
    }
}

impl TryFrom<Time> for datum::Time {
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(time: Time) -> Result<Self, Self::Error> {
        let Time(time) = time;

        datum::Time::new(
            time.hour() as u8,
            time.minute() as u8,
            time.second() as f64 + time.nanosecond() as f64 / NANOSECONDS_IN_SECOND as f64,
        )
    }
}

impl TryFrom<Interval> for datum::Interval {
    type Error = datum::datetime_support::DateTimeConversionError;

    fn try_from(interval: Interval) -> Result<Self, Self::Error> {
        let Interval(timedelta) = interval;
        Ok(datum::Interval::from_seconds(timedelta.num_seconds() as f64))
    }
}
