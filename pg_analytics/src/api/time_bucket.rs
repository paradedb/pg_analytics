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

use pgrx::{datum::*, pg_extern};

const TIME_BUCKET_FALLBACK_ERROR: &str = "Function `time_bucket()` must be used with a DuckDB FDW. Native postgres does not support this function. If you believe this function should be implemented natively as a fallback please submit a ticket to https://github.com/paradedb/pg_analytics/issues.";

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date(_bucket_width: Interval, _input: Date) -> Date {
    panic!("{}", TIME_BUCKET_FALLBACK_ERROR);
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_origin(_bucket_width: Interval, _input: Date, _origin: Date) -> Date {
    panic!("{}", TIME_BUCKET_FALLBACK_ERROR);
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_offset(_bucket_width: Interval, _input: Date, _offset: Interval) -> Date {
    panic!("{}", TIME_BUCKET_FALLBACK_ERROR);
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp(_bucket_width: Interval, _input: Timestamp) -> Timestamp {
    panic!("{}", TIME_BUCKET_FALLBACK_ERROR);
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp_offset_date(
    _bucket_width: Interval,
    _input: Timestamp,
    _origin: Date,
) -> Timestamp {
    panic!("{}", TIME_BUCKET_FALLBACK_ERROR);
}

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_timestamp_offset_interval(
    _bucket_width: Interval,
    _input: Timestamp,
    _offset: Interval,
) -> Timestamp {
    panic!("{}", TIME_BUCKET_FALLBACK_ERROR);
}
