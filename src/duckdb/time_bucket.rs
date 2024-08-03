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
use pgrx::iter::TableIterator;

#[pg_extern(name = "time_bucket")]
pub fn time_bucket_date_no_offset(
    _bucket_width: Interval,
    _input: Date,
) -> TableIterator<'static, (name!(time_bucket, Date),)> {
    TableIterator::once((""
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}

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
pub fn time_bucket_timestamp(
    _bucket_width: Interval,
    _input: Timestamp,
) -> TableIterator<'static, (name!(time_bucket, Timestamp),)> {
    TableIterator::once((""
        .parse()
        .unwrap_or_else(|err| panic!("There was an error while parsing time_bucket(): {}", err)),))
}

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
