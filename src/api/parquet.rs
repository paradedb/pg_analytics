// Copyright (c) 2023-2025 Retake, Inc.
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

use anyhow::Result;
use pgrx::*;
use supabase_wrappers::prelude::{options_to_hashmap, user_mapping_options};

use crate::duckdb::connection;
use crate::duckdb::parquet::ParquetOption;
use crate::duckdb::utils;
use crate::fdw::base::register_duckdb_view;
use crate::fdw::handler::FdwHandler;

type ParquetSchemaRow = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

type ParquetDescribeRow = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn parquet_describe(
    relation: PgRelation,
) -> iter::TableIterator<
    'static,
    (
        name!(column_name, Option<String>),
        name!(column_type, Option<String>),
        name!(null, Option<String>),
        name!(key, Option<String>),
        name!(default, Option<String>),
        name!(extra, Option<String>),
    ),
> {
    let rows = parquet_describe_impl(relation).unwrap_or_else(|e| {
        panic!("{}", e);
    });
    iter::TableIterator::new(rows)
}

#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn parquet_schema(
    relation: PgRelation,
) -> iter::TableIterator<
    'static,
    (
        name!(file_name, Option<String>),
        name!(name, Option<String>),
        name!(type, Option<String>),
        name!(type_length, Option<String>),
        name!(repetition_type, Option<String>),
        name!(num_children, Option<i64>),
        name!(converted_type, Option<String>),
        name!(scale, Option<i64>),
        name!(precision, Option<i64>),
        name!(field_id, Option<i64>),
        name!(logical_type, Option<String>),
    ),
> {
    let rows = parquet_schema_impl(relation).unwrap_or_else(|e| {
        panic!("{}", e);
    });
    iter::TableIterator::new(rows)
}

#[inline]
fn parquet_schema_impl(relation: PgRelation) -> Result<Vec<ParquetSchemaRow>> {
    let foreign_table = unsafe { pg_sys::GetForeignTable(relation.oid()) };
    let handler = FdwHandler::from(foreign_table);
    if FdwHandler::from(foreign_table) != FdwHandler::Parquet {
        panic!("relation is not a parquet table");
    }

    let foreign_server = unsafe { pg_sys::GetForeignServer((*foreign_table).serverid) };
    let user_mapping_options = unsafe { user_mapping_options(foreign_server) };
    let table_options = unsafe { options_to_hashmap((*foreign_table).options)? };

    register_duckdb_view(
        relation.name(),
        relation.namespace(),
        table_options.clone(),
        user_mapping_options,
        handler,
    )?;

    let files = utils::format_csv(
        table_options
            .get(ParquetOption::Files.as_ref())
            .expect("table should have files option"),
    );

    let conn = unsafe { &*connection::get_global_connection().get() };
    let query = format!("SELECT * FROM parquet_schema({files})");
    let mut stmt = conn.prepare(&query)?;

    Ok(stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<String>>(10)?,
            ))
        })?
        .map(|row| row.unwrap())
        .collect::<Vec<ParquetSchemaRow>>())
}

#[inline]
fn parquet_describe_impl(relation: PgRelation) -> Result<Vec<ParquetDescribeRow>> {
    let foreign_table = unsafe { pg_sys::GetForeignTable(relation.oid()) };
    let handler = FdwHandler::from(foreign_table);
    if FdwHandler::from(foreign_table) != FdwHandler::Parquet {
        panic!("relation is not a parquet table");
    }

    let foreign_server = unsafe { pg_sys::GetForeignServer((*foreign_table).serverid) };
    let user_mapping_options = unsafe { user_mapping_options(foreign_server) };
    let table_options = unsafe { options_to_hashmap((*foreign_table).options)? };

    register_duckdb_view(
        relation.name(),
        relation.namespace(),
        table_options.clone(),
        user_mapping_options,
        handler,
    )?;

    let files = utils::format_csv(
        table_options
            .get(ParquetOption::Files.as_ref())
            .expect("table should have files option"),
    );
    let conn = unsafe { &*connection::get_global_connection().get() };
    let query = format!("DESCRIBE SELECT * FROM {files}");
    let mut stmt = conn.prepare(&query)?;

    Ok(stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?
        .map(|row| row.unwrap())
        .collect::<Vec<ParquetDescribeRow>>())
}
