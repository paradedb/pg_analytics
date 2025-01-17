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

use std::ptr::null_mut;

use anyhow::Result;

use pgrx::{
    pg_sys::{self},
    warning,
};

use crate::{duckdb::connection::execute, hooks::query::is_duckdb_query};

use super::{get_query_relations, set_search_path_by_pg};

pub fn view_query(
    query_string: &core::ffi::CStr,
    stmt: *mut pg_sys::ViewStmt,
    stmt_location: i32,
    stmt_len: i32,
) -> Result<bool> {
    // Perform parsing and analysis to get the Query
    let rewritten_queries = unsafe {
        let mut raw_stmt = pgrx::PgBox::<pg_sys::RawStmt>::alloc_node(pg_sys::NodeTag::T_RawStmt);
        raw_stmt.stmt = (*stmt).query;
        raw_stmt.stmt_location = stmt_location;
        raw_stmt.stmt_len = stmt_len;

        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        {
            pg_sys::pg_analyze_and_rewrite_fixedparams(
                raw_stmt.as_ptr(),
                query_string.as_ptr(),
                null_mut(),
                0,
                null_mut(),
            )
        }

        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::pg_analyze_and_rewrite(
                raw_stmt.as_ptr(),
                query_string.as_ptr(),
                null_mut(),
                0,
                null_mut(),
            )
        }
    };

    let plan_list = unsafe {
        pg_sys::pg_plan_queries(
            rewritten_queries,
            query_string.as_ptr(),
            pg_sys::CURSOR_OPT_PARALLEL_OK as i32,
            null_mut(),
        )
    };

    unsafe {
        for i in 0..(*plan_list).length {
            let planned_stmt: *mut pg_sys::PlannedStmt =
                (*(*plan_list).elements.offset(i as isize)).ptr_value as *mut pg_sys::PlannedStmt;

            let query_relations = get_query_relations((*planned_stmt).rtable);

            if (*planned_stmt).commandType != pg_sys::CmdType::CMD_SELECT
                || !is_duckdb_query(&query_relations)
            {
                return Ok(true);
            }
        }
    }

    // Push down the view creation query to DuckDB
    set_search_path_by_pg()?;
    if let Err(e) = execute(query_string.to_str()?, []) {
        fallback_warning!(e.to_string());
    }

    Ok(true)
}
