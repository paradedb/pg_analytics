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

use std::ptr::null_mut;

use anyhow::Result;

use pgrx::{pg_sys, warning};

use crate::{duckdb::connection::execute, hooks::query::is_duckdb_query};

use super::{get_query_relations, set_search_path_by_pg};

pub fn view_query(
    query_string: &core::ffi::CStr,
    pstate: *mut pg_sys::ParseState,
    stmt: *mut pg_sys::ViewStmt,
    stmt_location: i32,
    stmt_len: i32,
) -> Result<bool> {
    // Perform parsing and analysis to get the Query
    let query = unsafe {
        let mut raw_stmt = pg_sys::RawStmt {
            type_: pg_sys::NodeTag::T_RawStmt,
            stmt: (*stmt).query,
            stmt_location,
            stmt_len,
        };

        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        {
            pg_sys::parse_analyze_fixedparams(
                &mut raw_stmt,
                (*pstate).p_sourcetext,
                null_mut(),
                0,
                null_mut(),
            )
        }

        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::parse_analyze(
                &mut raw_stmt,
                (*pstate).p_sourcetext,
                null_mut(),
                0,
                null_mut(),
            )
        }
    };

    let query_relations = get_query_relations(unsafe { (*query).rtable });

    if unsafe { (*query).commandType } != pg_sys::CmdType::CMD_SELECT
        || !is_duckdb_query(&query_relations)
    {
        fallback_warning!("Some relations are not in DuckDB");
        return Ok(true);
    }
    // Push down the view creation query to DuckDB
    set_search_path_by_pg()?;
    execute(query_string.to_str()?, [])?;
    Ok(true)
}
