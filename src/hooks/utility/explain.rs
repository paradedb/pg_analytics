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

use std::ffi::CString;

use anyhow::Result;
use pgrx::{error, pg_sys};

use super::parse_query_from_utility_stmt;
use crate::hooks::query::{get_query_relations, is_duckdb_query};

pub fn explain_query(
    query_string: &core::ffi::CStr,
    stmt: *mut pg_sys::ExplainStmt,
    dest: *mut pg_sys::DestReceiver,
) -> Result<bool> {
    let query = unsafe { (*stmt).query as *mut pg_sys::Query };

    let query_relations = get_query_relations(unsafe { (*query).rtable });
    if unsafe { (*query).commandType } != pg_sys::CmdType::CMD_SELECT
        || !is_duckdb_query(&query_relations)
    {
        return Ok(true);
    }

    if unsafe { !(*stmt).options.is_null() } {
        error!("the EXPLAIN options provided are not supported for DuckDB pushdown queries.");
    }

    unsafe {
        let tstate = pg_sys::begin_tup_output_tupdesc(
            dest,
            pg_sys::ExplainResultDesc(stmt),
            &pg_sys::TTSOpsVirtual,
        );
        let query = format!(
            "DuckDB Scan: {}",
            parse_query_from_utility_stmt(query_string)?
        );
        let query_c_str = CString::new(query)?;

        pg_sys::do_text_output_multiline(tstate, query_c_str.as_ptr());
        pg_sys::end_tup_output(tstate);
    }

    Ok(false)
}
