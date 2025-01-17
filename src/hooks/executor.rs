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
use std::ffi::CStr;

use crate::duckdb::connection;

use super::query::*;

#[cfg(debug_assertions)]
use crate::DEBUG_GUCS;

#[allow(deprecated)]
pub async fn executor_run(
    query_desc: PgBox<pg_sys::QueryDesc>,
    direction: pg_sys::ScanDirection::Type,
    count: u64,
    execute_once: bool,
    prev_hook: fn(
        query_desc: PgBox<pg_sys::QueryDesc>,
        direction: pg_sys::ScanDirection::Type,
        count: u64,
        execute_once: bool,
    ) -> HookResult<()>,
) -> Result<()> {
    #[cfg(debug_assertions)]
    if DEBUG_GUCS.disable_executor.get() {
        log!("executor hook query pushdown is disabled");
        prev_hook(query_desc, direction, count, execute_once);
        return Ok(());
    }

    let ps = query_desc.plannedstmt;
    let rtable = unsafe { (*ps).rtable };
    let query = get_current_query(ps, unsafe { CStr::from_ptr(query_desc.sourceText) })?;
    let query_relations = get_query_relations(unsafe { (*ps).rtable });
    let is_duckdb_query = is_duckdb_query(&query_relations);

    if rtable.is_null()
        || query_desc.operation != pg_sys::CmdType::CMD_SELECT
        || !is_duckdb_query
        // Tech Debt: Find a less hacky way to let COPY/CREATE go through
        || query.to_lowercase().starts_with("copy")
        || query.to_lowercase().starts_with("create")
        || query.to_lowercase().starts_with("prepare")
    {
        prev_hook(query_desc, direction, count, execute_once);
        return Ok(());
    }

    // Set DuckDB search path according search path in Postgres
    // Make sure it could find unqualified relations.
    set_search_path_by_pg()?;

    match connection::create_arrow(query.as_str()) {
        Err(err) => {
            connection::clear_arrow();
            fallback_warning!(err.to_string());
            prev_hook(query_desc, direction, count, execute_once);
            return Ok(());
        }
        Ok(false) => {
            connection::clear_arrow();
            return Ok(());
        }
        _ => {}
    }

    match connection::get_batches() {
        Ok(batches) => write_batches_to_slots(query_desc, batches)?,
        Err(err) => {
            connection::clear_arrow();
            fallback_warning!(err.to_string());
            prev_hook(query_desc, direction, count, execute_once);
            return Ok(());
        }
    }

    connection::clear_arrow();
    Ok(())
}
