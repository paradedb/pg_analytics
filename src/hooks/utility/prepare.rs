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

use std::ffi::CStr;
use std::ptr::null_mut;

use anyhow::Result;
use pgrx::{error, pg_sys, pgbox, warning, PgBox};

use crate::duckdb::connection;
use crate::hooks::query::*;

pub fn execute_query<T: pgbox::WhoAllocated>(
    _psate: *mut pg_sys::ParseState,
    stmt: *mut pg_sys::ExecuteStmt,
    query_desc: PgBox<pg_sys::QueryDesc, T>,
) -> Result<bool> {
    unsafe {
        let prepared_stmt = pg_sys::FetchPreparedStatement((*stmt).name, true);
        let plan_source = (*prepared_stmt).plansource;

        if plan_source.is_null() || !(*plan_source).fixed_result {
            return Ok(true);
        }

        // We need to ensure that DuckDB replans the PREPARE statement when the search path changes,
        // in order to match PostgreSQL’s default behavior.

        #[cfg(not(feature = "pg17"))]
        let need_replan = !pg_sys::OverrideSearchPathMatchesCurrent((*plan_source).search_path);
        #[cfg(feature = "pg17")]
        let need_replan = !pg_sys::SearchPathMatchesCurrentEnvironment((*plan_source).search_path);

        // For PostgreSQL 13
        #[cfg(feature = "pg13")]
        let cached_plan = pg_sys::GetCachedPlan(plan_source, null_mut(), false, null_mut());
        // For PostgreSQL 14 and above
        #[cfg(not(feature = "pg13"))]
        let cached_plan = pg_sys::GetCachedPlan(plan_source, null_mut(), null_mut(), null_mut());

        if cached_plan.is_null() || (*cached_plan).stmt_list.is_null() {
            return Ok(true);
        }

        let planned_stmt =
            (*(*(*cached_plan).stmt_list).elements.offset(0)).ptr_value as *mut pg_sys::PlannedStmt;
        let query_relations = get_query_relations((*planned_stmt).rtable);
        if (*planned_stmt).commandType != pg_sys::CmdType::CMD_SELECT
            || !is_duckdb_query(&query_relations)
        {
            return Ok(true);
        }

        (*query_desc.as_ptr()).tupDesc = (*plan_source).resultDesc;

        // Note that DuckDB does not replan prepared statements when the search path changes.
        // We enforce this by executing the PREPARE statement again.
        set_search_path_by_pg()?;

        if need_replan {
            let prepare_stmt = CStr::from_ptr((*plan_source).query_string);
            if let Err(e) = connection::execute(prepare_stmt.to_str()?, []) {
                error!("execute prepare replan error: {}", e.to_string());
            }
        }
    }

    let query = unsafe { CStr::from_ptr((*query_desc.as_ptr()).sourceText) };

    match connection::create_arrow(query.to_str()?) {
        Err(err) => {
            connection::clear_arrow();
            fallback_warning!(err.to_string());
            return Ok(true);
        }
        Ok(false) => {
            connection::clear_arrow();
            return Ok(false);
        }
        _ => {}
    }

    match connection::get_batches() {
        Ok(batches) => write_batches_to_slots(query_desc, batches)?,
        Err(err) => {
            connection::clear_arrow();
            fallback_warning!(err.to_string());
            return Ok(true);
        }
    }

    connection::clear_arrow();
    Ok(false)
}

pub fn deallocate_query(stmt: *mut pg_sys::DeallocateStmt) -> Result<bool> {
    if !unsafe { (*stmt).name }.is_null() {
        let name = unsafe { CStr::from_ptr((*stmt).name) };
        // We don't care the result
        // Next prepare statement will override this one.
        let _ = connection::execute(&format!(r#"DEALLOCATE "{}""#, name.to_str()?), []);
    }

    Ok(true)
}
