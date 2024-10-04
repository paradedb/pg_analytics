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

use std::ffi::CStr;
use std::ptr::null_mut;

use anyhow::Result;
use pgrx::{error, pg_sys, pgbox, warning, PgBox};

use crate::duckdb::connection;
use crate::hooks::query::*;

pub fn prepare_query(
    pstate: *mut pg_sys::ParseState,
    stmt: *mut pg_sys::PrepareStmt,
    stmt_location: i32,
    stmt_len: i32,
) -> Result<bool> {
    if unsafe { (*stmt).name }.is_null() || unsafe { *(*stmt).name } == '\0' as std::os::raw::c_char
    {
        return Ok(true);
    }

    let prepared_stmt = unsafe { pg_sys::FetchPreparedStatement((*stmt).name, false) };
    if !prepared_stmt.is_null() {
        let stmt_name = unsafe { CStr::from_ptr((*stmt).name) };
        error!(
            "prepared statement \"{}\" already exists",
            stmt_name.to_str()?
        );
    }

    // Perform parsing and analysis to get the Query
    let query = unsafe {
        let mut raw_stmt = pg_sys::RawStmt {
            type_: pg_sys::NodeTag::T_RawStmt,
            stmt: (*stmt).query,
            stmt_location,
            stmt_len,
        };

        let arg_types = (*stmt).argtypes;
        let mut nargs = if arg_types.is_null() {
            0
        } else {
            (*arg_types).length
        };

        // Transform list of TypeNames to array of type OIDs
        let mut types_oid: *mut pg_sys::Oid = if nargs > 0 {
            let oid_ptr = pg_sys::palloc((nargs as usize) * std::mem::size_of::<pg_sys::Oid>())
                as *mut pg_sys::Oid;
            let type_elements = (*arg_types).elements;
            for i in 0..(*arg_types).length {
                let type_name =
                    (*type_elements.offset(i as isize)).ptr_value as *const pg_sys::TypeName;
                *oid_ptr.offset(i as isize) = pg_sys::typenameTypeId(pstate, type_name)
            }
            oid_ptr
        } else {
            null_mut()
        };

        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        {
            pg_sys::parse_analyze_varparams(
                &mut raw_stmt,
                (*pstate).p_sourcetext,
                &mut types_oid,
                &mut nargs,
                null_mut(),
            )
        }
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::parse_analyze_varparams(
                &mut raw_stmt,
                (*pstate).p_sourcetext,
                &mut types_oid,
                &mut nargs,
            )
        }
    };

    let query_relations = get_query_relations(unsafe { (*query).rtable });
    if unsafe { (*query).commandType } != pg_sys::CmdType::CMD_SELECT
        || !is_duckdb_query(&query_relations)
    {
        return Ok(true);
    }

    // set search path according to postgres
    set_search_path_by_pg()?;

    let query_sql: &CStr = unsafe { CStr::from_ptr((*pstate).p_sourcetext) };
    if let Err(e) = connection::execute(query_sql.to_str()?, []) {
        fallback_warning!(e.to_string());
        return Ok(true);
    }

    // It's always necessary to execute the previous hook to store a prepared statement in PostgreSQL
    Ok(true)
}

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

        // we need to make Duckdb replan the `PREPARE` statement when search path changed.
        #[cfg(not(feature = "pg17"))]
        let need_replan = !pg_sys::OverrideSearchPathMatchesCurrent((*plan_source).search_path);

        #[cfg(feature = "pg17")]
        let need_replan = !pg_sys::SearchPathMatchesCurrentEnvironment((*plan_source).search_path);

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
