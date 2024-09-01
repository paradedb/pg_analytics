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

#![allow(clippy::too_many_arguments)]

use std::ffi::CString;

use anyhow::{anyhow, bail, Result};
use pg_sys::NodeTag;
use pgrx::*;
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

use super::query::*;

#[allow(deprecated)]
type ProcessUtilityHook = fn(
    pstmt: PgBox<pg_sys::PlannedStmt>,
    query_string: &core::ffi::CStr,
    read_only_tree: Option<bool>,
    context: pg_sys::ProcessUtilityContext::Type,
    params: PgBox<pg_sys::ParamListInfoData>,
    query_env: PgBox<pg_sys::QueryEnvironment>,
    dest: PgBox<pg_sys::DestReceiver>,
    completion_tag: *mut pg_sys::QueryCompletion,
) -> HookResult<()>;

pub async fn process_utility_hook(
    pstmt: PgBox<pg_sys::PlannedStmt>,
    query_string: &core::ffi::CStr,
    read_only_tree: Option<bool>,
    context: pg_sys::ProcessUtilityContext::Type,
    params: PgBox<pg_sys::ParamListInfoData>,
    query_env: PgBox<pg_sys::QueryEnvironment>,
    dest: PgBox<pg_sys::DestReceiver>,
    completion_tag: *mut pg_sys::QueryCompletion,
    prev_hook: ProcessUtilityHook,
) -> Result<()> {
    let stmt_type = unsafe { (*pstmt.utilityStmt).type_ };

    if !is_support_utility(stmt_type) {
        prev_hook(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            completion_tag,
        );

        return Ok(());
    }

    let need_exec_prev_hook = match stmt_type {
        pg_sys::NodeTag::T_ExplainStmt => explain_query(
            query_string,
            pstmt.utilityStmt as *mut pg_sys::ExplainStmt,
            dest.as_ptr(),
        )?,
        _ => bail!("unexpected statement type in utility hook"),
    };

    if need_exec_prev_hook {
        prev_hook(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            completion_tag,
        );
    }

    Ok(())
}

fn is_support_utility(stmt_type: NodeTag) -> bool {
    stmt_type == pg_sys::NodeTag::T_ExplainStmt
}

fn explain_query(
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
        return Ok(false);
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

fn parse_query_from_utility_stmt(query_string: &core::ffi::CStr) -> Result<String> {
    let query_string = query_string.to_str()?;

    let dialect = PostgreSqlDialect {};
    let utility = Parser::parse_sql(&dialect, query_string)?;

    debug_assert!(utility.len() == 1);
    match &utility[0] {
        Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            statement,
            format,
        } => Ok(statement.to_string()),
        _ => bail!("unexpected utility statement: {}", query_string),
    }
}
