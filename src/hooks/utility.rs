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
#![allow(deprecated)]
mod explain;
mod prepare;
mod view;

use std::ptr::null_mut;

use super::query::*;
use anyhow::{bail, Result};
use explain::explain_query;
use pgrx::{pg_sys, AllocatedByRust, HookResult, PgBox};
use prepare::*;
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
use view::view_query;

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

    // IsPostmasterEnvironment is false when it is false in a standalone process (bootstrap or
    // standalone backend).
    if !is_support_utility(stmt_type) || unsafe { !pg_sys::IsPostmasterEnvironment } {
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

    let parse_state = unsafe {
        let state = pg_sys::make_parsestate(null_mut());
        (*state).p_sourcetext = query_string.as_ptr();
        (*state).p_queryEnv = query_env.as_ptr();
        state
    };

    let need_exec_prev_hook = match stmt_type {
        pg_sys::NodeTag::T_PrepareStmt => prepare_query(
            parse_state,
            pstmt.utilityStmt as *mut pg_sys::PrepareStmt,
            pstmt.stmt_location,
            pstmt.stmt_len,
        )?,

        pg_sys::NodeTag::T_ExecuteStmt => {
            let mut query_desc = unsafe {
                PgBox::<pg_sys::QueryDesc, AllocatedByRust>::from_rust(pg_sys::CreateQueryDesc(
                    pstmt.as_ptr(),
                    query_string.as_ptr(),
                    null_mut(),
                    null_mut(),
                    dest.as_ptr(),
                    null_mut(),
                    query_env.as_ptr(),
                    0,
                ))
            };
            query_desc.estate = unsafe { pg_sys::CreateExecutorState() };

            execute_query(
                parse_state,
                pstmt.utilityStmt as *mut pg_sys::ExecuteStmt,
                query_desc,
            )?
        }

        pg_sys::NodeTag::T_DeallocateStmt => {
            deallocate_query(pstmt.utilityStmt as *mut pg_sys::DeallocateStmt)?
        }

        pg_sys::NodeTag::T_ExplainStmt => explain_query(
            query_string,
            pstmt.utilityStmt as *mut pg_sys::ExplainStmt,
            dest.as_ptr(),
        )?,
        pg_sys::NodeTag::T_ViewStmt => {
            let utility_stmt = unsafe {
                pg_sys::copyObjectImpl(pstmt.utilityStmt as *const std::ffi::c_void)
                    as *mut pg_sys::Node
            };
            view_query(
                query_string,
                utility_stmt as *mut pg_sys::ViewStmt,
                pstmt.stmt_location,
                pstmt.stmt_len,
            )?
        }
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

fn is_support_utility(stmt_type: pg_sys::NodeTag) -> bool {
    stmt_type == pg_sys::NodeTag::T_ExplainStmt
        || stmt_type == pg_sys::NodeTag::T_ViewStmt
        || stmt_type == pg_sys::NodeTag::T_PrepareStmt
        || stmt_type == pg_sys::NodeTag::T_DeallocateStmt
        || stmt_type == pg_sys::NodeTag::T_ExecuteStmt
}

fn parse_query_from_utility_stmt(query_string: &core::ffi::CStr) -> Result<String> {
    let query_string = query_string.to_str()?;

    let dialect = PostgreSqlDialect {};
    let utility = Parser::parse_sql(&dialect, query_string)?;

    debug_assert!(utility.len() == 1);
    match &utility[0] {
        Statement::Explain { statement, .. } => Ok(statement.to_string()),
        _ => bail!("unexpected utility statement: {}", query_string),
    }
}
