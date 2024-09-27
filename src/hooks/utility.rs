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

use std::ffi::{CStr, CString};

use anyhow::{bail, Result};
use pg_sys::NodeTag;
use pgrx::*;
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

use crate::{
    duckdb::connection::{execute, view_exists},
    fallback_warning,
};

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
        pg_sys::NodeTag::T_ViewStmt => {
            view_query(query_string, pstmt.utilityStmt as *mut pg_sys::ViewStmt)?
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

fn is_support_utility(stmt_type: NodeTag) -> bool {
    stmt_type == pg_sys::NodeTag::T_ExplainStmt || stmt_type == pg_sys::NodeTag::T_ViewStmt
}

fn view_query(query_string: &core::ffi::CStr, stmt: *mut pg_sys::ViewStmt) -> Result<bool> {
    // Get the current schema in Postgres
    let current_schema = get_postgres_current_schema();
    // Set DuckDB search path according search path in Postgres
    set_search_path_by_pg()?;

    let query = unsafe { (*stmt).query as *mut pg_sys::SelectStmt };
    let from_clause = unsafe { (*query).fromClause };
    unsafe {
        let elements = (*from_clause).elements;
        for i in 0..(*from_clause).length {
            let element = (*elements.offset(i as isize)).ptr_value as *mut pg_sys::Node;

            match (*element).type_ {
                pg_sys::NodeTag::T_RangeVar => {
                    if !analyze_range_var(
                        element as *mut pg_sys::RangeVar,
                        current_schema.as_str(),
                    )? {
                        return Ok(true);
                    }
                }
                pg_sys::NodeTag::T_JoinExpr => {
                    if !analyze_join_expr(
                        element as *mut pg_sys::JoinExpr,
                        current_schema.as_str(),
                    )? {
                        return Ok(true);
                    }
                }
                _ => {
                    continue;
                }
            }
        }
    }

    // Push down the view creation query to DuckDB
    execute(query_string.to_str()?, [])?;
    Ok(true)
}

/// Analyze the RangeVar to check if the relation exists in DuckDB
fn analyze_range_var(rv: *mut pg_sys::RangeVar, current_schema: &str) -> Result<bool> {
    let relation_name = unsafe { CStr::from_ptr((*rv).relname).to_str()? };
    let schema_name = unsafe {
        if (*rv).schemaname.is_null() {
            current_schema
        } else {
            CStr::from_ptr((*rv).schemaname).to_str()?
        }
    };

    if !view_exists(relation_name, schema_name)? {
        fallback_warning!(format!(
            "{schema_name}.{relation_name} does not exist in DuckDB"
        ));
        Ok(false)
    } else {
        Ok(true)
    }
}

/// Analyze the join expression to check if the relations in the join expression exist in DuckDB
fn analyze_join_expr(join_expr: *mut pg_sys::JoinExpr, current_schema: &str) -> Result<bool> {
    unsafe {
        let ltree = (*join_expr).larg;
        let rtree = (*join_expr).rarg;

        Ok(analyze_tree(ltree, current_schema)? && analyze_tree(rtree, current_schema)?)
    }
}

/// Analyze the tree to check if the relations in the tree exist in DuckDB
fn analyze_tree(mut tree: *mut pg_sys::Node, current_schema: &str) -> Result<bool> {
    while !tree.is_null() {
        unsafe {
            match (*tree).type_ {
                pg_sys::NodeTag::T_RangeVar => {
                    return analyze_range_var(tree as *mut pg_sys::RangeVar, current_schema);
                }
                pg_sys::NodeTag::T_JoinExpr => {
                    tree = (*(tree as *mut pg_sys::JoinExpr)).larg;
                }
                _ => break,
            }
        }
    }
    Ok(true)
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

fn parse_query_from_utility_stmt(query_string: &core::ffi::CStr) -> Result<String> {
    let query_string = query_string.to_str()?;

    let dialect = PostgreSqlDialect {};
    let utility = Parser::parse_sql(&dialect, query_string)?;

    debug_assert!(utility.len() == 1);
    match &utility[0] {
        Statement::Explain {
            describe_alias: _,
            analyze: _,
            verbose: _,
            statement,
            format: _,
        } => Ok(statement.to_string()),
        _ => bail!("unexpected utility statement: {}", query_string),
    }
}
