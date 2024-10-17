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

use anyhow::Result;
use pg_sys::{get_relname_relid, RangeVarGetCreationNamespace};

use pgrx::*;

use crate::{duckdb::connection::execute, hooks::query::is_duckdb_query};

use super::set_search_path_by_pg;

pub fn view_query(query_string: &core::ffi::CStr, stmt: *mut pg_sys::ViewStmt) -> Result<bool> {
    let query = unsafe { (*stmt).query as *mut pg_sys::SelectStmt };
    let from_clause = unsafe { (*query).fromClause };

    if analyze_from_clause(from_clause)? {
        // Push down the view creation query to DuckDB
        set_search_path_by_pg()?;
        execute(query_string.to_str()?, [])?;
    }

    Ok(true)
}

/// Analyze the from clause to find the RangeVar node to check if it's a DuckDB query
fn analyze_from_clause(from_clause: *mut pg_sys::List) -> Result<bool> {
    unsafe {
        let elements = (*from_clause).elements;
        for i in 0..(*from_clause).length {
            let element = (*elements.offset(i as isize)).ptr_value as *mut pg_sys::Node;

            match (*element).type_ {
                pg_sys::NodeTag::T_RangeVar => {
                    return analyze_range_var(element as *mut pg_sys::RangeVar);
                }
                pg_sys::NodeTag::T_JoinExpr => {
                    return analyze_join_expr(element as *mut pg_sys::JoinExpr);
                }
                _ => continue,
            }
        }
    }
    Ok(false)
}

/// Check if the RangeVar is a DuckDB query
fn analyze_range_var(rv: *mut pg_sys::RangeVar) -> Result<bool> {
    let (pg_relation, rel_name) = unsafe {
        let schema_id = RangeVarGetCreationNamespace(rv);
        let relid = get_relname_relid((*rv).relname, schema_id);

        let relation = pg_sys::RelationIdGetRelation(relid);
        (
            PgRelation::from_pg_owned(relation),
            CStr::from_ptr((*rv).relname),
        )
    };

    if is_duckdb_query(&[pg_relation]) {
        Ok(true)
    } else {
        fallback_warning!(format!(
            "{} is not a foreign table from DuckDB",
            rel_name.to_string_lossy()
        ));
        Ok(false)
    }
}

/// Check if the JoinExpr is a DuckDB query
fn analyze_join_expr(join_expr: *mut pg_sys::JoinExpr) -> Result<bool> {
    unsafe {
        let ltree = (*join_expr).larg;
        let rtree = (*join_expr).rarg;

        Ok(analyze_tree(ltree)? && analyze_tree(rtree)?)
    }
}

/// Analyze the tree recursively to find the RangeVar node to check if it's a DuckDB query
fn analyze_tree(mut tree: *mut pg_sys::Node) -> Result<bool> {
    while !tree.is_null() {
        unsafe {
            match (*tree).type_ {
                pg_sys::NodeTag::T_RangeVar => {
                    return analyze_range_var(tree as *mut pg_sys::RangeVar);
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
