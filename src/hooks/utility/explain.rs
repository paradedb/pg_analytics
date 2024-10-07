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

use std::ffi::{CStr, CString};
use std::time::Instant;

use anyhow::Result;
use pgrx::{error, pg_sys};

use super::parse_query_from_utility_stmt;
use crate::{
    duckdb::connection,
    hooks::query::{get_query_relations, is_duckdb_query, set_search_path_by_pg},
};

enum Style {
    Postgres,
    Duckdb,
}
struct ExplainState {
    analyze: bool,
    style: Style,
}

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

    let state = parse_explain_options(unsafe { (*stmt).options });
    let query = parse_query_from_utility_stmt(query_string)?;

    let output = match state.style {
        Style::Postgres => {
            let mut output = format!("DuckDB Scan: {}\n", query);
            if state.analyze {
                let start_time = Instant::now();
                set_search_path_by_pg()?;
                connection::execute(&query, [])?;
                let duration = start_time.elapsed();
                output += &format!(
                    "Execution Time: {:.3} ms\n",
                    duration.as_micros() as f64 / 1_000.0
                );
            }
            output
        }
        Style::Duckdb => {
            set_search_path_by_pg()?;
            let explain_query = if state.analyze {
                format!("EXPLAIN ANALYZE {query}")
            } else {
                format!("EXPLAIN {query}")
            };
            connection::execute_explain(&explain_query)?
        }
    };

    unsafe {
        let tstate = pg_sys::begin_tup_output_tupdesc(
            dest,
            pg_sys::ExplainResultDesc(stmt),
            &pg_sys::TTSOpsVirtual,
        );

        let output_cstr = CString::new(output)?;

        pg_sys::do_text_output_multiline(tstate, output_cstr.as_ptr());
        pg_sys::end_tup_output(tstate);
    }

    Ok(false)
}

fn parse_explain_options(options: *const pg_sys::List) -> ExplainState {
    let mut explain_state = ExplainState {
        analyze: false,
        style: Style::Postgres,
    };

    if options.is_null() {
        return explain_state;
    }

    unsafe {
        let elements = (*options).elements;

        for i in 0..(*options).length as isize {
            let opt = (*elements.offset(i)).ptr_value as *mut pg_sys::DefElem;

            let opt_name = match CStr::from_ptr((*opt).defname).to_str() {
                Ok(opt) => opt,
                Err(e) => {
                    error!("failed to parse EXPLAIN option name: {e}");
                }
            };
            match opt_name {
                "analyze" => {
                    explain_state.analyze = pg_sys::defGetBoolean(opt);
                }
                "style" => {
                    let style = match CStr::from_ptr(pg_sys::defGetString(opt)).to_str() {
                        Ok(style) => style,

                        Err(e) => {
                            error!("failed to parse STYLE option: {e}");
                        }
                    };

                    explain_state.style = match parse_explain_style(style) {
                        Some(s) => s,
                        None => {
                            error!("unrecognized STYLE option: {style}")
                        }
                    };
                }
                _ => error!("unrecognized EXPLAIN option \"{opt_name}\""),
            }
        }
    }

    explain_state
}

fn parse_explain_style(style: &str) -> Option<Style> {
    match style {
        "pg" => Some(Style::Postgres),
        "postgres" => Some(Style::Postgres),
        "duckdb" => Some(Style::Duckdb),
        _ => None,
    }
}
