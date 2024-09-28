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

use anyhow::Result;
use pgrx::*;
use std::ffi::CStr;
use std::str::Utf8Error;

use crate::duckdb::connection;
use crate::fdw::handler::FdwHandler;

pub fn get_current_query(
    planned_stmt: *mut pg_sys::PlannedStmt,
    query_string: &CStr,
) -> Result<String, Utf8Error> {
    let query_start_index = unsafe { (*planned_stmt).stmt_location };
    let query_len = unsafe { (*planned_stmt).stmt_len };
    let full_query = query_string.to_str()?;

    let current_query = if query_start_index != -1 {
        if query_len == 0 {
            full_query[(query_start_index as usize)..full_query.len()].to_string()
        } else {
            full_query[(query_start_index as usize)..((query_start_index + query_len) as usize)]
                .to_string()
        }
    } else {
        full_query.to_string()
    };

    Ok(current_query)
}

pub fn get_query_relations(rtable: *mut pg_sys::List) -> Vec<PgRelation> {
    let mut relations = Vec::new();

    unsafe {
        if rtable.is_null() {
            return relations;
        }

        let elements = (*rtable).elements;

        for i in 0..(*rtable).length {
            let rte = (*elements.offset(i as isize)).ptr_value as *mut pg_sys::RangeTblEntry;

            if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
                continue;
            }
            let relation = pg_sys::RelationIdGetRelation((*rte).relid);
            let pg_relation = PgRelation::from_pg_owned(relation);
            relations.push(pg_relation);
        }
    }

    relations
}

pub fn set_search_path_by_pg() -> Result<()> {
    let mut search_path = get_postgres_search_path();
    let duckdb_schemas = connection::get_available_schemas()?;

    // Filter schemas. If one of schemas doesn't exist, it will cause the DuckDB 'SET search_path' to fail.
    search_path.retain(|schema| duckdb_schemas.contains(schema));

    // Set duckdb catalog search path
    connection::set_search_path(search_path)?;

    Ok(())
}

fn get_postgres_search_path() -> Vec<String> {
    let active_schemas =
        unsafe { PgList::<pg_sys::Oid>::from_pg(pg_sys::fetch_search_path(false)) };

    let mut schema_vec: Vec<String> = Vec::with_capacity(active_schemas.len());
    for schema_oid in active_schemas.iter_oid() {
        let tuple = unsafe {
            pg_sys::SearchSysCache1(
                pg_sys::SysCacheIdentifier::NAMESPACEOID as i32,
                schema_oid.into_datum().unwrap(),
            )
        };

        if !tuple.is_null() {
            let pg_namespace = unsafe { pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_namespace };
            let name = pg_sys::name_data_to_str(unsafe { &(*pg_namespace).nspname });
            schema_vec.push(name.to_string());

            unsafe { pg_sys::ReleaseSysCache(tuple) };
        }
    }

    schema_vec
}

pub fn is_duckdb_query(relations: &[PgRelation]) -> bool {
    !relations.is_empty()
        && relations.iter().all(|pg_relation| {
            if pg_relation.is_foreign_table() {
                let foreign_table = unsafe { pg_sys::GetForeignTable(pg_relation.oid()) };
                let foreign_server = unsafe { pg_sys::GetForeignServer((*foreign_table).serverid) };
                let fdw_handler = FdwHandler::from(foreign_server);
                fdw_handler != FdwHandler::Other
            } else {
                false
            }
        })
}
