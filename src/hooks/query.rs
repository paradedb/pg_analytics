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

use anyhow::{anyhow, Result};
use duckdb::arrow::array::RecordBatch;
use pgrx::*;
use std::ffi::CStr;
use std::str::Utf8Error;

use crate::duckdb::connection;
use crate::fdw::handler::FdwHandler;
use crate::schema::cell::*;

macro_rules! fallback_warning {
    ($msg:expr) => {
        warning!("This query was not fully pushed down to DuckDB because DuckDB returned an error. Query times may be impacted. If you would like to see this query pushed down, please submit a request to https://github.com/paradedb/pg_analytics/issues with the following context:\n{}", $msg);
    };
}

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

pub fn get_postgres_current_schema() -> String {
    let active_schemas =
        unsafe { PgList::<pg_sys::Oid>::from_pg(pg_sys::fetch_search_path(false)) };

    let schema_oid = active_schemas.get_oid(0).unwrap();

    let mut current_schema = String::new();
    let tuple = unsafe {
        pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::NAMESPACEOID as i32,
            schema_oid.into_datum().unwrap(),
        )
    };

    if !tuple.is_null() {
        let pg_namespace = unsafe { pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_namespace };
        let name = pg_sys::name_data_to_str(unsafe { &(*pg_namespace).nspname });
        current_schema = name.to_string();

        unsafe { pg_sys::ReleaseSysCache(tuple) };
    }
    current_schema
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

#[inline]
pub fn write_batches_to_slots<T: WhoAllocated>(
    query_desc: PgBox<pg_sys::QueryDesc, T>,
    mut batches: Vec<RecordBatch>,
) -> Result<()> {
    // Convert the DataFusion batches to Postgres tuples and send them to the destination
    unsafe {
        let tuple_desc = PgTupleDesc::from_pg(query_desc.tupDesc);
        let estate = query_desc.estate;
        (*estate).es_processed = 0;

        let dest = query_desc.dest;
        let startup = (*dest)
            .rStartup
            .ok_or_else(|| anyhow!("rStartup not found"))?;
        startup(dest, query_desc.operation as i32, query_desc.tupDesc);

        let receive = (*dest)
            .receiveSlot
            .ok_or_else(|| anyhow!("receiveSlot not found"))?;

        for batch in batches.iter_mut() {
            for row_index in 0..batch.num_rows() {
                let tuple_table_slot =
                    pg_sys::MakeTupleTableSlot(query_desc.tupDesc, &pg_sys::TTSOpsVirtual);

                pg_sys::ExecStoreVirtualTuple(tuple_table_slot);

                for (col_index, _) in tuple_desc.iter().enumerate() {
                    let attribute = tuple_desc
                        .get(col_index)
                        .ok_or_else(|| anyhow!("attribute at {col_index} not found in tupdesc"))?;
                    let column = batch.column(col_index);
                    let tts_value = (*tuple_table_slot).tts_values.add(col_index);
                    let tts_isnull = (*tuple_table_slot).tts_isnull.add(col_index);

                    match column.get_cell(row_index, attribute.atttypid, attribute.name())? {
                        Some(cell) => {
                            if let Some(datum) = cell.into_datum() {
                                *tts_value = datum;
                            }
                        }
                        None => {
                            *tts_isnull = true;
                        }
                    };
                }

                receive(tuple_table_slot, dest);
                (*estate).es_processed += 1;
                pg_sys::ExecDropSingleTupleTableSlot(tuple_table_slot);
            }
        }

        let shutdown = (*dest)
            .rShutdown
            .ok_or_else(|| anyhow!("rShutdown not found"))?;
        shutdown(dest);
    }

    Ok(())
}
