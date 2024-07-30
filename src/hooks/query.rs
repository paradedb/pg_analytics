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

use pgrx::*;
use std::ffi::CStr;
use std::str::Utf8Error;

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

pub fn get_query_relations(planned_stmt: *mut pg_sys::PlannedStmt) -> Vec<PgRelation> {
    let mut relations = Vec::new();

    unsafe {
        let rtable = (*planned_stmt).rtable;

        if rtable.is_null() {
            return relations;
        }

        #[cfg(feature = "pg12")]
        let mut current_cell = (*rtable).head;
        #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
        let elements = (*rtable).elements;

        for i in 0..(*rtable).length {
            let rte: *mut pg_sys::RangeTblEntry;
            #[cfg(feature = "pg12")]
            {
                rte = (*current_cell).data.ptr_value as *mut pg_sys::RangeTblEntry;
                current_cell = (*current_cell).next;
            }
            #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
            {
                rte = (*elements.offset(i as isize)).ptr_value as *mut pg_sys::RangeTblEntry;
            }

            if (*rte).rtekind != pg_sys::RTEKind_RTE_RELATION {
                continue;
            }
            let relation = pg_sys::RelationIdGetRelation((*rte).relid);
            let pg_relation = PgRelation::from_pg_owned(relation);
            relations.push(pg_relation);
        }
    }

    relations
}
