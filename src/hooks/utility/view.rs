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

use anyhow::{bail, Result};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
use std::ops::ControlFlow;

use pgrx::*;
use sqlparser::ast::visit_relations;

use crate::duckdb::connection::{execute, view_exists};

use super::{get_postgres_current_schema, set_search_path_by_pg};

pub fn view_query(query_string: &core::ffi::CStr) -> Result<bool> {
    // Use the current scheme if the schema is not provided in the query.
    let current_schema = get_postgres_current_schema();
    // Set DuckDB search path according search path in Postgres
    set_search_path_by_pg()?;

    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, query_string.to_str()?)?;
    // visit statements, capturing relations (table names)
    let mut visited = vec![];

    visit_relations(&statements, |relation| {
        visited.push(relation.clone());
        ControlFlow::<()>::Continue(())
    });

    for relation in visited.iter() {
        let (schema_name, relation_name) = if relation.0.len() == 1 {
            (current_schema.clone(), relation.0[0].to_string())
        } else if relation.0.len() == 2 {
            (relation.0[0].to_string(), relation.0[1].to_string())
        } else if relation.0.len() == 3 {
            // pg_analytics does not create view with database name now
            error!(
                "pg_analytics does not support creating view with database name: {}",
                relation.0[0].to_string()
            );
        } else {
            bail!("unexpected relation name: {:?}", relation.0);
        };

        // If the table does not exist in DuckDB, do not push down the query to DuckDB
        if !view_exists(&relation_name, &schema_name)? {
            fallback_warning!(format!(
                "{schema_name}.{relation_name} does not exist in DuckDB"
            ));
            return Ok(true);
        }
    }
    // Push down the view creation query to DuckDB
    execute(query_string.to_str()?, [])?;
    Ok(true)
}
