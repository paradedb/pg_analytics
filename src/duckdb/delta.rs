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

use crate::fdw::base::OptionValidator;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use strum::{AsRefStr, EnumIter};

#[derive(EnumIter, AsRefStr, PartialEq, Debug)]
pub enum DeltaOption {
    #[strum(serialize = "cache")]
    Cache,
    #[strum(serialize = "files")]
    Files,
    #[strum(serialize = "preserve_casing")]
    PreserveCasing,
    #[strum(serialize = "select")]
    Select,
}

impl OptionValidator for DeltaOption {
    fn is_required(&self) -> bool {
        match self {
            Self::Cache => false,
            Self::Files => true,
            Self::PreserveCasing => false,
            Self::Select => false,
        }
    }
}

pub fn create_duckdb_relation(
    table_name: &str,
    schema_name: &str,
    table_options: HashMap<String, String>,
) -> Result<String> {
    let files = format!(
        "'{}'",
        table_options
            .get(DeltaOption::Files.as_ref())
            .ok_or_else(|| anyhow!("files option is required"))?
    );

    let cache = table_options
        .get(DeltaOption::Cache.as_ref())
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let relation = if cache { "TABLE" } else { "VIEW" };

    Ok(format!(
        "CREATE {relation} IF NOT EXISTS {schema_name}.{table_name} AS SELECT * FROM delta_scan({files})"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    #[test]
    fn test_create_delta_relation() {
        let table_name = "test";
        let schema_name = "main";
        let table_options = HashMap::from([(
            DeltaOption::Files.as_ref().to_string(),
            "/data/delta".to_string(),
        )]);

        let expected =
            "CREATE VIEW IF NOT EXISTS main.test AS SELECT * FROM delta_scan('/data/delta')";
        let actual = create_duckdb_relation(table_name, schema_name, table_options).unwrap();

        assert_eq!(expected, actual);

        let conn = Connection::open_in_memory().unwrap();
        match conn.prepare(&actual) {
            Ok(_) => panic!("invalid delta file should throw an error"),
            Err(e) => assert!(e.to_string().contains("/data/delta")),
        }
    }
}
