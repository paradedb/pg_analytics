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
use std::collections::HashMap;
use strum::{AsRefStr, EnumIter};

use crate::fdw::base::OptionValidator;

#[derive(EnumIter, AsRefStr, PartialEq, Debug)]
pub enum IcebergOption {
    #[strum(serialize = "allow_moved_paths")]
    AllowMovedPaths,
    #[strum(serialize = "files")]
    Files,
    #[strum(serialize = "preserve_casing")]
    PreserveCasing,
    #[strum(serialize = "select")]
    Select,
}

impl OptionValidator for IcebergOption {
    fn is_required(&self) -> bool {
        match self {
            Self::AllowMovedPaths => false,
            Self::Files => true,
            Self::PreserveCasing => false,
            Self::Select => false,
        }
    }
}

pub fn create_view(
    table_name: &str,
    schema_name: &str,
    table_options: HashMap<String, String>,
) -> Result<String> {
    let files = Some(format!(
        "'{}'",
        table_options
            .get(IcebergOption::Files.as_ref())
            .ok_or_else(|| anyhow!("files option is required"))?
    ));

    let allow_moved_paths = table_options
        .get(IcebergOption::AllowMovedPaths.as_ref())
        .map(|option| format!("allow_moved_paths = {option}"));

    let create_iceberg_str = [files, allow_moved_paths]
        .into_iter()
        .flatten()
        .collect::<Vec<String>>()
        .join(", ");

    let default_select = "*".to_string();
    let select = table_options
        .get(IcebergOption::Select.as_ref())
        .unwrap_or(&default_select);

    Ok(format!("CREATE VIEW IF NOT EXISTS {schema_name}.{table_name} AS SELECT {select} FROM iceberg_scan({create_iceberg_str})"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    #[test]
    fn test_create_iceberg_view() {
        let table_name = "test";
        let schema_name = "main";
        let table_options = HashMap::from([(
            IcebergOption::Files.as_ref().to_string(),
            "/data/iceberg".to_string(),
        )]);

        let expected =
            "CREATE VIEW IF NOT EXISTS main.test AS SELECT * FROM iceberg_scan('/data/iceberg')";
        let actual = create_view(table_name, schema_name, table_options).unwrap();

        assert_eq!(expected, actual);

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL iceberg; LOAD iceberg;")
            .unwrap();

        match conn.prepare(&actual) {
            Ok(_) => panic!("invalid iceberg file should throw an error"),
            Err(e) => assert!(e.to_string().contains("/data/iceberg")),
        }
    }
}
