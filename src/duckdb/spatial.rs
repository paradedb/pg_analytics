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
use strum::IntoEnumIterator;
use strum::{AsRefStr, EnumIter};

use crate::fdw::base::OptionValidator;

/// SpatialOption is an enum that represents the options that can be passed to the st_read function.
/// Reference https://github.com/duckdb/duckdb_spatial/blob/main/docs/functions.md#st_read
#[derive(EnumIter, AsRefStr, PartialEq, Debug)]
pub enum SpatialOption {
    #[strum(serialize = "files")]
    Files,
    #[strum(serialize = "cache")]
    Cache,
    #[strum(serialize = "sequential_layer_scan")]
    SequentialLayerScan,
    #[strum(serialize = "spatial_filter")]
    SpatialFilter,
    #[strum(serialize = "open_options")]
    OpenOptions,
    #[strum(serialize = "layer")]
    Layer,
    #[strum(serialize = "allowed_drivers")]
    AllowedDrivers,
    #[strum(serialize = "sibling_files")]
    SiblingFiles,
    #[strum(serialize = "spatial_filter_box")]
    SpatialFilterBox,
    #[strum(serialize = "keep_wkb")]
    KeepWkb,
}

impl OptionValidator for SpatialOption {
    fn is_required(&self) -> bool {
        match self {
            Self::Files => true,
            Self::Cache => false,
            Self::SequentialLayerScan => false,
            Self::SpatialFilter => false,
            Self::OpenOptions => false,
            Self::Layer => false,
            Self::AllowedDrivers => false,
            Self::SiblingFiles => false,
            Self::SpatialFilterBox => false,
            Self::KeepWkb => false,
        }
    }
}

pub fn create_duckdb_relation(
    table_name: &str,
    schema_name: &str,
    table_options: HashMap<String, String>,
) -> Result<String> {
    if !table_options.contains_key(SpatialOption::Files.as_ref()) {
        return Err(anyhow!("Files option is required"));
    }

    let spatial_options = SpatialOption::iter()
        .filter_map(|param| {
            let value = table_options.get(param.as_ref())?;
            Some(match param {
                SpatialOption::Files => format!("'{}'", value),
                _ => format!("{}={}", param.as_ref(), value),
            })
        })
        .collect::<Vec<String>>();

    let cache = table_options
        .get(SpatialOption::Cache.as_ref())
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let relation = if cache { "TABLE" } else { "VIEW" };

    Ok(format!(
        "CREATE {relation} IF NOT EXISTS {}.{} AS SELECT * FROM st_read({})",
        schema_name,
        table_name,
        spatial_options.join(", "),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    #[test]
    fn test_create_spatial_view() {
        let table_name = "test";
        let schema_name = "main";
        let table_options = HashMap::from([(
            SpatialOption::Files.as_ref().to_string(),
            "/data/spatial".to_string(),
        )]);

        let expected =
            "CREATE VIEW IF NOT EXISTS main.test AS SELECT * FROM st_read('/data/spatial')";
        let actual = create_duckdb_relation(table_name, schema_name, table_options).unwrap();

        assert_eq!(expected, actual);

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL spatial; LOAD spatial;")
            .unwrap();

        match conn.prepare(&actual) {
            Ok(_) => panic!("invalid spatial file should throw an error"),
            Err(e) => assert!(e.to_string().contains("data/spatial")),
        }
    }
}
