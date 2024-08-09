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

#[derive(PartialEq)]
pub enum SpatialOption {
    Files,
    SequentialLayerScan,
    SpatialFilter,
    OpenOptions,
    Layer,
    AllowedDrivers,
    SiblingFiles,
    SpatialFilterBox,
    KeepWkb,
}

impl SpatialOption {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Files => "files",
            Self::SequentialLayerScan => "sequential_layer_scan",
            Self::SpatialFilter => "spatial_filter",
            Self::OpenOptions => "open_options",
            Self::Layer => "layer",
            Self::AllowedDrivers => "allowed_drivers",
            Self::SiblingFiles => "sibling_files",
            Self::SpatialFilterBox => "spatial_filter_box",
            Self::KeepWkb => "keep_wkb",
        }
    }

    pub fn is_required(&self) -> bool {
        match self {
            Self::Files => true,
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

    pub fn iter() -> impl Iterator<Item = Self> {
        [
            Self::Files,
            Self::SequentialLayerScan,
            Self::SpatialFilter,
            Self::OpenOptions,
            Self::Layer,
            Self::AllowedDrivers,
            Self::SiblingFiles,
            Self::SpatialFilterBox,
            Self::KeepWkb,
        ]
        .into_iter()
    }
}

pub fn create_view(
    table_name: &str,
    schema_name: &str,
    table_options: HashMap<String, String>,
) -> Result<String> {
    if table_options.get(SpatialOption::Files.as_str()).is_none() {
        return Err(anyhow!("Files option is required"));
    }

    let spatial_options = SpatialOption::iter()
        .filter_map(|param| {
            let value = table_options.get(param.as_str())?;
            Some(match param {
                SpatialOption::Files => format!("'{}'", value),
                _ => format!("{}={}", param.as_str(), value),
            })
        })
        .collect::<Vec<String>>();

    Ok(format!(
        "CREATE VIEW IF NOT EXISTS {}.{} AS SELECT * FROM st_read({})",
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
            SpatialOption::Files.as_str().to_string(),
            "/data/spatial".to_string(),
        )]);

        let expected =
            "CREATE VIEW IF NOT EXISTS main.test AS SELECT * FROM st_read('/data/spatial')";
        let actual = create_view(table_name, schema_name, table_options).unwrap();

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
