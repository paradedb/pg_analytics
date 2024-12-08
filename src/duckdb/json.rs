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
use strum::{AsRefStr, Display, EnumIter};

use crate::fdw::base::OptionValidator;

use super::utils;

#[derive(EnumIter, AsRefStr, PartialEq, Debug, Display)]
#[strum(serialize_all = "snake_case")]
pub enum JsonOption {
    AutoDetect,
    Columns,
    Compression,
    ConvertStringsToIntegers,
    Dateformat,
    Filename,
    Files,
    Format,
    HivePartitioning,
    IgnoreErrors,
    MaximumDepth,
    MaximumObjectSize,
    Records,
    SampleSize,
    Select,
    Timestampformat,
    UnionByName,
}

impl OptionValidator for JsonOption {
    fn is_required(&self) -> bool {
        matches!(self, Self::Files)
    }
}

pub fn create_view(
    table_name: &str,
    schema_name: &str,
    table_options: HashMap<String, String>,
) -> Result<String> {
    let files = Some(utils::format_csv(
        table_options
            .get(JsonOption::Files.as_ref())
            .ok_or_else(|| anyhow!("files option is required"))?,
    ));

    let create_json_str = vec![
        files,
        extract_option(JsonOption::AutoDetect, &table_options, false),
        extract_option(JsonOption::Columns, &table_options, false),
        extract_option(JsonOption::Compression, &table_options, true),
        extract_option(JsonOption::ConvertStringsToIntegers, &table_options, false),
        extract_option(JsonOption::Dateformat, &table_options, true),
        extract_option(JsonOption::Filename, &table_options, false),
        extract_option(JsonOption::Format, &table_options, true),
        extract_option(JsonOption::HivePartitioning, &table_options, false),
        extract_option(JsonOption::IgnoreErrors, &table_options, false),
        extract_option(JsonOption::MaximumDepth, &table_options, false),
        extract_option(JsonOption::MaximumObjectSize, &table_options, false),
        extract_option(JsonOption::Records, &table_options, false),
        extract_option(JsonOption::SampleSize, &table_options, false),
        extract_option(JsonOption::Timestampformat, &table_options, true),
        extract_option(JsonOption::UnionByName, &table_options, false),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<String>>()
    .join(", ");

    let default_select = "*".to_string();
    let select = table_options
        .get(JsonOption::Select.as_ref())
        .unwrap_or(&default_select);

    Ok(format!("CREATE VIEW IF NOT EXISTS {schema_name}.{table_name} AS SELECT {select} FROM read_json({create_json_str})"))
}

fn extract_option(
    option: JsonOption,
    table_options: &HashMap<String, String>,
    quote: bool,
) -> Option<String> {
    table_options.get(option.as_ref()).map(|res| match quote {
        true => format!("{option} = '{res}'"),
        false => format!("{option} = {res}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    #[test]
    fn test_create_json_view_basic() {
        let table_name = "json_test";
        let schema_name = "main";
        let table_options = HashMap::from([(
            JsonOption::Files.as_ref().to_string(),
            "/data/file1.json".to_string(),
        )]);

        let expected = "CREATE VIEW IF NOT EXISTS main.json_test AS SELECT * FROM read_json('/data/file1.json')";
        let actual = create_view(table_name, schema_name, table_options).unwrap();

        assert_eq!(expected, actual);

        let conn = Connection::open_in_memory().unwrap();
        match conn.prepare(&actual) {
            Ok(_) => panic!("invalid json file should throw an error"),
            Err(e) => assert!(e.to_string().contains("file1.json")),
        }
    }

    #[test]
    fn test_create_json_view_with_options() {
        let table_name = "json_test";
        let schema_name = "main";
        let table_options = HashMap::from([
            (
                JsonOption::Files.to_string(),
                "/data/file1.json, /data/file2.json".to_string(),
            ),
            (
                JsonOption::Columns.to_string(),
                "{'key1': 'INTEGER', 'key2': 'VARCHAR'}".to_string(),
            ),
            (
                JsonOption::Compression.to_string(),
                "uncompressed".to_string(),
            ),
            (
                JsonOption::ConvertStringsToIntegers.to_string(),
                "false".to_string(),
            ),
            (JsonOption::Dateformat.to_string(), "%d/%m/%Y".to_string()),
            (JsonOption::Filename.to_string(), "true".to_string()),
            (JsonOption::Format.to_string(), "array".to_string()),
            (
                JsonOption::HivePartitioning.to_string(),
                "false".to_string(),
            ),
            (JsonOption::IgnoreErrors.to_string(), "true".to_string()),
            (JsonOption::MaximumDepth.to_string(), "4096".to_string()),
            (
                JsonOption::MaximumObjectSize.to_string(),
                "65536".to_string(),
            ),
            (JsonOption::Records.to_string(), "auto".to_string()),
            (JsonOption::SampleSize.to_string(), "-1".to_string()),
            (JsonOption::Select.to_string(), "key1".to_string()),
            (
                JsonOption::Timestampformat.to_string(),
                "yyyy-MM-dd".to_string(),
            ),
            (JsonOption::UnionByName.to_string(), "true".to_string()),
        ]);

        let expected = "CREATE VIEW IF NOT EXISTS main.json_test AS SELECT key1 FROM read_json(['/data/file1.json', '/data/file2.json'], columns = {'key1': 'INTEGER', 'key2': 'VARCHAR'}, compression = 'uncompressed', convert_strings_to_integers = false, dateformat = '%d/%m/%Y', filename = true, format = 'array', hive_partitioning = false, ignore_errors = true, maximum_depth = 4096, maximum_object_size = 65536, records = auto, sample_size = -1, timestampformat = 'yyyy-MM-dd', union_by_name = true)";
        let actual = create_view(table_name, schema_name, table_options).unwrap();

        assert_eq!(expected, actual);

        let conn = Connection::open_in_memory().unwrap();
        match conn.prepare(&actual) {
            Ok(_) => panic!("invalid json file should throw an error"),
            Err(e) => assert!(e.to_string().contains("file1.json")),
        }
    }
}
