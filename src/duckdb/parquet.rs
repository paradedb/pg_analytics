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

use super::utils;

#[derive(EnumIter, AsRefStr, PartialEq, Debug)]
pub enum ParquetOption {
    #[strum(serialize = "binary_as_string")]
    BinaryAsString,
    #[strum(serialize = "cache")]
    Cache,
    #[strum(serialize = "file_name")]
    FileName,
    #[strum(serialize = "file_row_number")]
    FileRowNumber,
    #[strum(serialize = "files")]
    Files,
    #[strum(serialize = "hive_partitioning")]
    HivePartitioning,
    #[strum(serialize = "hive_types")]
    HiveTypes,
    #[strum(serialize = "hive_types_autocast")]
    HiveTypesAutocast,
    #[strum(serialize = "preserve_casing")]
    PreserveCasing,
    #[strum(serialize = "union_by_name")]
    UnionByName,
    #[strum(serialize = "select")]
    Select,
    // TODO: EncryptionConfig
}

impl OptionValidator for ParquetOption {
    fn is_required(&self) -> bool {
        match self {
            Self::BinaryAsString => false,
            Self::Cache => false,
            Self::FileName => false,
            Self::FileRowNumber => false,
            Self::Files => true,
            Self::HivePartitioning => false,
            Self::HiveTypes => false,
            Self::HiveTypesAutocast => false,
            Self::PreserveCasing => false,
            Self::UnionByName => false,
            Self::Select => false,
        }
    }
}

pub fn create_duckdb_relation(
    table_name: &str,
    schema_name: &str,
    table_options: HashMap<String, String>,
) -> Result<String> {
    let files = Some(utils::format_csv(
        table_options
            .get(ParquetOption::Files.as_ref())
            .ok_or_else(|| anyhow!("files option is required"))?,
    ));

    let binary_as_string = table_options
        .get(ParquetOption::BinaryAsString.as_ref())
        .map(|option| format!("binary_as_string = {option}"));

    let file_name = table_options
        .get(ParquetOption::FileName.as_ref())
        .map(|option| format!("filename = {option}"));

    let file_row_number = table_options
        .get(ParquetOption::FileRowNumber.as_ref())
        .map(|option| format!("file_row_number = {option}"));

    let hive_partitioning = table_options
        .get(ParquetOption::HivePartitioning.as_ref())
        .map(|option| format!("hive_partitioning = {option}"));

    let hive_types = table_options
        .get(ParquetOption::HiveTypes.as_ref())
        .map(|option| format!("hive_types = {option}"));

    let hive_types_autocast = table_options
        .get(ParquetOption::HiveTypesAutocast.as_ref())
        .map(|option| format!("hive_types_autocast = {option}"));

    let union_by_name = table_options
        .get(ParquetOption::UnionByName.as_ref())
        .map(|option| format!("union_by_name = {option}"));

    let create_parquet_str = [
        files,
        binary_as_string,
        file_name,
        file_row_number,
        hive_partitioning,
        hive_types,
        hive_types_autocast,
        union_by_name,
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<String>>()
    .join(", ");

    let cache = table_options
        .get(ParquetOption::Cache.as_ref())
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let relation = if cache { "TABLE" } else { "VIEW" };

    Ok(format!("CREATE {relation} IF NOT EXISTS {schema_name}.{table_name} AS SELECT * FROM read_parquet({create_parquet_str})"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    #[test]
    fn test_create_parquet_relation_single_file() {
        let table_name = "test";
        let schema_name = "main";
        let files = "/data/file.parquet";
        let table_options =
            HashMap::from([(ParquetOption::Files.as_ref().to_string(), files.to_string())]);
        let expected = "CREATE VIEW IF NOT EXISTS main.test AS SELECT * FROM read_parquet('/data/file.parquet')";
        let actual = create_duckdb_relation(table_name, schema_name, table_options).unwrap();

        assert_eq!(expected, actual);

        let conn = Connection::open_in_memory().unwrap();
        match conn.prepare(&actual) {
            Ok(_) => panic!("invalid parquet file should throw an error"),
            Err(e) => assert!(e.to_string().contains("file.parquet")),
        }
    }

    #[test]
    fn test_create_parquet_relation_multiple_files() {
        let table_name = "test";
        let schema_name = "main";
        let files = "/data/file1.parquet, /data/file2.parquet";
        let table_options =
            HashMap::from([(ParquetOption::Files.as_ref().to_string(), files.to_string())]);

        let expected = "CREATE VIEW IF NOT EXISTS main.test AS SELECT * FROM read_parquet(['/data/file1.parquet', '/data/file2.parquet'])";
        let actual = create_duckdb_relation(table_name, schema_name, table_options).unwrap();

        assert_eq!(expected, actual);

        let conn = Connection::open_in_memory().unwrap();
        match conn.prepare(&actual) {
            Ok(_) => panic!("invalid parquet file should throw an error"),
            Err(e) => assert!(e.to_string().contains("file1.parquet")),
        }
    }

    #[test]
    fn test_create_parquet_relation_with_options() {
        let table_name = "test";
        let schema_name = "main";
        let table_options = HashMap::from([
            (
                ParquetOption::Files.as_ref().to_string(),
                "/data/file.parquet".to_string(),
            ),
            (
                ParquetOption::BinaryAsString.as_ref().to_string(),
                "true".to_string(),
            ),
            (
                ParquetOption::FileName.as_ref().to_string(),
                "false".to_string(),
            ),
            (
                ParquetOption::FileRowNumber.as_ref().to_string(),
                "true".to_string(),
            ),
            (
                ParquetOption::HivePartitioning.as_ref().to_string(),
                "true".to_string(),
            ),
            (
                ParquetOption::HiveTypes.as_ref().to_string(),
                "{'release': DATE, 'orders': BIGINT}".to_string(),
            ),
            (
                ParquetOption::HiveTypesAutocast.as_ref().to_string(),
                "true".to_string(),
            ),
            (
                ParquetOption::UnionByName.as_ref().to_string(),
                "true".to_string(),
            ),
        ]);

        let expected = "CREATE VIEW IF NOT EXISTS main.test AS SELECT * FROM read_parquet('/data/file.parquet', binary_as_string = true, filename = false, file_row_number = true, hive_partitioning = true, hive_types = {'release': DATE, 'orders': BIGINT}, hive_types_autocast = true, union_by_name = true)";
        let actual = create_duckdb_relation(table_name, schema_name, table_options).unwrap();

        assert_eq!(expected, actual);

        let conn = Connection::open_in_memory().unwrap();
        match conn.prepare(expected) {
            Ok(_) => panic!("invalid parquet file should throw an error"),
            Err(e) => assert!(e.to_string().contains("file.parquet")),
        }
    }
}
