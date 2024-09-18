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
pub enum JsonOption {
    #[strum(serialize = "auto_detect")]
    AutoDetect,
    #[strum(serialize = "columns")]
    Columns,
    #[strum(serialize = "compression")]
    Compression,
    #[strum(serialize = "convert_strings_to_integers")]
    ConvertStringsToIntegers,
    #[strum(serialize = "dateformat")]
    Dateformat,
    #[strum(serialize = "filename")]
    Filename,
    #[strum(serialize = "files")]
    Files,
    #[strum(serialize = "format")]
    Format,
    #[strum(serialize = "hive_partitioning")]
    HivePartitioning,
    #[strum(serialize = "ignore_errors")]
    IgnoreErrors,
    #[strum(serialize = "maximum_depth")]
    MaximumDepth,
    #[strum(serialize = "maximum_object_size")]
    MaximumObjectSize,
    #[strum(serialize = "records")]
    Records,
    #[strum(serialize = "sample_size")]
    SampleSize,
    #[strum(serialize = "select")]
    Select,
    #[strum(serialize = "timestampformat")]
    Timestampformat,
    #[strum(serialize = "union_by_name")]
    UnionByName,
}

impl OptionValidator for JsonOption {
    fn is_required(&self) -> bool {
        return match self {
            Self::Files => true,
            _ => false,
        };
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
        extract_option(JsonOption::AutoDetect, &table_options),
        extract_option(JsonOption::Columns, &table_options),
        extract_option(JsonOption::Compression, &table_options),
        extract_option(JsonOption::ConvertStringsToIntegers, &table_options),
        extract_option(JsonOption::Dateformat, &table_options),
        extract_option(JsonOption::Filename, &table_options),
        extract_option(JsonOption::Format, &table_options),
        extract_option(JsonOption::HivePartitioning, &table_options),
        extract_option(JsonOption::IgnoreErrors, &table_options),
        extract_option(JsonOption::IgnoreErrors, &table_options),
        extract_option(JsonOption::MaximumDepth, &table_options),
        extract_option(JsonOption::MaximumObjectSize, &table_options),
        extract_option(JsonOption::Records, &table_options),
        extract_option(JsonOption::SampleSize, &table_options),
        extract_option(JsonOption::Timestampformat, &table_options),
        extract_option(JsonOption::UnionByName, &table_options),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<String>>()
    .join(", ");

    let default_select = "*".to_string();
    let select = extract_option(JsonOption::Select, &table_options).unwrap_or(default_select);

    Ok(format!("CREATE VIEW IF NOT EXISTS {schema_name}.{table_name} AS SELECT {select} FROM read_json({create_json_str})"))
}

fn extract_option(option: JsonOption, table_options: &HashMap<String, String>) -> Option<String> {
    return table_options
        .get(option.as_ref())
        .map(|res| format!("{option} = {res}"));
}
