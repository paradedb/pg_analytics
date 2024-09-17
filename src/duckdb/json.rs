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
