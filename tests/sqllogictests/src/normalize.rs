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

use crate::conversion::{
    big_decimal_to_str, bool_to_str, f32_to_str, f64_to_str, varchar_to_str, NULL_STR,
};
use crate::output::DFColumnType;
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use sqlx::postgres::{PgColumn, PgRow};
use sqlx::TypeInfo;
use sqlx::{Column, Row};

pub(crate) fn convert_rows(rows: &[PgRow]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.columns()
                .iter()
                .enumerate()
                .map(|(idx, column)| cell_to_string(row, column, idx))
                .collect::<Vec<String>>()
        })
        .collect::<Vec<_>>()
}

macro_rules! make_string {
    ($row:ident, $idx:ident, $t:ty) => {{
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => value.to_string(),
            None => NULL_STR.to_string(),
        }
    }};
    ($row:ident, $idx:ident, $t:ty, $convert:ident) => {{
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => $convert(value).to_string(),
            None => NULL_STR.to_string(),
        }
    }};
}

fn cell_to_string(row: &PgRow, column: &PgColumn, idx: usize) -> String {
    match column.type_info().name() {
        "CHAR" => make_string!(row, idx, i8),
        "BOOL" => make_string!(row, idx, bool, bool_to_str),
        "INT2" => make_string!(row, idx, i16),
        "INT4" => make_string!(row, idx, i32),
        "INT8" => make_string!(row, idx, i64),
        "FLOAT4" => make_string!(row, idx, f32, f32_to_str),
        "FLOAT8" => make_string!(row, idx, f64, f64_to_str),
        "NUMERIC" => make_string!(row, idx, BigDecimal, big_decimal_to_str),
        "BPCHAR" | "VARCHAR" | "TEXT" => make_string!(row, idx, &str, varchar_to_str),
        "DATE" => make_string!(row, idx, chrono::NaiveDate),
        "TIME" => make_string!(row, idx, chrono::NaiveTime),
        "TIMESTAMP" => {
            let value: Option<NaiveDateTime> = row.get(idx);
            value
                .map(|d| format!("{d:?}"))
                .unwrap_or_else(|| "NULL".to_string())
        }
        name => unimplemented!("Unsupported type: {}", name),
    }
}

pub(crate) fn convert_types(columns: &[PgColumn]) -> Vec<DFColumnType> {
    columns
        .iter()
        .map(|t| match t.type_info().name() {
            "BOOL" => DFColumnType::Boolean,
            "INT2" | "INT4" | "INT8" => DFColumnType::Integer,
            "BPCHAR" | "VARCHAR" | "TEXT" => DFColumnType::Text,
            "FLOAT4" | "FLOAT8" | "NUMERIC" => DFColumnType::Float,
            "DATE" | "TIME" => DFColumnType::DateTime,
            "TIMESTAMP" => DFColumnType::Timestamp,
            _ => DFColumnType::Another,
        })
        .collect()
}
