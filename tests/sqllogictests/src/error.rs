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

use datafusion::arrow::error::ArrowError;
use sqllogictest::TestError;
use sqlparser::parser::ParserError;
use thiserror::Error;

pub type Result<T, E = DFSqlLogicTestError> = std::result::Result<T, E>;

/// DataFusion sql-logicaltest error
#[derive(Debug, Error)]
pub enum DFSqlLogicTestError {
    /// Error from sqlx
    #[error("Postgres error(from sqlx crate): {0}")]
    Sqlx(#[from] sqlx::Error),
    /// Error from sqllogictest-rs
    #[error("SqlLogicTest error(from sqllogictest-rs crate): {0}")]
    SqlLogicTest(#[from] TestError),
    /// Error returned when SQL is syntactically incorrect.
    #[error("SQL Parser error: {0}")]
    Sql(#[from] ParserError),
    /// Error from arrow-rs
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    /// Generic error
    #[error("Other Error: {0}")]
    Other(String),
}

impl From<String> for DFSqlLogicTestError {
    fn from(value: String) -> Self {
        DFSqlLogicTestError::Other(value)
    }
}
