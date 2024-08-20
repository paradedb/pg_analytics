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
use anyhow::Result;
use datafusion::prelude::*;
use prettytable::{format, Cell, Row, Table};
use std::fmt::{Debug, Display};

pub trait Printable: Debug {
    fn to_row(&self) -> Vec<String>;
}

macro_rules! impl_printable_for_tuple {
    ($($T:ident),+) => {
        impl<$($T),+> Printable for ($($T,)+)
        where
            $($T: Debug + Display,)+
        {
            #[allow(non_snake_case)]
            fn to_row(&self) -> Vec<String> {
                let ($($T,)+) = self;
                vec![$($T.to_string(),)+]
            }
        }
    }
}

// Implement Printable for tuples up to 12 elements
impl_printable_for_tuple!(T1);
impl_printable_for_tuple!(T1, T2);
impl_printable_for_tuple!(T1, T2, T3);
// impl_printable_for_tuple!(T1, T2, T3, T4);
impl_printable_for_tuple!(T1, T2, T3, T4, T5);
impl_printable_for_tuple!(T1, T2, T3, T4, T5, T6);
impl_printable_for_tuple!(T1, T2, T3, T4, T5, T6, T7);
impl_printable_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_printable_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_printable_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_printable_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_printable_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

// Special implementation for (i32, i32, i64, Vec<i64>)
impl Printable for (i32, i32, i64, Vec<i64>) {
    fn to_row(&self) -> Vec<String> {
        vec![
            self.0.to_string(),
            self.1.to_string(),
            self.2.to_string(),
            format!("{:?}", self.3.iter().take(5).collect::<Vec<_>>()),
        ]
    }
}

impl Printable for (i32, i32, i64, f64) {
    fn to_row(&self) -> Vec<String> {
        vec![
            self.0.to_string(),
            self.1.to_string(),
            self.2.to_string(),
            self.3.to_string(),
        ]
    }
}

pub async fn print_results<T: Printable>(
    headers: Vec<String>,
    left_source: String,
    left_dataset: &[T],
    right_source: String,
    right_dataset: &[T],
) -> Result<()> {
    let mut left_table = Table::new();
    left_table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    let mut right_table = Table::new();
    right_table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    // Prepare headers
    let mut title_cells = vec![Cell::new("Source")];
    title_cells.extend(headers.into_iter().map(|h| Cell::new(&h)));
    left_table.set_titles(Row::new(title_cells.clone()));
    right_table.set_titles(Row::new(title_cells));

    // Add rows for left dataset
    for item in left_dataset {
        let mut row_cells = vec![Cell::new(&left_source)];
        row_cells.extend(item.to_row().into_iter().map(|c| Cell::new(&c)));
        left_table.add_row(Row::new(row_cells));
    }

    // Add rows for right dataset
    for item in right_dataset {
        let mut row_cells = vec![Cell::new(&right_source)];
        row_cells.extend(item.to_row().into_iter().map(|c| Cell::new(&c)));
        right_table.add_row(Row::new(row_cells));
    }

    // Print the table
    left_table.printstd();
    right_table.printstd();

    Ok(())
}
