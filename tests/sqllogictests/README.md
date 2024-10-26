<!--
  Copyright (c) 2023-2024 Retake, Inc.
  This file is part of ParadeDB - Postgres for Search and Analytics
  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.
  This program is distributed in the hope that it will be useful
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
  GNU Affero General Public License for more details.
  You should have received a copy of the GNU Affero General Public License
  along with this program. If not, see <http://www.gnu.org/licenses/>.
-->

# **ParadeDB SQL Logic Tests**

This document outlines the `sqllogictests` submodule of the **ParadeDB** project, which provides functionality for validating SQL query behavior using the [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) format.

## **Overview**

The `sqllogictests` module leverages [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) to parse and execute `.slt` files located in the [`test_files`](test_files) directory. These tests ensure correctness of SQL queries by comparing actual outputs against expected results, facilitating cross-engine SQL validation.

## **Running Tests: Quick Examples**

Below are example commands for running the sqllogictests with various configurations:

```shell
# Run all SQL logic tests
cargo test --test sqllogictests
```

## **Reference: `.slt` File Format**

The `.slt` file format was initially developed for SQLite to validate SQL engine behavior. It is designed to be **engine-agnostic**, meaning it can be reused across different SQL engines by ensuring query outputs are consistent with expected results.

Each `.slt` file operates independently, allowing parallel execution of tests. Typical `.slt` files contain setup commands (e.g., `CREATE TABLE`) followed by queries to test the behavior of the SQL engine.

### **Format of Query Records**

```sql
# <test_name>
query <type_string> <sort_mode>
<sql_query>
----
<expected_result>
```

#### **Explanation of Components**

- **`test_name`**: Unique identifier for the test case.
- **`type_string`**: A string defining the number and type of result columns. Each character in the string corresponds to a column type:

  - `B`: **Boolean**
  - `D`: **Datetime**
  - `I`: **Integer**
  - `P`: Timestamp (**P**)
  - `R`: Floating-point (**R**eal)
  - `T`: **Text**
  - `?`: Any other type

- **`sort_mode`**: Optional parameter specifying how results should be sorted.

  - `nosort` (default): Results are presented in the order they are returned by the database engine. Use this only when an `ORDER BY` clause is specified, or the result contains a single row.
  - `rowsort`: Rows are sorted lexicographically by their string representation using [sort_unstable](https://doc.rust-lang.org/std/primitive.slice.html#method.sort_unstable). Note: `"9"` sorts after `"10"`.
  - `valuesort`: Each value is sorted independently, disregarding row groupings.

- **`expected_result`**: The output expected from the SQL engine.
  - Floating-point values are rounded to 12 decimal places.
  - `NULL` values are represented as `NULL`.
  - Empty strings are rendered as `(empty)`.
  - Boolean values appear as `true` or `false`.

> :warning: It is recommended to either **use `ORDER BY`** or apply `rowsort` when queries do not specify an explicit order.

---

## **Example Test Case**

```sql
# group_by_distinct
query TTI
SELECT a, b, COUNT(DISTINCT c) FROM my_table GROUP BY a, b ORDER BY a, b;
----
foo bar 10
foo baz 5
foo     4
        3
```

---

This technical documentation provides the necessary guidelines for configuring, running, and extending the SQL logic tests in the `ParadeDB` project. For more details on value conversions, consult the conversion rules defined in [`/sqllogictests/src/conversion.rs`].
