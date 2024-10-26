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

// TECH DEBT: This file is a copy of the `db.rs` file from https://github.com/paradedb/paradedb/blob/dev/shared/src/fixtures/db.rs
// We duplicated because the paradedb repo may use a different version of pgrx than pg_analytics, but eventually we should
// move this into a separate crate without any dependencies on pgrx.

use crate::normalize::convert_rows;
use crate::normalize::convert_types;
use async_std::prelude::Stream;
use async_std::stream::StreamExt;
use async_std::task::block_on;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use sqllogictest::DBOutput;
use sqlx::Row;
use sqlx::{
    postgres::PgRow,
    testing::{TestArgs, TestContext, TestSupport},
    ConnectOptions, Decode, Executor, FromRow, PgConnection, Postgres, Type,
};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{DFSqlLogicTestError, Result};
use crate::{
    arrow::schema_to_batch,
    output::{DFColumnType, DFOutput},
};

pub struct ParadeDB {
    context: TestContext<Postgres>,
}

impl ParadeDB {
    pub async fn new() -> Self {
        // Use a timestamp as a unique identifier.
        let path = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
            .to_string();

        let args = TestArgs::new(Box::leak(path.into_boxed_str()));
        let context = Postgres::test_context(&args)
            .await
            .unwrap_or_else(|err| panic!("could not create test database: {err:#?}"));

        Self { context }
    }

    pub async fn connection(&self) -> PgConnection {
        self.context
            .connect_opts
            .connect()
            .await
            .unwrap_or_else(|err| panic!("failed to connect to test database: {err:#?}"))
    }
}

impl Drop for ParadeDB {
    fn drop(&mut self) {
        let db_name = self.context.db_name.to_string();
        async_std::task::spawn(async move {
            Postgres::cleanup_test(db_name.as_str()).await.unwrap();
        });
    }
}

pub trait Query
where
    Self: AsRef<str> + Sized,
{
    fn execute(self, connection: &mut PgConnection) {
        block_on(async {
            connection.execute(self.as_ref()).await.unwrap();
        })
    }

    fn execute_result(self, connection: &mut PgConnection) -> Result<(), sqlx::Error> {
        block_on(async { connection.execute(self.as_ref()).await })?;
        Ok(())
    }

    fn fetch<T>(self, connection: &mut PgConnection) -> Vec<T>
    where
        T: for<'r> FromRow<'r, <Postgres as sqlx::Database>::Row> + Send + Unpin,
    {
        block_on(async {
            sqlx::query_as::<_, T>(self.as_ref())
                .fetch_all(connection)
                .await
                .unwrap_or_else(|_| panic!("error in query '{}'", self.as_ref()))
        })
    }

    fn fetch_dynamic(self, connection: &mut PgConnection) -> Vec<PgRow> {
        block_on(async {
            sqlx::query(self.as_ref())
                .fetch_all(connection)
                .await
                .unwrap_or_else(|_| panic!("error in query '{}'", self.as_ref()))
        })
    }

    fn fetch_dynamic_result(
        self,
        connection: &mut PgConnection,
    ) -> Result<Vec<PgRow>, sqlx::Error> {
        block_on(async { sqlx::query(self.as_ref()).fetch_all(connection).await })
    }

    /// A convenient helper for processing PgRow results from Postgres into a DataFusion RecordBatch.
    /// It's important to note that the retrieved RecordBatch may not necessarily have the same
    /// column order as your Postgres table, or parquet file in a foreign table.
    /// You shouldn't expect to be able to test two RecordBatches directly for equality.
    /// Instead, just test the column equality for each column, like so:
    ///
    /// assert_eq!(stored_batch.num_columns(), retrieved_batch.num_columns());
    /// for field in stored_batch.schema().fields() {
    ///     assert_eq!(
    ///         stored_batch.column_by_name(field.name()),
    ///         retrieved_batch.column_by_name(field.name())
    ///     )
    /// }
    ///
    fn fetch_recordbatch(self, connection: &mut PgConnection, schema: &SchemaRef) -> RecordBatch {
        block_on(async {
            let rows = sqlx::query(self.as_ref())
                .fetch_all(connection)
                .await
                .unwrap_or_else(|_| panic!("error in query '{}'", self.as_ref()));
            schema_to_batch(schema, &rows).expect("could not convert rows to RecordBatch")
        })
    }

    fn fetch_scalar<T>(self, connection: &mut PgConnection) -> Vec<T>
    where
        T: Type<Postgres> + for<'a> Decode<'a, sqlx::Postgres> + Send + Unpin,
    {
        block_on(async {
            sqlx::query_scalar(self.as_ref())
                .fetch_all(connection)
                .await
                .unwrap_or_else(|_| panic!("error in query '{}'", self.as_ref()))
        })
    }

    fn fetch_one<T>(self, connection: &mut PgConnection) -> T
    where
        T: for<'r> FromRow<'r, <Postgres as sqlx::Database>::Row> + Send + Unpin,
    {
        block_on(async {
            sqlx::query_as::<_, T>(self.as_ref())
                .fetch_one(connection)
                .await
                .unwrap_or_else(|_| panic!("error in query '{}'", self.as_ref()))
        })
    }

    fn fetch_result<T>(self, connection: &mut PgConnection) -> Result<Vec<T>, sqlx::Error>
    where
        T: for<'r> FromRow<'r, <Postgres as sqlx::Database>::Row> + Send + Unpin,
    {
        block_on(async {
            sqlx::query_as::<_, T>(self.as_ref())
                .fetch_all(connection)
                .await
        })
    }

    fn fetch_collect<T, B>(self, connection: &mut PgConnection) -> B
    where
        T: for<'r> FromRow<'r, <Postgres as sqlx::Database>::Row> + Send + Unpin,
        B: FromIterator<T>,
    {
        self.fetch(connection).into_iter().collect::<B>()
    }
}

impl Query for String {}
impl Query for &String {}
impl Query for &str {}

pub trait DisplayAsync: Stream<Item = Result<Bytes, sqlx::Error>> + Sized {
    fn to_csv(self) -> String {
        let mut csv_str = String::new();
        let mut stream = Box::pin(self);

        while let Some(chunk) = block_on(stream.as_mut().next()) {
            let chunk = chunk.unwrap();
            csv_str.push_str(&String::from_utf8_lossy(&chunk));
        }

        csv_str
    }
}

impl<T> DisplayAsync for T where T: Stream<Item = Result<Bytes, sqlx::Error>> + Send + Sized {}

#[async_trait]
impl sqllogictest::AsyncDB for ParadeDB {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut conn = self.connection().await;
        run_query(sql, &mut conn).await
    }

    fn engine_name(&self) -> &str {
        "ParadeDB"
    }
}

async fn run_query(sql: impl Into<String> + Query, conn: &mut PgConnection) -> Result<DFOutput> {
    let results: Vec<PgRow> = sql.fetch_dynamic_result(conn)?;

    let rows = convert_rows(&results);
    let types = if rows.is_empty() {
        vec![]
    } else {
        convert_types(results[0].columns())
    };

    if rows.is_empty() && types.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        Ok(DBOutput::Rows { types, rows })
    }
}
