use async_trait::async_trait;
use datafusion_federation_sql::SQLExecutor;
use deltalake::datafusion::arrow::datatypes::SchemaRef;
use deltalake::datafusion::arrow::record_batch::RecordBatch;
use deltalake::datafusion::error::{DataFusionError, Result};
use deltalake::datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, SendableRecordBatchStream,
};
use deltalake::datafusion::sql::sqlparser::dialect::{Dialect, PostgreSqlDialect};
use memoffset::offset_of;
use pgrx::*;
use std::sync::Arc;

use crate::datafusion::plan::LogicalPlanDetails;
use crate::datafusion::query::QueryString;
use crate::datafusion::session::Session;
use crate::datafusion::table::DatafusionTable;
use crate::types::array::IntoArrowArray;
use crate::types::datatype::PgTypeMod;

pub struct ColumnExecutor {
    schema_name: String,
}

impl ColumnExecutor {
    pub fn new(schema_name: String) -> Result<Self> {
        Ok(Self { schema_name })
    }
}
#[async_trait]
impl SQLExecutor for ColumnExecutor {
    fn name(&self) -> &str {
        "column_executor"
    }

    fn compute_context(&self) -> Option<String> {
        Some("col".to_string())
    }

    fn execute(
        &self,
        sql: &str,
        schema: SchemaRef,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let logical_plan = LogicalPlanDetails::try_from(QueryString(sql))
            .map_err(|err| DataFusionError::External(err.into()))?
            .logical_plan();

        let batch_stream = Session::with_session_context(|context| {
            Box::pin(async move {
                let dataframe = context.execute_logical_plan(logical_plan.clone()).await?;
                Ok(dataframe.execute_stream().await?)
            })
        })
        .map_err(|err| DataFusionError::External(err.into()))?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            batch_stream,
        )))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "column source: table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef, DataFusionError> {
        let pg_relation = unsafe {
            PgRelation::open_with_name(format!("{}.{}", self.schema_name, table_name).as_str())
                .map_err(|err| DataFusionError::External(err.into()))?
        };

        let schema = Arc::new(
            pg_relation
                .arrow_schema()
                .map_err(|err| DataFusionError::External(err.into()))?,
        );

        Ok(schema)
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(PostgreSqlDialect {})
    }
}

pub struct RowExecutor {
    schema_name: String,
}

impl RowExecutor {
    pub fn new(schema_name: String) -> Result<Self> {
        Ok(Self { schema_name })
    }
}

#[async_trait]
impl SQLExecutor for RowExecutor {
    fn name(&self) -> &str {
        "row_executor"
    }

    fn compute_context(&self) -> Option<String> {
        Some("row".to_string())
    }

    fn execute(
        &self,
        sql: &str,
        schema: SchemaRef,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let mut col_arrays = vec![];
        Spi::connect(|client| {
            let mut cursor = client.open_cursor(sql, None);
            let schema_tuple_table = cursor
                .fetch(0)
                .map_err(|err| DataFusionError::External(err.into()))?;

            let num_cols = schema_tuple_table
                .columns()
                .map_err(|err| DataFusionError::External(err.into()))?;
            let mut col_datums: Vec<Vec<Option<pg_sys::Datum>>> =
                (0..num_cols).map(|_| vec![]).collect();

            // We can only get the typmod from the raw tuptable
            let raw_schema_tuple_table = unsafe { pg_sys::SPI_tuptable };
            let tuple_attrs = unsafe { (*(*raw_schema_tuple_table).tupdesc).attrs.as_mut_ptr() };

            // Fill all columns with the appropriate datums
            let mut tuple_table;
            // Calculate MAX_TUPLES_PER_PAGE and fetch that many tuples at a time
            let max_tuples = unsafe {
                (pg_sys::BLCKSZ as usize - offset_of!(pg_sys::PageHeaderData, pd_linp))
                    / (pg_sys::MAXALIGN(offset_of!(pg_sys::HeapTupleHeaderData, t_bits))
                        + std::mem::size_of::<pg_sys::ItemIdData>())
            };
            loop {
                tuple_table = cursor
                    .fetch(max_tuples as i64)
                    .map_err(|err| DataFusionError::External(err.into()))?;
                tuple_table = tuple_table.first();
                if tuple_table.is_empty() {
                    break;
                }
                while tuple_table
                    .get_heap_tuple()
                    .map_err(|err| DataFusionError::External(err.into()))?
                    .is_some()
                {
                    for (col_idx, col) in col_datums.iter_mut().enumerate().take(num_cols) {
                        col.push(
                            tuple_table
                                .get_datum_by_ordinal(col_idx + 1)
                                .map_err(|err| DataFusionError::External(err.into()))?,
                        );
                    }

                    if tuple_table.next().is_none() {
                        break;
                    }
                }
            }

            // Convert datum columns to arrow arrays
            for (col_idx, col_datum_vec) in col_datums.iter().enumerate().take(num_cols) {
                let oid = tuple_table
                    .column_type_oid(col_idx + 1)
                    .map_err(|err| DataFusionError::External(err.into()))?;
                let typmod = unsafe { (*tuple_attrs.add(col_idx)).atttypmod };

                col_arrays.push(
                    col_datum_vec
                        .clone()
                        .into_iter()
                        .into_arrow_array(oid, PgTypeMod(typmod))
                        .map_err(|err| DataFusionError::External(err.into()))?,
                );
            }

            Ok::<(), DataFusionError>(())
        })?;

        let record_batch = RecordBatch::try_new(schema.clone(), col_arrays)?;
        let stream = futures::stream::once(async move { Ok(record_batch) });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "row source: table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef, DataFusionError> {
        let pg_relation = unsafe {
            PgRelation::open_with_name(format!("{}.{}", self.schema_name, table_name).as_str())
                .map_err(|err| DataFusionError::External(err.into()))?
        };

        let schema = Arc::new(
            pg_relation
                .arrow_schema()
                .map_err(|err| DataFusionError::External(err.into()))?,
        );

        Ok(schema)
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(PostgreSqlDialect {})
    }
}
