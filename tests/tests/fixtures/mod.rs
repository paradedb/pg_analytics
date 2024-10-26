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

pub mod arrow;
pub mod tables;

use anyhow::Result;
use async_std::task::block_on;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Duration};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::TimeUnit::Millisecond;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::{
    arrow::{datatypes::FieldRef, record_batch::RecordBatch},
    parquet::arrow::ArrowWriter,
};
use futures::future::{BoxFuture, FutureExt};
use rstest::*;
use serde::Serialize;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use sqlx::PgConnection;
use std::sync::Arc;
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};
use testcontainers::ContainerAsync;
use testcontainers_modules::{
    localstack::LocalStack,
    testcontainers::{runners::AsyncRunner, RunnableImage},
};

use crate::fixtures::tables::nyc_trips::NycTripsTable;
use paradedb_sqllogictest::engine::*;

#[fixture]
pub fn database() -> Db {
    block_on(async { Db::new().await })
}

#[fixture]
pub fn conn(database: Db) -> PgConnection {
    block_on(async {
        let mut conn = database.connection().await;
        sqlx::query("CREATE EXTENSION pg_analytics;")
            .execute(&mut conn)
            .await
            .expect("could not create extension pg_analytics");
        conn
    })
}

#[fixture]
pub fn conn_with_pg_search(database: Db) -> PgConnection {
    block_on(async {
        let mut conn = database.connection().await;
        sqlx::query("CREATE EXTENSION pg_analytics;")
            .execute(&mut conn)
            .await
            .expect("could not create extension pg_analytics");
        conn
    })
}

/// A wrapper type to own both the testcontainers container for localstack
/// and the S3 client. It's important that they be owned together, because
/// testcontainers will stop the Docker container is stopped once the variable
/// is dropped.
#[allow(unused)]
pub struct S3 {
    container: ContainerAsync<LocalStack>,
    pub client: aws_sdk_s3::Client,
    pub url: String,
}

impl S3 {
    async fn new() -> Self {
        let image: RunnableImage<LocalStack> =
            RunnableImage::from(LocalStack).with_env_var(("SERVICES", "s3"));
        let container = image.start().await;

        let host_ip = container.get_host().await;
        let host_port = container.get_host_port_ipv4(4566).await;
        let url = format!("{host_ip}:{host_port}");
        let creds = aws_sdk_s3::config::Credentials::new("fake", "fake", None, None, "test");

        let config = aws_sdk_s3::config::Builder::default()
            .behavior_version(BehaviorVersion::v2024_03_28())
            .region(Region::new("us-east-1"))
            .credentials_provider(creds)
            .endpoint_url(format!("http://{}", url.clone()))
            .force_path_style(true)
            .build();

        let client = aws_sdk_s3::Client::from_conf(config);
        Self {
            container,
            client,
            url,
        }
    }

    #[allow(unused)]
    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.client.create_bucket().bucket(bucket).send().await?;
        Ok(())
    }

    #[allow(unused)]
    pub async fn put_batch(&self, bucket: &str, key: &str, batch: &RecordBatch) -> Result<()> {
        let mut buf = vec![];
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;

        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(buf.into())
            .send()
            .await?;
        Ok(())
    }

    #[allow(unused)]
    pub async fn put_rows<T: Serialize>(&self, bucket: &str, key: &str, rows: &[T]) -> Result<()> {
        let fields = Vec::<FieldRef>::from_type::<NycTripsTable>(TracingOptions::default())?;
        let batch = serde_arrow::to_record_batch(&fields, &rows)?;

        self.put_batch(bucket, key, &batch).await
    }

    #[allow(dead_code)]
    pub async fn put_directory(&self, bucket: &str, path: &str, dir: &Path) -> Result<()> {
        fn upload_files(
            client: aws_sdk_s3::Client,
            bucket: String,
            base_path: PathBuf,
            current_path: PathBuf,
            key_prefix: PathBuf,
        ) -> BoxFuture<'static, Result<()>> {
            async move {
                let entries = fs::read_dir(&current_path)?
                    .filter_map(|entry| entry.ok())
                    .collect::<Vec<_>>();

                for entry in entries {
                    let entry_path = entry.path();
                    if entry_path.is_file() {
                        let key = key_prefix.join(entry_path.strip_prefix(&base_path)?);
                        let mut file = File::open(&entry_path)?;
                        let mut buf = vec![];
                        file.read_to_end(&mut buf)?;
                        client
                            .put_object()
                            .bucket(&bucket)
                            .key(key.to_str().unwrap())
                            .body(ByteStream::from(buf))
                            .send()
                            .await?;
                    } else if entry_path.is_dir() {
                        let new_key_prefix = key_prefix.join(entry_path.strip_prefix(&base_path)?);
                        upload_files(
                            client.clone(),
                            bucket.clone(),
                            base_path.clone(),
                            entry_path.clone(),
                            new_key_prefix,
                        )
                        .await?;
                    }
                }

                Ok(())
            }
            .boxed()
        }

        let key_prefix = PathBuf::from(path);
        upload_files(
            self.client.clone(),
            bucket.to_string(),
            dir.to_path_buf(),
            dir.to_path_buf(),
            key_prefix,
        )
        .await?;
        Ok(())
    }
}

#[fixture]
pub async fn s3() -> S3 {
    S3::new().await
}

#[fixture]
pub fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

#[fixture]
pub fn duckdb_conn() -> duckdb::Connection {
    duckdb::Connection::open_in_memory().unwrap()
}

#[fixture]
pub fn time_series_record_batch_minutes() -> Result<RecordBatch> {
    let fields = vec![
        Field::new("value", DataType::Int32, false),
        Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
    ];

    let schema = Arc::new(Schema::new(fields));

    let start_time = DateTime::from_timestamp(60, 0).unwrap();
    let timestamps: Vec<i64> = (0..10)
        .map(|i| (start_time + Duration::minutes(i)).timestamp_millis())
        .collect();

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, -1, 0, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(TimestampMillisecondArray::from(timestamps)),
        ],
    )?)
}

#[fixture]
pub fn time_series_record_batch_years() -> Result<RecordBatch> {
    let fields = vec![
        Field::new("value", DataType::Int32, false),
        Field::new("timestamp", DataType::Timestamp(Millisecond, None), false),
    ];

    let schema = Arc::new(Schema::new(fields));

    let start_time = DateTime::from_timestamp(60, 0).unwrap();
    let timestamps: Vec<i64> = (0..10)
        .map(|i| (start_time + Duration::days(i * 366)).timestamp_millis())
        .collect();

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, -1, 0, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(TimestampMillisecondArray::from(timestamps)),
        ],
    )?)
}
