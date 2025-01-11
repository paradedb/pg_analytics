<h1 align="center">
  <img src="assets/pg_analytics.svg" alt="pg_analytics">
<br>
</h1>

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/paradedb)](https://artifacthub.io/packages/search?repo=paradedb)
[![Docker Pulls](https://img.shields.io/docker/pulls/paradedb/paradedb)](https://hub.docker.com/r/paradedb/paradedb)
[![License](https://img.shields.io/badge/License-PostgreSQL-blue)](https://github.com/paradedb/pg_analytics?tab=PostgreSQL-1-ov-file#readme)
[![Slack URL](https://img.shields.io/badge/Join%20Slack-purple?logo=slack&link=https%3A%2F%2Fjoin.slack.com%2Ft%2Fparadedbcommunity%2Fshared_invite%2Fzt-2lkzdsetw-OiIgbyFeiibd1DG~6wFgTQ)](https://join.slack.com/t/paradedbcommunity/shared_invite/zt-2lkzdsetw-OiIgbyFeiibd1DG~6wFgTQ)
[![X URL](https://img.shields.io/twitter/url?url=https%3A%2F%2Ftwitter.com%2Fparadedb&label=Follow%20%40paradedb)](https://x.com/paradedb)

## Overview

`pg_analytics` (formerly named `pg_lakehouse`) puts DuckDB inside Postgres. With `pg_analytics` installed, Postgres can query foreign object stores like AWS S3 and table formats like Iceberg or Delta Lake. Queries are pushed down to DuckDB, a high performance analytical query engine.

`pg_analytics` uses DuckDB v1.0.0 and is supported on Postgres 13+.

### Motivation

Today, a vast amount of non-operational data — events, metrics, historical snapshots, vendor data, etc. — is ingested into data lakes like AWS S3. Querying this data by moving it into a cloud data warehouse or operating a new query engine is expensive and time-consuming. The goal of `pg_analytics` is to enable this data to be queried directly from Postgres. This eliminates the need for new infrastructure, loss of data freshness, data movement, and non-Postgres dialects of other query engines.

`pg_analytics` uses the foreign data wrapper (FDW) API to connect to any object store or table format and the executor hook API to push queries to DuckDB. While other FDWs like `aws_s3` have existed in the Postgres extension ecosystem, these FDWs suffer from two limitations:

1. Lack of support for most object stores and table formats
2. Too slow over large datasets to be a viable analytical engine

`pg_analytics` differentiates itself by supporting a wide breadth of stores and formats and by being very fast (thanks to DuckDB).

### Roadmap

- [x] Read support for `pg_analytics`
- [ ] (In progress) Write support for `pg_analytics`
- [x] `EXPLAIN` support
- [x] `VIEW` support
- [x] Automatic schema detection
- [ ] Integration with the catalog providers

#### Object Stores

- [x] AWS S3
- [x] S3-compatible stores (MinIO, R2)
- [x] Google Cloud Storage
- [x] Azure Blob Storage
- [x] Azure Data Lake Storage Gen2
- [x] Hugging Face (`.parquet`, `.csv`, `.jsonl`)
- [x] HTTP server
- [x] Local file system

#### File/Table Formats

- [x] Parquet
- [x] CSV
- [x] JSON
- [x] Geospatial (`.geojson`, `.xlsx`)
- [x] Delta Lake
- [x] Apache Iceberg
- [ ] Apache Hudi

## Installation

### From ParadeDB

The easiest way to use the extension is to run the ParadeDB Dockerfile:

```bash
docker run --name paradedb -e POSTGRES_PASSWORD=password paradedb/paradedb
docker exec -it paradedb psql -U postgres
```

This will spin up a PostgreSQL 16 instance with `pg_analytics` preinstalled.

### From Self-Hosted PostgreSQL

Because this extension uses Postgres hooks to intercept and push queries down to DuckDB, it is **very important** that it is added to `shared_preload_libraries` inside `postgresql.conf`.

```bash
# Inside postgresql.conf
shared_preload_libraries = 'pg_analytics'
```

This ensures the best query performance from the extension.

#### Linux & macOS

We provide prebuilt binaries for macOS, Debian, Ubuntu, and Red Hat Enterprise Linux for Postgres 14+. You can download the latest version for your architecture from the [GitHub Releases page](https://github.com/paradedb/paradedb/releases).

#### Windows

Windows is not supported. This restriction is [inherited from pgrx not supporting Windows](https://github.com/pgcentralfoundation/pgrx?tab=readme-ov-file#caveats--known-issues).

## Usage

The following example uses `pg_analytics` to query an example dataset of 3 million NYC taxi trips from January 2024, hosted in a public `us-east-1` S3 bucket provided by ParadeDB.

```sql
CREATE EXTENSION pg_analytics;
CREATE FOREIGN DATA WRAPPER parquet_wrapper HANDLER parquet_fdw_handler VALIDATOR parquet_fdw_validator;

-- Provide S3 credentials
CREATE SERVER parquet_server FOREIGN DATA WRAPPER parquet_wrapper;

-- Create foreign table with auto schema creation
CREATE FOREIGN TABLE trips ()
SERVER parquet_server
OPTIONS (files 's3://paradedb-benchmarks/yellow_tripdata_2024-01.parquet');

-- Success! Now you can query the remote Parquet file like a regular Postgres table
SELECT COUNT(*) FROM trips;
  count
---------
 2964624
(1 row)
```

## Documentation

Complete documentation for `pg_analytics` can be found under the [docs](/docs/) folder as Markdown files. It covers how to query the various object stores and file and table formats supports, and how to configure and tune the extension.

A hosted version of the documentation can be found [here](https://docs.paradedb.com/integrations/overview).

## Development

### Install Rust

To develop the extension, first install Rust via `rustup`.

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup install <version>

rustup default <version>
```

Note: While it is possible to install Rust via your package manager, we recommend using `rustup` as we've observed inconsistencies with Homebrew's Rust installation on macOS.

### Install Dependencies

Before compiling the extension, you'll need to have the following dependencies installed.

```bash
# macOS
brew install make gcc pkg-config openssl

# Ubuntu
sudo apt-get install -y make gcc pkg-config libssl-dev libclang-dev

# Arch Linux
sudo pacman -S core/openssl extra/clang
```

### Install Postgres

```bash
# macOS
brew install postgresql@17

# Ubuntu
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo apt-get update && sudo apt-get install -y postgresql-17 postgresql-server-dev-17

# Arch Linux
sudo pacman -S extra/postgresql
```

If you are using Postgres.app to manage your macOS PostgreSQL, you'll need to add the `pg_config` binary to your path before continuing:

```bash
export PATH="$PATH:/Applications/Postgres.app/Contents/Versions/latest/bin"
```

### Install pgrx

Then, install and initialize `pgrx`:

```bash
# Note: Replace --pg17 with your version of Postgres, if different (i.e. --pg16)
cargo install --locked cargo-pgrx --version 0.12.7

# macOS arm64
cargo pgrx init --pg17=/opt/homebrew/opt/postgresql@17/bin/pg_config

# macOS amd64
cargo pgrx init --pg17=/usr/local/opt/postgresql@17/bin/pg_config

# Ubuntu
cargo pgrx init --pg17=/usr/lib/postgresql/17/bin/pg_config

# Arch Linux
cargo pgrx init --pg17=/usr/bin/pg_config
```

If you prefer to use a different version of Postgres, update the `--pg` flag accordingly.

### Running the Extension

First, start pgrx:

```bash
cargo pgrx run
```

This will launch an interactive connection to Postgres. Inside Postgres, create the extension by running:

```sql
CREATE EXTENSION pg_analytics;
```

You now have access to all the extension functions.

### Modifying the Extension

If you make changes to the extension code, follow these steps to update it:

1. Recompile the extension:

```bash
cargo pgrx run
```

2. Recreate the extension to load the latest changes:

```sql
DROP EXTENSION pg_analytics;
CREATE EXTENSION pg_analytics;
```

### Running Tests

We use `cargo test` as our runner for `pg_analytics` tests. Tests are conducted using [testcontainers](https://github.com/testcontainers/testcontainers-rs) to manage testing containers like [LocalStack](https://hub.docker.com/r/localstack/localstack). `testcontainers` will pull any Docker images that it requires to perform the test.

You also need a running Postgres instance to run the tests. The test suite will look for a connection string on the `DATABASE_URL` environment variable. You can set this variable manually, or use `.env` file with contents like this:

```text
DATABASE_URL=postgres://<username>@<host>:<port>/<database>
```

## License

`pg_analytics` is licensed under the [PostgreSQL License](https://www.postgresql.org/about/licence/).
