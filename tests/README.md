# Test suite for pg_analytics

This is the test suite for the `pg_analytics` extension.

An example of doing all that's necessary to run the tests is, from the root of the repo is:

```shell
#!/bin/bash

set -x
export DATABASE_URL=postgresql://localhost:28816/pg_analytics
export RUST_BACKTRACE=1

# Reload pg_analytics
cargo pgrx stop --package pg_analytics
cargo pgrx install --package pg_analytics --pg-config ~/.pgrx/16.4/pgrx-install/bin/pg_config
cargo pgrx start --package pg_analytics

# Run tests
cargo test --package tests --features pg16
```
