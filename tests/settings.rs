mod fixtures;

use crate::fixtures::conn;
use crate::fixtures::db::Query;
use anyhow::Result;
use rstest::*;
use sqlx::PgConnection;

#[rstest]
async fn test_duckdb_settings(mut conn: PgConnection) -> Result<()> {
    "SELECT duckdb_execute($$SET memory_limit='10GiB'$$)".execute(&mut conn);
    let memory_limit: (Option<String>,) =
        "SELECT value FROM duckdb_settings() WHERE name='memory_limit'".fetch_one(&mut conn);
    assert_eq!(memory_limit.0, Some("10.0 GiB".to_string()));

    Ok(())
}
