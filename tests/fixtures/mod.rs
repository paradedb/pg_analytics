pub mod db;
pub mod tables;
pub mod utils;

use async_std::task::block_on;
use rstest::*;
use sqlx::PgConnection;

pub use crate::db::*;
#[allow(unused_imports)]
pub use crate::tables::research_project_arrays::*;
#[allow(unused_imports)]
pub use crate::tables::user_session_logs::*;
#[allow(unused_imports)]
pub use crate::utils::*;

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
        sqlx::query("CREATE EXTENSION pg_search;")
            .execute(&mut conn)
            .await
            .expect("could not create extension pg_search");
        conn
    })
}
