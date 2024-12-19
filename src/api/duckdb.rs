use anyhow::Result;
use pgrx::*;

use crate::duckdb::connection;

type DuckdbSettingsRow = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

type DuckdbExtensionsRow = (
    Option<String>,
    Option<bool>,
    Option<bool>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[pg_extern]
pub fn duckdb_execute(query: &str) {
    connection::execute(query, []).unwrap_or_else(|err| panic!("error executing query: {err:?}"));
}

#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn duckdb_settings() -> iter::TableIterator<
    'static,
    (
        name!(name, Option<String>),
        name!(value, Option<String>),
        name!(description, Option<String>),
        name!(input_type, Option<String>),
        name!(scope, Option<String>),
    ),
> {
    let rows = duckdb_settings_impl().unwrap_or_else(|e| {
        panic!("{}", e);
    });
    iter::TableIterator::new(rows)
}

#[inline]
fn duckdb_settings_impl() -> Result<Vec<DuckdbSettingsRow>> {
    let conn = unsafe { &*connection::get_global_connection().get() };
    let mut stmt = conn.prepare("SELECT * FROM duckdb_settings()")?;

    Ok(stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?
        .map(|row| row.unwrap())
        .collect::<Vec<DuckdbSettingsRow>>())
}

#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn duckdb_extensions() -> iter::TableIterator<
    'static,
    (
        name!(extension_name, Option<String>),
        name!(loaded, Option<bool>),
        name!(installed, Option<bool>),
        name!(install_path, Option<String>),
        name!(description, Option<String>),
        name!(aliases, Option<String>),
        name!(extension_version, Option<String>),
        name!(install_mode, Option<String>),
        name!(installed_from, Option<String>),
    ),
> {
    let rows = duckdb_extensions_impl().unwrap_or_else(|e| {
        panic!("{}", e);
    });
    iter::TableIterator::new(rows)
}

#[inline]
fn duckdb_extensions_impl() -> Result<Vec<DuckdbExtensionsRow>> {
    let conn = unsafe { &*connection::get_global_connection().get() };
    let mut stmt = conn.prepare(
        "SELECT
            extension_name,
            loaded,
            installed,
            install_path,
            description,
            aliases::varchar,
            extension_version,
            install_mode,
            installed_from
        FROM
            duckdb_extensions();",
    )?;

    Ok(stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<bool>>(1)?,
                row.get::<_, Option<bool>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
            ))
        })?
        .map(|row| row.unwrap())
        .collect::<Vec<DuckdbExtensionsRow>>())
}
