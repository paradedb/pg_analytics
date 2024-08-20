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

mod api;
#[cfg(debug_assertions)]
mod debug_guc;
mod duckdb;
mod env;
mod fdw;
mod hooks;
mod schema;

#[cfg(debug_assertions)]
use crate::debug_guc::DebugGucSettings;
use hooks::ExtensionHook;
use pgrx::*;

// TODO: Reactivate once we've properly integrated with the monorepo
// A static variable is required to host grand unified configuration settings.
// pub static GUCS: PostgresGlobalGucSettings = PostgresGlobalGucSettings::new();

#[cfg(debug_assertions)]
pub static DEBUG_GUCS: DebugGucSettings = DebugGucSettings::new();

pg_module_magic!();

static mut EXTENSION_HOOK: ExtensionHook = ExtensionHook;

#[pg_guard]
pub extern "C" fn _PG_init() {
    pgrx::warning!("pga:: extension is being initialized");
    #[allow(static_mut_refs)]
    #[allow(deprecated)]
    unsafe {
        register_hook(&mut EXTENSION_HOOK)
    };

    // GUCS.init("pg_analytics");
    pg_shmem_init!(env::DUCKDB_CONNECTION_CACHE);

    #[cfg(debug_assertions)]
    DEBUG_GUCS.init();
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
