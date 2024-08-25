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

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

pub struct DebugGucSettings {
    // disable query pushdown to duckdb in executor hook.
    pub disable_executor_pushdown: GucSetting<bool>,

    // force exeuctor hook pushdown, if not report error.
    pub force_executor_pushdown: GucSetting<bool>,
}

impl DebugGucSettings {
    pub const fn new() -> Self {
        Self {
            disable_executor_pushdown: GucSetting::<bool>::new(false),
            force_executor_pushdown: GucSetting::<bool>::new(false),
        }
    }

    pub fn init(&self) {
        GucRegistry::define_bool_guc(
            "paradedb.debug_disable_executor_pushdown",
            "Disable pushdown query in executor hook.",
            "Disable pushdown query in executor hook.",
            &self.disable_executor_pushdown,
            GucContext::Userset,
            GucFlags::default(),
        );

        GucRegistry::define_bool_guc(
            "paradedb.debug_force_executor_pushdown",
            "Force pushdown query in executor hook.",
            "Force pushdown query in executor hook.",
            &self.force_executor_pushdown,
            GucContext::Userset,
            GucFlags::default(),
        );
    }
}

impl Default for DebugGucSettings {
    fn default() -> Self {
        Self::new()
    }
}
