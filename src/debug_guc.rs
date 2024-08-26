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
    // disable executor hook to test FDW
    pub disable_executor: GucSetting<bool>,

    // disable FDW to test executor hook
    pub disable_fdw: GucSetting<bool>,
}

impl DebugGucSettings {
    pub const fn new() -> Self {
        Self {
            disable_executor: GucSetting::<bool>::new(false),
            disable_fdw: GucSetting::<bool>::new(false),
        }
    }

    pub fn init(&self) {
        GucRegistry::define_bool_guc(
            "paradedb.disable_executor",
            "Disable executor hook to test FDW.",
            "Disable executor hook to test FDW.",
            &self.disable_executor,
            GucContext::Userset,
            GucFlags::default(),
        );

        GucRegistry::define_bool_guc(
            "paradedb.disable_fdw",
            "Disable FDW to test executor hook.",
            "Disable FDW to test executor hook.",
            &self.disable_fdw,
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
