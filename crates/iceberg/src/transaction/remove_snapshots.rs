// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! ARGON fork: snapshot-expiry primitive — a transaction action emitting
//! `TableUpdate::RemoveSnapshots`. Orphaned data-file cleanup is a separate
//! concern (the caller's maintenance plane).

use std::sync::Arc;

use async_trait::async_trait;

use crate::TableUpdate;
use crate::error::Result;
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};

/// Removes the given snapshot ids from table metadata.
pub struct RemoveSnapshotsAction {
    snapshot_ids: Vec<i64>,
}

impl RemoveSnapshotsAction {
    pub(crate) fn new() -> Self {
        Self {
            snapshot_ids: vec![],
        }
    }

    /// Snapshot ids to remove.
    pub fn remove(mut self, ids: impl IntoIterator<Item = i64>) -> Self {
        self.snapshot_ids.extend(ids);
        self
    }
}

#[async_trait]
impl TransactionAction for RemoveSnapshotsAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        Ok(ActionCommit::new(
            vec![TableUpdate::RemoveSnapshots {
                snapshot_ids: self.snapshot_ids.clone(),
            }],
            vec![],
        ))
    }
}
