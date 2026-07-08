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

//! ARGON fork: full-table rewrite — a `replace` snapshot whose data is
//! exactly the added files; no manifests carry forward. The compaction
//! primitive: the table's live rows are rewritten into fewer/sorted files
//! and any accumulated delete files fall away (their effect is baked into
//! the rewritten data). Prior snapshots keep referencing the old files, so
//! history and time travel survive until snapshot expiry reclaims them.
//! Data-preservation is the CALLER's contract: the added files must hold
//! exactly the table's live rows as of the snapshot this action commits
//! against (the `RefSnapshotIdMatch` requirement rejects the commit if the
//! table moved after the caller read it).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// A transaction action replacing the table's entire live file set with the
/// added data files, as a `replace` snapshot (compaction / re-clustering).
pub struct RewriteFilesAction {
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
        }
    }

    /// Add the rewritten data files (the table's complete new contents).
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            vec![],
        );

        snapshot_producer.validate_added_data_files()?;

        snapshot_producer
            .commit(RewriteFilesOperation, DefaultManifestProcess)
            .await
    }
}

struct RewriteFilesOperation;

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Nothing carries forward: the new snapshot's contents are exactly
        // the added files.
        Ok(vec![])
    }
}
