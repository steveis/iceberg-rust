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

//! ARGON fork (G-LIN / G-UPG): v3 row-lineage assignment through the fork's
//! own snapshot actions — `row_delta` above all, since it is the ArgonDB
//! committer's steady-state commit path and upstream's lineage tests only
//! cover `fast_append`.
//!
//! Spec basis (format/spec.md @ apache-iceberg-1.11.0, mapped in the ArgonDB
//! repo's plans/D18-V3-MUST-MAP.md):
//! - 1.5  snapshot `first-row-id` = table `next-row-id` at each commit
//!        attempt; reassigned on retry; required even for commits that
//!        assign no ID space (delete-only).
//! - 1.6  snapshot `added-rows` required in v3.
//! - 1.7  `next-row-id` ends greater than every assigned row ID.
//! - 1.8  manifest-list write assigns `first_row_id` to every data manifest
//!        lacking one, preserves existing values, delete manifests null.
//! - 1.9  new data-file entries carry null `first_row_id` (inherited).
//! - 1.10 v2→v3 upgrade: next-row-id init 0, old snapshots untouched, first
//!        post-upgrade commit assigns first_row_id to ALL data manifests.
//! - 7.1  retried commits reassign first-row-id from refreshed metadata
//!        (the catalog stores whatever the client sends — Lakekeeper probe
//!        plans/GCAT-LAKEKEEPER-V3.md — so the re-stamp is load-bearing).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use crate::catalog::MockCatalog;
use crate::io::FileIO;
use crate::memory::tests::new_memory_catalog;
use crate::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
    NestedField, PrimitiveType, Schema, Struct, TableMetadata, Type,
};
use crate::table::Table;
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{Catalog, Error, ErrorKind, TableCreation, TableIdent, TableUpdate};

/// Unpartitioned table (ArgonDB never partitions — MUST-map 3.5) with an
/// `id long` / `v string` schema, created at `version` in `catalog`.
async fn make_unpartitioned_table(catalog: &impl Catalog, version: FormatVersion) -> Table {
    let ident = TableIdent::from_strs([
        format!("ns-{}", uuid::Uuid::new_v4()),
        "argon_lineage".to_string(),
    ])
    .unwrap();
    catalog
        .create_namespace(ident.namespace(), HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    let creation = TableCreation::builder()
        .name(ident.name().to_string())
        .schema(schema)
        .format_version(version)
        .build();
    catalog
        .create_table(ident.namespace(), creation)
        .await
        .unwrap()
}

fn data_file(rows: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("test/data-{}-{rows}.parquet", uuid::Uuid::new_v4()))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(rows)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .build()
        .unwrap()
}

fn equality_delete_file(rows: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::EqualityDeletes)
        .file_path(format!("test/del-{}-{rows}.parquet", uuid::Uuid::new_v4()))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(50)
        .record_count(rows)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .equality_ids(Some(vec![1]))
        .build()
        .unwrap()
}

/// G-LIN: the committer's steady-state path. Two row-delta commits (data +
/// equality-delete files, ArgonDB upsert shape): snapshot row ranges chain
/// (1.5/1.6/1.7), data manifests are assigned/preserved (1.8), delete
/// manifests stay null (1.8), new data entries inherit (1.9), and the
/// equality-delete path itself stays v3-valid (map 1.3 — no DVs, no
/// positional deletes anywhere in this file).
#[tokio::test]
async fn row_delta_v3_lineage_data_and_delete_manifests() {
    let catalog = new_memory_catalog().await;
    let table = make_unpartitioned_table(&catalog, FormatVersion::V3).await;
    assert_eq!(table.metadata().next_row_id(), 0);

    // Commit 1: 30 upsert rows + their tombstones (delete-first upsert).
    let tx = Transaction::new(&table);
    let tx = tx
        .row_delta()
        .add_data_files(vec![data_file(30)])
        .add_delete_files(vec![equality_delete_file(30)])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();

    let snap1 = table.metadata().current_snapshot().unwrap();
    assert_eq!(snap1.first_row_id(), Some(0), "1.5: first-row-id = next-row-id at commit");
    assert_eq!(snap1.added_rows_count(), Some(30), "1.6: added-rows recorded");
    assert_eq!(table.metadata().next_row_id(), 30, "1.7: next-row-id > all assigned IDs");

    let list1 = table.manifest_list_reader(snap1).load().await.unwrap();
    let (data1, del1): (Vec<_>, Vec<_>) = list1
        .entries()
        .iter()
        .partition(|m| m.content == crate::spec::ManifestContentType::Data);
    assert_eq!((data1.len(), del1.len()), (1, 1));
    assert_eq!(data1[0].first_row_id, Some(0), "1.8: data manifest assigned");
    assert_eq!(del1[0].first_row_id, None, "1.8: delete manifest always null");

    // 1.9: the new data-file ENTRY carries null first_row_id (inherited).
    let manifest = data1[0].load_manifest(table.file_io()).await.unwrap();
    for entry in manifest.entries() {
        assert_eq!(
            entry.data_file().first_row_id(),
            None,
            "1.9: added data entries written with null first_row_id"
        );
    }

    // Commit 2: 17 more rows + tombstones. Carried-forward manifests keep
    // their assignment; the new manifest continues the ID space.
    let tx = Transaction::new(&table);
    let tx = tx
        .row_delta()
        .add_data_files(vec![data_file(17)])
        .add_delete_files(vec![equality_delete_file(17)])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();

    let snap2 = table.metadata().current_snapshot().unwrap();
    assert_eq!(snap2.first_row_id(), Some(30));
    assert_eq!(snap2.added_rows_count(), Some(17));
    assert_eq!(table.metadata().next_row_id(), 47);

    let list2 = table.manifest_list_reader(snap2).load().await.unwrap();
    let mut data_ids: Vec<Option<u64>> = list2
        .entries()
        .iter()
        .filter(|m| m.content == crate::spec::ManifestContentType::Data)
        .map(|m| m.first_row_id)
        .collect();
    data_ids.sort();
    assert_eq!(
        data_ids,
        vec![Some(0), Some(30)],
        "1.8: existing assignment preserved, new manifest appended after it"
    );
    assert!(
        list2
            .entries()
            .iter()
            .filter(|m| m.content == crate::spec::ManifestContentType::Deletes)
            .all(|m| m.first_row_id.is_none()),
        "1.8: all delete manifests null"
    );
}

/// G-LIN boundary (map note 3): a delete-only tombstone commit assigns no ID
/// space but MUST still carry first-row-id (1.5) with added-rows = 0, and
/// MUST NOT advance next-row-id.
#[tokio::test]
async fn row_delta_v3_delete_only_commit_zero_added_rows() {
    let catalog = new_memory_catalog().await;
    let table = make_unpartitioned_table(&catalog, FormatVersion::V3).await;

    let tx = Transaction::new(&table);
    let tx = tx
        .row_delta()
        .add_data_files(vec![data_file(30)])
        .add_delete_files(vec![equality_delete_file(30)])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();
    assert_eq!(table.metadata().next_row_id(), 30);

    // DELETE-only cadence: tombstones, no new row images.
    let tx = Transaction::new(&table);
    let tx = tx
        .row_delta()
        .add_delete_files(vec![equality_delete_file(5)])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();

    let snap = table.metadata().current_snapshot().unwrap();
    assert_eq!(
        snap.first_row_id(),
        Some(30),
        "1.5: first-row-id required even when no ID space is assigned"
    );
    assert_eq!(snap.added_rows_count(), Some(0), "1.6: added-rows = 0");
    assert_eq!(table.metadata().next_row_id(), 30, "1.7: no advance");
}

/// G-UPG (1.10) + the mixed existing/added manifest vector (1.8): two v2
/// appends, an in-place upgrade, then the first post-upgrade commit must
/// assign first_row_id to ALL data manifests — pre-upgrade ones first, in
/// manifest-list order, each advanced by existing+added rows of the one
/// before it.
#[tokio::test]
async fn upgrade_v2_to_v3_first_commit_assigns_all_data_manifests() {
    let catalog = new_memory_catalog().await;
    let table = make_unpartitioned_table(&catalog, FormatVersion::V2).await;
    assert_eq!(table.metadata().format_version(), FormatVersion::V2);

    // Two v2 appends: manifests A (100 rows) and B (50 rows), no lineage.
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![data_file(100)])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![data_file(50)])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();
    let v2_snap_id = table.metadata().current_snapshot().unwrap().snapshot_id();
    assert_eq!(
        table.metadata().current_snapshot().unwrap().first_row_id(),
        None,
        "v2 snapshots carry no row range"
    );

    // Metadata-only upgrade (4.7): next-row-id initialized to 0, old
    // snapshots untouched.
    let tx = Transaction::new(&table);
    let tx = tx
        .upgrade_table_version()
        .set_format_version(FormatVersion::V3)
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
    assert_eq!(table.metadata().next_row_id(), 0, "1.10: next-row-id init 0");
    let old_snap = table.metadata().snapshot_by_id(v2_snap_id).unwrap();
    assert_eq!(old_snap.first_row_id(), None, "1.10: old snapshots untouched");

    // First post-upgrade commit: manifest C (20 rows) added; A and B must be
    // assigned too, in order, before C.
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![data_file(20)])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();

    let snap = table.metadata().current_snapshot().unwrap();
    assert_eq!(snap.first_row_id(), Some(0), "1.5 on the upgrade boundary");
    assert_eq!(
        snap.added_rows_count(),
        Some(170),
        "1.6: upper bound covers ALL newly assigned IDs (100+50 existing + 20 added)"
    );
    assert_eq!(table.metadata().next_row_id(), 170, "1.7");

    let list = table.manifest_list_reader(snap).load().await.unwrap();
    let ids: Vec<Option<u64>> = list
        .entries()
        .iter()
        .filter(|m| m.content == crate::spec::ManifestContentType::Data)
        .map(|m| m.first_row_id)
        .collect();
    // Manifest-list order: existing manifests (A, B — newest snapshot's list
    // order) then the added manifest C. Each assignment advances by the
    // previous manifest's existing+added rows (1.8).
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec![Some(0), Some(100), Some(150)],
        "1.10/1.8: every data manifest assigned, ID space dense: got {ids:?}"
    );
}

/// v2 byte-identity guard: with the table at v2, the same row-delta path
/// must emit NO lineage fields anywhere — v2 snapshots have no row range and
/// v2 manifest-list entries no first_row_id (D18: v2 output unchanged until
/// the knob demands v3).
#[tokio::test]
async fn v2_row_delta_writes_no_lineage_fields() {
    let catalog = new_memory_catalog().await;
    let table = make_unpartitioned_table(&catalog, FormatVersion::V2).await;

    let tx = Transaction::new(&table);
    let tx = tx
        .row_delta()
        .add_data_files(vec![data_file(30)])
        .add_delete_files(vec![equality_delete_file(30)])
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();

    assert_eq!(table.metadata().format_version(), FormatVersion::V2);
    let snap = table.metadata().current_snapshot().unwrap();
    assert_eq!(snap.first_row_id(), None);
    assert_eq!(snap.added_rows_count(), None);
    let list = table.manifest_list_reader(snap).load().await.unwrap();
    assert!(
        list.entries().iter().all(|m| m.first_row_id.is_none()),
        "v2 manifest-list entries must carry no first_row_id"
    );
}

/// G-LIN retry (1.5 / 7.1) — the load-bearing test: Lakekeeper stores
/// whatever first-row-id the client sends (no server-side validation or
/// reassignment, plans/GCAT-LAKEKEEPER-V3.md), so on a commit conflict the
/// client MUST re-stamp first-row-id from freshly-read metadata. Attempt 1
/// commits against next-row-id=100 and fails with a retryable conflict; the
/// refresh returns next-row-id=150 (a concurrent writer advanced it); the
/// retried commit must carry first-row-id 150, not 100.
#[tokio::test]
async fn v3_commit_retry_restamps_first_row_id_from_refreshed_metadata() {
    fn v3_table_with_next_row_id(next_row_id: u64) -> Table {
        let file = std::fs::read_to_string(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV3ValidMinimal.json"
        ))
        .unwrap();
        let mut doc: serde_json::Value = serde_json::from_str(&file).unwrap();
        doc["next-row-id"] = serde_json::json!(next_row_id);
        doc["location"] = serde_json::json!("memory://root/argon_retry");
        doc["properties"] = serde_json::json!({
            "commit.retry.min-wait-ms": "10",
            "commit.retry.max-wait-ms": "20",
            "commit.retry.total-timeout-ms": "3000",
            "commit.retry.num-retries": "3",
        });
        let metadata: TableMetadata = serde_json::from_value(doc).unwrap();
        Table::builder()
            .metadata(metadata)
            .metadata_location(format!("memory://root/argon_retry/metadata/v-{next_row_id}.json"))
            .identifier(TableIdent::from_strs(["ns1", "argon_retry"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(crate::test_utils::test_runtime())
            .build()
            .unwrap()
    }

    // The minimal-v3 fixture is partitioned on `x` (identity): data files
    // need a one-field partition struct.
    fn partitioned_data_file(rows: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("memory://root/argon_retry/data/{rows}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(rows)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    let seen_first_row_ids: Arc<Mutex<Vec<Option<u64>>>> = Arc::new(Mutex::new(Vec::new()));

    let mut mock_catalog = MockCatalog::new();
    // Refresh 1 sees next-row-id=100; refresh 2 (after the conflict) sees a
    // concurrent writer advanced it to 150.
    let load_calls = AtomicU32::new(0);
    mock_catalog.expect_load_table().returning_st(move |_| {
        let n = load_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(v3_table_with_next_row_id(if n == 0 { 100 } else { 150 }))
        })
    });

    let update_calls = AtomicU32::new(0);
    let seen = Arc::clone(&seen_first_row_ids);
    mock_catalog
        .expect_update_table()
        .times(2)
        .returning_st(move |mut commit| {
            for update in commit.take_updates() {
                if let TableUpdate::AddSnapshot { snapshot } = update {
                    seen.lock().unwrap().push(snapshot.first_row_id());
                }
            }
            let n = update_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if n == 0 {
                    Err(
                        Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                            .with_retryable(true),
                    )
                } else {
                    Ok(v3_table_with_next_row_id(180))
                }
            })
        });

    let table = v3_table_with_next_row_id(100);
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![partitioned_data_file(30)])
        .apply(tx)
        .unwrap();
    tx.commit(&mock_catalog).await.unwrap();

    let seen = seen_first_row_ids.lock().unwrap();
    assert_eq!(
        *seen,
        vec![Some(100), Some(150)],
        "7.1/1.5: retried commit must re-stamp first-row-id from refreshed \
         next-row-id (stale value would be stored verbatim by the catalog)"
    );
}
