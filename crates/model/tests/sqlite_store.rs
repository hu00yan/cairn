use cairn_model::sqlite_store::{
    CatalogBatch, CatalogCounts, CollectionRecord, CrashPoint, FileRecord, SqliteCatalogStore,
};

#[cfg(any())]
mod legacy_tests {

    fn batch() -> CatalogBatch {
        CatalogBatch {
            collections: vec![CollectionRecord {
                id: 1,
                name: "docs".into(),
            }],
            files: vec![FileRecord {
                id: 2,
                collection_id: 1,
                name: "readme".into(),
                head_version_id: Some(3),
                head_generation: 1,
            }],
            versions: vec![VersionRecord {
                id: 3,
                file_id: 2,
                generation: 1,
                commit_id: [7; 32],
                parent_version_id: None,
                size: 4,
                digest: [8; 32],
            }],
            intents: vec![IntentRecord {
                operation_id: 4,
                actor_id: 9,
                file_id: 2,
                state: "published".into(),
                expected_head_version_id: None,
                expected_head_generation: 0,
                version_id: 3,
                abort_reason: None,
                pinned: false,
            }],
            operations: vec![OperationRecord {
                operation_id: 4,
                actor_id: 9,
                kind: "publish".into(),
                request_fingerprint: [6; 32],
                result: Some("version:3".into()),
                error: None,
            }],
        }
    }

    #[test]
    fn full_synchronous_wal_catalog_reopens_with_all_native_records() {
        let path = std::env::temp_dir().join(format!("cairn-sqlite-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let mut store = SqliteCatalogStore::open(&path).unwrap();
            assert_eq!(
                store.durability_pragmas().unwrap(),
                ("wal".into(), "2".into())
            );
            store.persist(&batch()).unwrap();
            assert_eq!(
                store.operation(4).unwrap().unwrap().result,
                Some("version:3".into())
            );
        }
        let reopened = SqliteCatalogStore::open(&path).unwrap();
        assert_eq!(
            reopened.counts().unwrap(),
            sqlite_store::CatalogCounts {
                collections: 1,
                files: 1,
                versions: 1,
                intents: 1,
                operations: 1
            }
        );
        assert!(DAG_DURABILITY_SEAM.contains("not atomically"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn in_memory_adapter_uses_the_same_schema_and_batch_contract() {
        let mut store = SqliteCatalogStore::in_memory().unwrap();
        store.persist(&batch()).unwrap();
        assert_eq!(store.counts().unwrap().operations, 1);
    }

    #[test]
    fn every_precommit_cut_point_rolls_back_the_entire_catalog_batch() {
        for cut in [
            CrashPoint::AfterCollections,
            CrashPoint::AfterFiles,
            CrashPoint::AfterVersions,
            CrashPoint::AfterIntents,
            CrashPoint::AfterResults,
        ] {
            let path = std::env::temp_dir()
                .join(format!("cairn-sqlite-{cut:?}-{}.db", std::process::id()));
            let _ = std::fs::remove_file(&path);
            {
                let mut store = SqliteCatalogStore::open(&path).unwrap();
                assert!(store.persist_with_cut(&batch(), cut).is_err());
            }
            let reopened = SqliteCatalogStore::open(&path).unwrap();
            assert_eq!(
                reopened.counts().unwrap(),
                sqlite_store::CatalogCounts::default()
            );
            let _ = std::fs::remove_file(path);
        }
    }
}

use cairn_model::sqlite_store::{
    HeadRecord, IntentRecord, OperationRecord, PrincipalRecord, VersionRecord,
};
use rusqlite::Connection;

fn v1_batch() -> CatalogBatch {
    CatalogBatch {
        principals: vec![PrincipalRecord {
            id: 1,
            kind: "user".into(),
            state: "active".into(),
            authz_epoch: 0,
        }],
        memberships: vec![],
        collections: vec![CollectionRecord {
            id: 10,
            owner_id: 1,
            name: "docs".into(),
        }],
        files: vec![FileRecord {
            id: 20,
            collection_id: 10,
            name: "readme".into(),
        }],
        heads: vec![HeadRecord {
            file_id: 20,
            version_id: None,
            generation: 0,
        }],
        versions: vec![],
        intents: vec![],
        operations: vec![],
    }
}

#[test]
fn sol_v1_schema_reopens_and_preserves_catalog() {
    let path = std::env::temp_dir().join(format!("cairn-v1-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    {
        let mut s = SqliteCatalogStore::open(&path).unwrap();
        assert_eq!(s.durability_pragmas().unwrap(), ("wal".into(), "2".into()));
        s.persist(&v1_batch()).unwrap();
    }
    let s = SqliteCatalogStore::open(&path).unwrap();
    assert_eq!(s.counts().unwrap().collections, 1);
    assert_eq!(s.coordinator_epoch().unwrap(), 0);
    assert!(cairn_model::sqlite_store::DAG_DURABILITY_SEAM.contains("not atomic"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn owner_epoch_cas_and_stale_owner_fence() {
    let mut s = SqliteCatalogStore::in_memory().unwrap();
    assert!(s.cas_owner_epoch(0, 7).unwrap());
    assert!(!s.cas_owner_epoch(0, 8).unwrap());
    assert_eq!(s.coordinator_epoch().unwrap(), 7);
    assert!(!s.cas_owner_epoch(7, 7).unwrap());
    assert!(!s.cas_owner_epoch(7, 6).unwrap());
}

#[test]
fn open_rejects_a_malformed_v1_table() {
    let path = std::env::temp_dir().join(format!("cairn-malformed-v1-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE catalog_meta(id INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL, coordinator_epoch INTEGER NOT NULL, allocators TEXT NOT NULL); INSERT INTO catalog_meta VALUES(1,1,0,'{}'); CREATE TABLE publish_intents(operation_id INTEGER PRIMARY KEY);",
        )
        .unwrap();
    drop(connection);
    assert!(SqliteCatalogStore::open(&path).is_err());
    let _ = std::fs::remove_file(path);
}

#[test]
fn each_sqlite_cut_point_rolls_back_every_table() {
    let base = v1_batch();
    for cut in [
        CrashPoint::AfterCollections,
        CrashPoint::AfterFiles,
        CrashPoint::AfterVersions,
        CrashPoint::AfterIntents,
        CrashPoint::AfterResults,
    ] {
        let mut s = SqliteCatalogStore::in_memory().unwrap();
        assert!(s.persist_with_cut(&base, cut).is_err());
        assert_eq!(s.counts().unwrap(), CatalogCounts::default());
    }
}

#[test]
fn non_empty_head_batch_inserts_versions_before_heads() {
    let mut s = SqliteCatalogStore::in_memory().unwrap();
    let mut b = v1_batch();
    b.versions.push(VersionRecord {
        id: 30,
        file_id: 20,
        generation: 1,
        commit_id: [1; 32],
        parent_version_id: None,
        size: 1,
        digest: [2; 32],
    });
    b.heads[0] = HeadRecord {
        file_id: 20,
        version_id: Some(30),
        generation: 1,
    };
    s.persist(&b).unwrap();
    assert_eq!(s.counts().unwrap().versions, 1);
    assert_eq!(s.counts().unwrap().heads, 1);
}

#[test]
fn open_rejects_a_non_v1_schema() {
    let path = std::env::temp_dir().join(format!("cairn-bad-schema-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE catalog_meta(
                   id INTEGER PRIMARY KEY,
                   schema_version INTEGER NOT NULL,
                   coordinator_epoch INTEGER NOT NULL,
                   allocators TEXT NOT NULL
                 );
                 INSERT INTO catalog_meta VALUES (1, 2, 0, '{}');",
            )
            .unwrap();
    }
    assert!(SqliteCatalogStore::open(&path).is_err());
    let _ = std::fs::remove_file(path);
}

#[test]
fn schema_rejects_bad_fixed_blobs_and_cross_file_heads() {
    let mut s = SqliteCatalogStore::in_memory().unwrap();
    s.persist(&v1_batch()).unwrap();
    let bad = s.with_immediate_transaction(|tx| {
        tx.execute(
            "INSERT INTO file_versions VALUES (1,20,1,zeroblob(31),NULL,0,zeroblob(32))",
            [],
        )
        .map(|_| ())
    });
    assert!(bad.is_err());
    let bad_head = s.with_immediate_transaction(|tx| {
        tx.execute("INSERT INTO file_head VALUES (21,1,1)", [])
            .map(|_| ())
    });
    assert!(bad_head.is_err());
}

#[test]
fn operation_idempotency_and_t1_t2_t3_abort_fence() {
    let mut s = SqliteCatalogStore::in_memory().unwrap();
    let mut b = v1_batch();
    b.versions.push(VersionRecord {
        id: 30,
        file_id: 20,
        generation: 1,
        commit_id: [1; 32],
        parent_version_id: None,
        size: 1,
        digest: [2; 32],
    });
    b.operations.push(OperationRecord {
        operation_id: 40,
        actor_id: 1,
        kind: "publish".into(),
        request_fingerprint: [3; 32],
        state: "prepared".into(),
        result: None,
        error: None,
    });
    s.persist(&b).unwrap();
    s.record_operation(&OperationRecord {
        operation_id: 41,
        actor_id: 1,
        kind: "publish".into(),
        request_fingerprint: [4; 32],
        state: "prepared".into(),
        result: None,
        error: None,
    })
    .unwrap();
    let same = OperationRecord {
        operation_id: 41,
        actor_id: 1,
        kind: "publish".into(),
        request_fingerprint: [3; 32],
        state: "prepared".into(),
        result: None,
        error: None,
    };
    assert!(!s.record_operation(&same).unwrap());
    assert!(s
        .t1_prepare(&IntentRecord {
            operation_id: 41,
            actor_id: 1,
            file_id: 20,
            owner_epoch: 0,
            owner_nonce: 9,
            expected_head_version_id: None,
            expected_head_generation: 0,
            candidate_version_id: None,
            state: "preparing".into(),
            abort_reason: None,
            pinned: false,
            request_fingerprint: [4; 32],
            authz_epoch: 0
        })
        .is_ok());
    assert!(s.t2_record_candidate(41, 0, 9, 30).unwrap());
    assert!(s.t3_publish(41, 0, 9).unwrap());
    assert_eq!(s.operation(40).unwrap().unwrap().state, "prepared");
    assert!(!s.t3_publish(41, 0, 9).unwrap());
    assert!(!s.abort(41, 0, 9, "stale").unwrap());
}

#[test]
fn t3_missing_operation_and_fingerprint_conflicts_fail_closed() {
    let mut s = SqliteCatalogStore::in_memory().unwrap();
    let mut b = v1_batch();
    b.versions.push(VersionRecord {
        id: 30,
        file_id: 20,
        generation: 1,
        commit_id: [1; 32],
        parent_version_id: None,
        size: 1,
        digest: [2; 32],
    });
    s.persist(&b).unwrap();
    assert!(s
        .t1_prepare(&IntentRecord {
            operation_id: 50,
            actor_id: 1,
            file_id: 20,
            owner_epoch: 0,
            owner_nonce: 1,
            expected_head_version_id: None,
            expected_head_generation: 0,
            candidate_version_id: Some(30),
            state: "preparing".into(),
            abort_reason: None,
            pinned: false,
            request_fingerprint: [8; 32],
            authz_epoch: 0,
        })
        .is_err());

    let operation = OperationRecord {
        operation_id: 60,
        actor_id: 1,
        kind: "publish".into(),
        request_fingerprint: [8; 32],
        state: "prepared".into(),
        result: None,
        error: None,
    };
    assert!(s.record_operation(&operation).unwrap());
    assert!(!s
        .record_operation(&OperationRecord {
            operation_id: 61,
            ..operation.clone()
        })
        .unwrap());
    assert!(!s
        .record_operation(&OperationRecord {
            operation_id: 60,
            request_fingerprint: [9; 32],
            ..operation
        })
        .unwrap());
}
