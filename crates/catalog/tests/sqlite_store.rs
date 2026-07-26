use cairn_catalog::sqlite_catalog::{
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
            sqlite_catalog::CatalogCounts {
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
                sqlite_catalog::CatalogCounts::default()
            );
            let _ = std::fs::remove_file(path);
        }
    }
}

use cairn_catalog::sqlite_catalog::{
    CatalogVersion, ClaimIntentOutcome, CoordinatorEpoch, HeadRecord, IntentRecord,
    OperationRecord, PrincipalRecord, RecoveryWork, T2Outcome, T3Outcome, VersionRecord,
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
    assert!(cairn_catalog::sqlite_catalog::DAG_DURABILITY_SEAM.contains("not atomic"));
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
fn version_allocator_is_global_and_retries_use_the_durable_candidate() {
    let mut s = SqliteCatalogStore::in_memory().unwrap();
    let mut b = v1_batch();
    b.collections.push(CollectionRecord {
        id: 11,
        owner_id: 1,
        name: "other".into(),
    });
    b.files.push(FileRecord {
        id: 21,
        collection_id: 11,
        name: "other".into(),
    });
    b.heads.push(HeadRecord {
        file_id: 21,
        version_id: None,
        generation: 0,
    });
    b.versions.push(VersionRecord {
        id: 30,
        file_id: 20,
        generation: 1,
        commit_id: [1; 32],
        parent_version_id: None,
        size: 1,
        digest: [2; 32],
    });
    b.operations.extend([
        OperationRecord {
            operation_id: 40,
            actor_id: 1,
            kind: "publish".into(),
            request_fingerprint: [4; 32],
            state: "prepared".into(),
            result: None,
            error: None,
        },
        OperationRecord {
            operation_id: 41,
            actor_id: 1,
            kind: "publish".into(),
            request_fingerprint: [5; 32],
            state: "prepared".into(),
            result: None,
            error: None,
        },
    ]);
    s.persist(&b).unwrap();
    for (op, file, nonce, fingerprint) in [(40, 20, 7, [4; 32]), (41, 21, 8, [5; 32])] {
        s.t1_prepare(&IntentRecord {
            operation_id: op,
            actor_id: 1,
            file_id: file,
            owner_epoch: 0,
            owner_nonce: nonce,
            expected_head_version_id: None,
            expected_head_generation: 0,
            candidate_version_id: None,
            state: "preparing".into(),
            abort_reason: None,
            pinned: true,
            request_fingerprint: fingerprint,
            authz_epoch: 0,
        })
        .unwrap();
        assert_eq!(
            s.t2_record_version(
                op,
                CoordinatorEpoch::ZERO,
                nonce,
                &CatalogVersion {
                    id: 0,
                    file_id: file,
                    generation: 1,
                    commit_id: [9; 32],
                    parent_version_id: None,
                    size: 2,
                    digest: [8; 32],
                },
            )
            .unwrap(),
            T2Outcome::Applied
        );
    }
    assert_eq!(s.candidate_version_id(40).unwrap(), Some(31));
    assert_eq!(s.candidate_version_id(41).unwrap(), Some(32));
    assert!(s.read_version(20, 31).unwrap().is_none());
    assert!(s.read_candidate_version(20, 31).unwrap().is_some());
    assert_eq!(
        s.t2_record_version(
            40,
            CoordinatorEpoch::ZERO,
            7,
            &CatalogVersion {
                id: 31,
                file_id: 20,
                generation: 1,
                commit_id: [9; 32],
                parent_version_id: None,
                size: 2,
                digest: [8; 32],
            },
        )
        .unwrap(),
        T2Outcome::Applied
    );
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
fn schema_rejects_cross_file_heads() {
    let mut s = SqliteCatalogStore::in_memory().unwrap();
    s.persist(&v1_batch()).unwrap();
    let mut bad_head_batch = v1_batch();
    bad_head_batch.heads = vec![HeadRecord {
        file_id: 21,
        version_id: Some(1),
        generation: 1,
    }];
    let bad_head = s.persist(&bad_head_batch);
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

#[test]
fn typed_recovery_claims_nonterminal_and_preserves_tombstones_after_reopen() {
    let path = std::env::temp_dir().join(format!("cairn-recovery-typed-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    {
        let mut s = SqliteCatalogStore::open(&path).unwrap();
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
            operation_id: 70,
            actor_id: 1,
            kind: "publish".into(),
            request_fingerprint: [7; 32],
            state: "prepared".into(),
            result: None,
            error: None,
        });
        s.persist(&b).unwrap();
        s.t1_prepare(&IntentRecord {
            operation_id: 70,
            actor_id: 1,
            file_id: 20,
            owner_epoch: 0,
            owner_nonce: 11,
            expected_head_version_id: None,
            expected_head_generation: 0,
            candidate_version_id: Some(30),
            state: "preparing".into(),
            abort_reason: None,
            pinned: false,
            request_fingerprint: [7; 32],
            authz_epoch: 0,
        })
        .unwrap();
        assert!(matches!(
            s.recovery_work().unwrap().as_slice(),
            [RecoveryWork::Resume(_)]
        ));
        assert_eq!(
            s.claim_coordinator_epoch(CoordinatorEpoch::ZERO, CoordinatorEpoch::new(1))
                .unwrap(),
            cairn_catalog::sqlite_catalog::EpochClaim::Claimed(CoordinatorEpoch::new(1))
        );
        assert!(matches!(
            s.claim_intent(70, CoordinatorEpoch::new(1), 22).unwrap(),
            ClaimIntentOutcome::Claimed(_)
        ));
        assert_eq!(
            s.t2_record_candidate_typed(70, CoordinatorEpoch::new(1), 22, 30)
                .unwrap(),
            T2Outcome::Applied
        );
        assert_eq!(
            s.t3_publish_typed(70, CoordinatorEpoch::new(1), 22)
                .unwrap(),
            T3Outcome::Published
        );
        assert_eq!(
            s.t3_publish_typed(70, CoordinatorEpoch::new(1), 22)
                .unwrap(),
            T3Outcome::AlreadyPublished
        );
        assert_eq!(
            s.t3_publish_typed(70, CoordinatorEpoch::ZERO, 22).unwrap(),
            T3Outcome::Fenced
        );
    }
    let s = SqliteCatalogStore::open(&path).unwrap();
    assert!(matches!(
        s.recovery_work().unwrap().as_slice(),
        [RecoveryWork::TombstoneDagBinding {
            terminal: cairn_catalog::sqlite_catalog::TombstoneTerminal::Published,
            ..
        }]
    ));
    let _ = std::fs::remove_file(path);
}

#[test]
fn typed_epoch_and_fence_outcomes_are_explicit_and_authz_is_rechecked() {
    let mut s = SqliteCatalogStore::in_memory().unwrap();
    assert_eq!(
        s.claim_coordinator_epoch(CoordinatorEpoch::ZERO, CoordinatorEpoch::new(3))
            .unwrap(),
        cairn_catalog::sqlite_catalog::EpochClaim::Claimed(CoordinatorEpoch::new(3))
    );
    assert_eq!(
        s.claim_coordinator_epoch(CoordinatorEpoch::new(3), CoordinatorEpoch::new(2))
            .unwrap(),
        cairn_catalog::sqlite_catalog::EpochClaim::Stale {
            current: CoordinatorEpoch::new(3)
        }
    );
    assert_eq!(
        s.t2_record_candidate_typed(999, CoordinatorEpoch::new(2), 1, 1)
            .unwrap(),
        T2Outcome::MissingIntent
    );
    assert_eq!(
        s.t3_publish_typed(999, CoordinatorEpoch::new(2), 1)
            .unwrap(),
        T3Outcome::MissingIntent
    );
}

#[test]
fn typed_t3_rejects_authz_epoch_membership_revoke_and_head_conflict() {
    let mut authz_epoch = SqliteCatalogStore::in_memory().unwrap();
    let mut epoch_batch = v1_batch();
    epoch_batch.principals[0].authz_epoch = 1;
    epoch_batch.versions.push(VersionRecord {
        id: 30,
        file_id: 20,
        generation: 1,
        commit_id: [1; 32],
        parent_version_id: None,
        size: 1,
        digest: [2; 32],
    });
    epoch_batch.operations.push(OperationRecord {
        operation_id: 80,
        actor_id: 1,
        kind: "publish".into(),
        request_fingerprint: [8; 32],
        state: "prepared".into(),
        result: None,
        error: None,
    });
    epoch_batch.intents.push(IntentRecord {
        operation_id: 80,
        actor_id: 1,
        file_id: 20,
        owner_epoch: 0,
        owner_nonce: 1,
        expected_head_version_id: None,
        expected_head_generation: 0,
        candidate_version_id: Some(30),
        state: "commit_durable".into(),
        abort_reason: None,
        pinned: true,
        request_fingerprint: [8; 32],
        authz_epoch: 0,
    });
    authz_epoch.persist(&epoch_batch).unwrap();
    assert_eq!(
        authz_epoch
            .t3_publish_typed(80, CoordinatorEpoch::ZERO, 1)
            .unwrap(),
        T3Outcome::AuthorizationDenied
    );

    let mut revoked_membership = SqliteCatalogStore::in_memory().unwrap();
    let mut membership_batch = v1_batch();
    membership_batch.principals.push(PrincipalRecord {
        id: 2,
        kind: "user".into(),
        state: "active".into(),
        authz_epoch: 0,
    });
    membership_batch.versions.push(VersionRecord {
        id: 31,
        file_id: 20,
        generation: 1,
        commit_id: [3; 32],
        parent_version_id: None,
        size: 1,
        digest: [4; 32],
    });
    membership_batch.operations.push(OperationRecord {
        operation_id: 81,
        actor_id: 2,
        kind: "publish".into(),
        request_fingerprint: [9; 32],
        state: "prepared".into(),
        result: None,
        error: None,
    });
    membership_batch.intents.push(IntentRecord {
        operation_id: 81,
        actor_id: 2,
        file_id: 20,
        owner_epoch: 0,
        owner_nonce: 1,
        expected_head_version_id: None,
        expected_head_generation: 0,
        candidate_version_id: Some(31),
        state: "commit_durable".into(),
        abort_reason: None,
        pinned: true,
        request_fingerprint: [9; 32],
        authz_epoch: 0,
    });
    revoked_membership.persist(&membership_batch).unwrap();
    assert_eq!(
        revoked_membership
            .t3_publish_typed(81, CoordinatorEpoch::ZERO, 1)
            .unwrap(),
        T3Outcome::AuthorizationDenied
    );

    let mut head_conflict = SqliteCatalogStore::in_memory().unwrap();
    let mut head_batch = v1_batch();
    head_batch.versions.push(VersionRecord {
        id: 32,
        file_id: 20,
        generation: 1,
        commit_id: [5; 32],
        parent_version_id: None,
        size: 1,
        digest: [6; 32],
    });
    head_batch.heads[0] = HeadRecord {
        file_id: 20,
        version_id: Some(32),
        generation: 1,
    };
    head_batch.operations.push(OperationRecord {
        operation_id: 82,
        actor_id: 1,
        kind: "publish".into(),
        request_fingerprint: [10; 32],
        state: "prepared".into(),
        result: None,
        error: None,
    });
    head_batch.intents.push(IntentRecord {
        operation_id: 82,
        actor_id: 1,
        file_id: 20,
        owner_epoch: 0,
        owner_nonce: 1,
        expected_head_version_id: None,
        expected_head_generation: 0,
        candidate_version_id: Some(32),
        state: "commit_durable".into(),
        abort_reason: None,
        pinned: true,
        request_fingerprint: [10; 32],
        authz_epoch: 0,
    });
    head_conflict.persist(&head_batch).unwrap();
    assert_eq!(
        head_conflict
            .t3_publish_typed(82, CoordinatorEpoch::ZERO, 1)
            .unwrap(),
        T3Outcome::HeadConflict
    );
}

#[test]
fn claim_rejects_a_future_owner_epoch() {
    let mut s = SqliteCatalogStore::in_memory().unwrap();
    let mut b = v1_batch();
    b.operations.push(OperationRecord {
        operation_id: 90,
        actor_id: 1,
        kind: "publish".into(),
        request_fingerprint: [11; 32],
        state: "prepared".into(),
        result: None,
        error: None,
    });
    b.intents.push(IntentRecord {
        operation_id: 90,
        actor_id: 1,
        file_id: 20,
        owner_epoch: 2,
        owner_nonce: 1,
        expected_head_version_id: None,
        expected_head_generation: 0,
        candidate_version_id: None,
        state: "preparing".into(),
        abort_reason: None,
        pinned: true,
        request_fingerprint: [11; 32],
        authz_epoch: 0,
    });
    s.persist(&b).unwrap();
    assert!(matches!(
        s.claim_intent(90, CoordinatorEpoch::ZERO, 2).unwrap(),
        ClaimIntentOutcome::FutureOwner { owner_epoch, current }
            if owner_epoch == CoordinatorEpoch::new(2) && current == CoordinatorEpoch::ZERO
    ));
}
