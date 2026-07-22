use cairn_device::{
    DeviceEffect, DeviceEventKind, DeviceRule, DeviceScript, EventOccurrence, EventSelector,
    LatencyProfile,
};
use cairn_sim::replay::{
    decode_json, encode_json, replay, ChunkSpec, DiskSpec, ReplayCase, StoreOp,
};

fn case_with_script(script: DeviceScript) -> ReplayCase {
    ReplayCase {
        version: 1,
        seed: Some(7),
        disk: DiskSpec {
            capacity_bytes: 64 * 1024,
            script,
        },
        operations: vec![
            StoreOp::PutChunk {
                slot: 0,
                bytes: b"hello".to_vec(),
            },
            StoreOp::PutManifest {
                slot: 0,
                chunks: vec![ChunkSpec {
                    chunk_slot: 0,
                    len: 5,
                }],
            },
            StoreOp::CommitRoot {
                manifest_slot: 0,
                generation: 1,
            },
            StoreOp::CrashReopen,
        ],
    }
}

#[test]
fn replay_round_trip_and_report_are_deterministic() {
    let case = case_with_script(DeviceScript::default());
    let encoded = encode_json(&case).unwrap();
    assert_eq!(decode_json(&encoded).unwrap(), case);
    assert_eq!(replay(&case).unwrap(), replay(&case).unwrap());
}

#[test]
fn replay_uses_device_latency_and_reports_trace_size() {
    let case = case_with_script(DeviceScript {
        latency: LatencyProfile {
            write_ticks: 3,
            flush_data_ticks: 5,
            flush_all_ticks: 7,
            ..Default::default()
        },
        ..Default::default()
    });
    let report = replay(&case).unwrap();
    assert!(report.device_events > 0);
    assert!(report.virtual_time > 0);
}

#[test]
fn replay_accepts_device_range_faults_without_operation_ids() {
    let case = case_with_script(DeviceScript {
        rules: vec![DeviceRule {
            selector: EventSelector {
                kind: DeviceEventKind::FlushData,
                occurrence: EventOccurrence::Every {
                    first: 0,
                    period: 2,
                },
                range: None,
            },
            effect: DeviceEffect::Drop,
        }],
        ..Default::default()
    });
    let result = replay(&case);
    assert!(result.is_ok() || matches!(result, Err(cairn_sim::replay::ReplayError::Core { .. })));
}

#[test]
fn replay_recovers_after_a_scripted_device_crash() {
    let case = case_with_script(DeviceScript {
        rules: vec![DeviceRule {
            selector: EventSelector {
                kind: DeviceEventKind::Write,
                occurrence: EventOccurrence::Exact(13),
                range: None,
            },
            effect: DeviceEffect::CrashBefore,
        }],
        ..Default::default()
    });
    let report = replay(&case).unwrap();
    assert!(report
        .steps
        .iter()
        .any(|step| { step.outcome == cairn_sim::replay::StepOutcome::InjectedCrash }));
    assert_eq!(report.recovered_root, None);
}

#[test]
fn replay_preserves_a_root_published_before_a_flush_crash() {
    let case = case_with_script(DeviceScript {
        rules: vec![DeviceRule {
            selector: EventSelector {
                kind: DeviceEventKind::FlushAll,
                occurrence: EventOccurrence::Exact(19),
                range: None,
            },
            effect: DeviceEffect::CrashAfter,
        }],
        ..Default::default()
    });
    let report = replay(&case).unwrap();
    assert_eq!(report.recovered_root.unwrap().generation, 1);
}

#[test]
fn replay_accepts_a_complete_torn_superblock_before_power_loss() {
    let mut case = decode_json(include_bytes!(
        "fixtures/v1-crash-after-superblock-flush.json"
    ))
    .unwrap();
    case.disk.script.rules[0].selector.kind = DeviceEventKind::Write;
    case.disk.script.rules[0].selector.occurrence = EventOccurrence::Exact(18);
    case.disk.script.rules[0].effect = DeviceEffect::TearAndCrashAfter {
        durable_prefix: 128,
    };
    let report = replay(&case).unwrap();
    assert_eq!(report.recovered_root.unwrap().generation, 1);
}

#[test]
fn replay_rejects_an_uncommitted_chunk_exposed_after_recovery() {
    let mut case = case_with_script(DeviceScript {
        rules: vec![DeviceRule {
            selector: EventSelector {
                kind: DeviceEventKind::FlushData,
                occurrence: EventOccurrence::Exact(5),
                range: None,
            },
            effect: DeviceEffect::CrashAfter,
        }],
        ..Default::default()
    });
    case.operations.truncate(1);
    case.operations.push(StoreOp::CrashReopen);
    let report = replay(&case).unwrap();
    assert_eq!(report.recovered_root, None);
}

#[test]
fn replay_rejects_an_uncommitted_manifest_exposed_without_a_root() {
    let mut case = case_with_script(DeviceScript {
        rules: vec![DeviceRule {
            selector: EventSelector {
                kind: DeviceEventKind::FlushData,
                occurrence: EventOccurrence::Exact(8),
                range: None,
            },
            effect: DeviceEffect::CrashAfter,
        }],
        ..Default::default()
    });
    case.operations.truncate(2);
    case.operations.push(StoreOp::CrashReopen);
    let report = replay(&case).unwrap();
    assert_eq!(report.recovered_root, None);
}

#[test]
fn replay_rejects_an_untriggered_device_rule() {
    let case = case_with_script(DeviceScript {
        rules: vec![DeviceRule {
            selector: EventSelector {
                kind: DeviceEventKind::Write,
                occurrence: EventOccurrence::Exact(999),
                range: None,
            },
            effect: DeviceEffect::CrashBefore,
        }],
        ..Default::default()
    });
    assert!(matches!(
        replay(&case),
        Err(cairn_sim::replay::ReplayError::InvalidCase(detail))
            if detail.contains("rule 0 was not triggered")
    ));
}

#[test]
fn replay_preserves_an_earlier_device_error_over_later_untriggered_rules() {
    let case = case_with_script(DeviceScript {
        bad_ranges: vec![cairn_device::ByteRange { offset: 0, len: 4 }],
        rules: vec![DeviceRule {
            selector: EventSelector {
                kind: DeviceEventKind::Write,
                occurrence: EventOccurrence::Exact(999),
                range: None,
            },
            effect: DeviceEffect::CrashBefore,
        }],
        ..Default::default()
    });
    assert!(matches!(
        replay(&case),
        Err(cairn_sim::replay::ReplayError::Core {
            kind: cairn_sim::replay::CoreFailureKind::Format,
            device: Some(cairn_sim::replay::DeviceFailureKind::Injected(
                cairn_device::FaultKind::MediaError
            )),
            ..
        })
    ));
}
