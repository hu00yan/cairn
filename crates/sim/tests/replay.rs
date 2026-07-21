use std::process::Command;

use cairn_sim::replay::{
    decode_json, encode_json, replay, CrashPoint, CrashTiming, DiskSpec, MutationPhase, ReplayCase,
    ReplayError, RootReport, StoreOp, MAX_REPLAY_INPUT_BYTES,
};

fn base_case() -> ReplayCase {
    ReplayCase {
        version: 1,
        seed: None,
        disk: DiskSpec::default(),
        operations: vec![
            StoreOp::PutChunk {
                slot: 0,
                bytes: b"hello".to_vec(),
            },
            StoreOp::PutManifest {
                slot: 0,
                chunks: vec![cairn_sim::replay::ChunkSpec {
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
        crash: None,
    }
}

#[test]
fn json_v1_round_trip_is_stable() {
    let case = base_case();
    let encoded = encode_json(&case).unwrap();
    assert_eq!(decode_json(&encoded).unwrap(), case);
    assert!(decode_json(&encoded).unwrap().validate().is_ok());
}

#[test]
fn replay_report_is_deterministic() {
    let case = base_case();
    assert_eq!(replay(&case).unwrap(), replay(&case).unwrap());
}

#[test]
fn nonzero_latency_is_included_in_the_final_report_snapshot() {
    let fast = replay(&base_case()).unwrap();
    let mut slow_case = base_case();
    slow_case.disk.latency = cairn_sim::replay::LatencySpec {
        read_ticks: 7,
        write_ticks: 11,
        flush_data_ticks: 13,
        flush_all_ticks: 17,
    };
    let slow = replay(&slow_case).unwrap();
    assert!(slow.virtual_time > fast.virtual_time);
    assert_eq!(slow, replay(&slow_case).unwrap());
}

#[test]
fn maximum_generation_remains_replayable() {
    let mut case = base_case();
    case.operations[2] = StoreOp::CommitRoot {
        manifest_slot: 0,
        generation: u64::MAX,
    };
    let report = replay(&case).unwrap();
    assert_eq!(
        report.recovered_root.as_ref().map(|root| root.generation),
        Some(u64::MAX)
    );
}

#[test]
fn transaction_crash_cut_matrix_preserves_the_right_root() {
    let phases = [
        MutationPhase::RecordHeaderWrite,
        MutationPhase::RecordPayloadWrite,
        MutationPhase::RecordFlush,
        MutationPhase::SuperblockWrite,
        MutationPhase::SuperblockFlush,
    ];
    for (step, step_phases) in [
        [
            MutationPhase::RecordHeaderWrite,
            MutationPhase::RecordPayloadWrite,
            MutationPhase::RecordFlush,
        ]
        .as_slice(),
        [
            MutationPhase::RecordHeaderWrite,
            MutationPhase::RecordPayloadWrite,
            MutationPhase::RecordFlush,
        ]
        .as_slice(),
        phases.as_slice(),
    ]
    .into_iter()
    .enumerate()
    {
        for &phase in step_phases.iter() {
            for timing in [CrashTiming::Before, CrashTiming::After] {
                let mut case = base_case();
                case.operations.truncate(step + 1);
                case.operations.push(StoreOp::CrashReopen);
                case.crash = Some(CrashPoint {
                    step: step as u16,
                    phase,
                    timing,
                });
                let report = replay(&case).unwrap();
                let published = step == 2
                    && phase == MutationPhase::SuperblockFlush
                    && timing == CrashTiming::After;
                assert_eq!(report.recovered_root.is_some(), published);
                let phase_offset = match phase {
                    MutationPhase::RecordHeaderWrite => 0,
                    MutationPhase::RecordPayloadWrite => 1,
                    MutationPhase::RecordFlush => 2,
                    MutationPhase::SuperblockWrite => 3,
                    MutationPhase::SuperblockFlush => 4,
                };
                let base_operation = [3u64, 6, 9][step];
                assert_eq!(
                    report.resolved_fault_op,
                    Some(base_operation + phase_offset.min(if step == 2 { 4 } else { 2 }))
                );
                assert_eq!(
                    report
                        .steps
                        .iter()
                        .filter(|step| step.outcome == cairn_sim::replay::StepOutcome::InjectedCrash)
                        .count(),
                    1
                );
                assert_eq!(
                    report
                        .steps
                        .iter()
                        .filter(|step| step.outcome == cairn_sim::replay::StepOutcome::Reopened)
                        .count(),
                    1
                );
                assert_eq!(report.faults_remaining, 0);
            }
        }
    }
}

fn two_generation_case() -> ReplayCase {
    ReplayCase {
        version: 1,
        seed: None,
        disk: DiskSpec::default(),
        operations: vec![
            StoreOp::PutChunk {
                slot: 0,
                bytes: b"alpha".to_vec(),
            },
            StoreOp::PutManifest {
                slot: 0,
                chunks: vec![cairn_sim::replay::ChunkSpec {
                    chunk_slot: 0,
                    len: 5,
                }],
            },
            StoreOp::CommitRoot {
                manifest_slot: 0,
                generation: 10,
            },
            StoreOp::PutChunk {
                slot: 1,
                bytes: b"bravo!".to_vec(),
            },
            StoreOp::PutManifest {
                slot: 1,
                chunks: vec![cairn_sim::replay::ChunkSpec {
                    chunk_slot: 1,
                    len: 6,
                }],
            },
            StoreOp::CommitRoot {
                manifest_slot: 1,
                generation: 20,
            },
            StoreOp::CrashReopen,
        ],
        crash: None,
    }
}

fn independent_chunk_id(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cairn/chunk/v1\0");
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn independent_manifest_id(chunk: [u8; 32], len: u32) -> [u8; 32] {
    let mut body = Vec::with_capacity(52);
    body.extend_from_slice(&1u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&u64::from(len).to_le_bytes());
    body.extend_from_slice(&chunk);
    body.extend_from_slice(&len.to_le_bytes());
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cairn/manifest/v1\0");
    hasher.update(&body);
    *hasher.finalize().as_bytes()
}

#[test]
fn two_generation_crash_cuts_use_independent_roots_and_operation_ids() {
    let phases = [
        [
            MutationPhase::RecordHeaderWrite,
            MutationPhase::RecordPayloadWrite,
            MutationPhase::RecordFlush,
        ]
        .as_slice(),
        [
            MutationPhase::RecordHeaderWrite,
            MutationPhase::RecordPayloadWrite,
            MutationPhase::RecordFlush,
        ]
        .as_slice(),
        [
            MutationPhase::RecordHeaderWrite,
            MutationPhase::RecordPayloadWrite,
            MutationPhase::RecordFlush,
            MutationPhase::SuperblockWrite,
            MutationPhase::SuperblockFlush,
        ]
        .as_slice(),
    ];
    let alpha = independent_chunk_id(b"alpha");
    let bravo = independent_chunk_id(b"bravo!");
    let old_manifest = independent_manifest_id(alpha, 5);
    let new_manifest = independent_manifest_id(bravo, 6);
    for (target_step, target_phases) in phases.into_iter().enumerate() {
        let step = target_step + 3;
        for &phase in target_phases {
            for timing in [CrashTiming::Before, CrashTiming::After] {
                let mut case = two_generation_case();
                case.operations.truncate(step + 1);
                case.operations.push(StoreOp::CrashReopen);
                case.crash = Some(CrashPoint {
                    step: step as u16,
                    phase,
                    timing,
                });
                let report = replay(&case).unwrap();
                let phase_offset = match phase {
                    MutationPhase::RecordHeaderWrite => 0,
                    MutationPhase::RecordPayloadWrite => 1,
                    MutationPhase::RecordFlush => 2,
                    MutationPhase::SuperblockWrite => 3,
                    MutationPhase::SuperblockFlush => 4,
                };
                let expected_op = [14u64, 17, 20][target_step] + phase_offset;
                let published = target_step == 2
                    && phase == MutationPhase::SuperblockFlush
                    && timing == CrashTiming::After;
                let expected_root = if published {
                    RootReport {
                        generation: 20,
                        manifest: new_manifest,
                    }
                } else {
                    RootReport {
                        generation: 10,
                        manifest: old_manifest,
                    }
                };
                assert_eq!(report.recovered_root, Some(expected_root));
                assert_eq!(report.resolved_fault_op, Some(expected_op));
                assert_eq!(
                    report.steps[step].outcome,
                    cairn_sim::replay::StepOutcome::InjectedCrash
                );
                assert_eq!(
                    report
                        .steps
                        .iter()
                        .filter(|step| step.outcome == cairn_sim::replay::StepOutcome::Reopened)
                        .count(),
                    1
                );
                assert_eq!(report, replay(&case).unwrap());
            }
        }
    }
}

#[test]
fn duplicate_puts_and_rejected_generations_do_not_hide_fault_cursor_errors() {
    let duplicate = ReplayCase {
        version: 1,
        seed: None,
        disk: DiskSpec::default(),
        operations: vec![
            StoreOp::PutChunk {
                slot: 0,
                bytes: b"same".to_vec(),
            },
            StoreOp::PutChunk {
                slot: 1,
                bytes: b"same".to_vec(),
            },
            StoreOp::PutManifest {
                slot: 0,
                chunks: vec![cairn_sim::replay::ChunkSpec {
                    chunk_slot: 0,
                    len: 4,
                }],
            },
            StoreOp::PutManifest {
                slot: 1,
                chunks: vec![cairn_sim::replay::ChunkSpec {
                    chunk_slot: 0,
                    len: 4,
                }],
            },
            StoreOp::CommitRoot {
                manifest_slot: 1,
                generation: 1,
            },
            StoreOp::CrashReopen,
        ],
        crash: Some(CrashPoint {
            step: 4,
            phase: MutationPhase::RecordHeaderWrite,
            timing: CrashTiming::Before,
        }),
    };
    let report = replay(&duplicate).unwrap();
    assert_eq!(report.resolved_fault_op, Some(12));
    assert_eq!(
        report.steps[1].outcome,
        cairn_sim::replay::StepOutcome::Accepted
    );
    let mut generations = two_generation_case();
    generations.operations.insert(
        3,
        StoreOp::CommitRoot {
            manifest_slot: 0,
            generation: 10,
        },
    );
    generations.operations.insert(
        4,
        StoreOp::CommitRoot {
            manifest_slot: 0,
            generation: 9,
        },
    );
    generations.operations.pop();
    generations.operations.push(StoreOp::CrashReopen);
    generations.crash = Some(CrashPoint {
        step: 7,
        phase: MutationPhase::SuperblockFlush,
        timing: CrashTiming::After,
    });
    let report = replay(&generations).unwrap();
    assert_eq!(
        report.steps[3].outcome,
        cairn_sim::replay::StepOutcome::Rejected {
            reason: cairn_sim::replay::RejectionReason::InvalidGeneration,
        }
    );
    assert_eq!(
        report.steps[4].outcome,
        cairn_sim::replay::StepOutcome::Rejected {
            reason: cairn_sim::replay::RejectionReason::InvalidGeneration,
        }
    );
    assert_eq!(report.resolved_fault_op, Some(24));
    assert_eq!(
        report.recovered_root.as_ref().map(|root| root.generation),
        Some(20)
    );
}

#[test]
fn invalid_manifest_can_be_visible_as_a_record_but_never_as_a_root() {
    let mut case = two_generation_case();
    case.operations[4] = StoreOp::PutManifest {
        slot: 1,
        chunks: vec![cairn_sim::replay::ChunkSpec {
            chunk_slot: 1,
            len: 5,
        }],
    };
    case.operations[5] = StoreOp::PutManifest {
        slot: 2,
        chunks: vec![cairn_sim::replay::ChunkSpec {
            chunk_slot: 0,
            len: 5,
        }],
    };
    case.operations.insert(
        6,
        StoreOp::CommitRoot {
            manifest_slot: 2,
            generation: 20,
        },
    );
    case.operations[7] = StoreOp::CrashReopen;
    let report = replay(&case).unwrap();
    assert_eq!(
        report.recovered_root.as_ref().map(|root| root.generation),
        Some(20)
    );
}

#[test]
fn bounded_sequences_are_replayable() {
    let mut rejected = 0;
    for seed in 0..256u16 {
        let case = generated_case(seed);
        let report = replay(&case).unwrap();
        rejected += report
            .steps
            .iter()
            .filter(|step| {
                matches!(
                    step.outcome,
                    cairn_sim::replay::StepOutcome::Rejected { .. }
                )
            })
            .count();
        assert_eq!(report, replay(&case).unwrap());
    }
    assert!(rejected > 0);
}

fn generated_case(seed: u16) -> ReplayCase {
    let chunk_count = usize::from(seed % 4) + 1;
    let mut lengths = Vec::with_capacity(chunk_count);
    let mut operations = Vec::new();
    for slot in 0..chunk_count {
        let len = usize::from(seed.wrapping_mul(7).wrapping_add(slot as u16) % 31) + 1;
        lengths.push(len);
        operations.push(StoreOp::PutChunk {
            slot: slot as u8,
            bytes: vec![seed as u8 ^ slot as u8; len],
        });
    }
    if seed % 3 == 0 {
        let len = usize::from(seed % 17) + 1;
        lengths[0] = len;
        operations.push(StoreOp::PutChunk {
            slot: 0,
            bytes: vec![seed as u8; len],
        });
    }

    let manifest_len = usize::from(seed % chunk_count as u16) + 1;
    let manifest_chunks = (0..manifest_len)
        .map(|slot| cairn_sim::replay::ChunkSpec {
            chunk_slot: slot as u8,
            len: lengths[slot] as u32 + if seed % 7 == 0 && slot == 0 { 1 } else { 0 },
        })
        .collect();
    operations.push(StoreOp::PutManifest {
        slot: 0,
        chunks: manifest_chunks,
    });
    operations.push(StoreOp::CommitRoot {
        manifest_slot: 0,
        generation: 1,
    });
    if seed % 5 == 0 {
        operations.push(StoreOp::CommitRoot {
            manifest_slot: 0,
            generation: 1,
        });
    }
    if seed % 2 == 0 {
        operations.push(StoreOp::PutManifest {
            slot: 1,
            chunks: vec![cairn_sim::replay::ChunkSpec {
                chunk_slot: 0,
                len: lengths[0] as u32,
            }],
        });
        operations.push(StoreOp::CommitRoot {
            manifest_slot: 1,
            generation: 2,
        });
    }
    operations.push(StoreOp::CrashReopen);
    ReplayCase {
        version: 1,
        seed: Some(u64::from(seed)),
        disk: DiskSpec::default(),
        operations,
        crash: None,
    }
}

#[test]
fn validation_rejects_capacity_slot_reference_and_phase_errors() {
    let mut case = base_case();
    case.disk.capacity_bytes = 16 * 1024;
    if let StoreOp::PutChunk { bytes, .. } = &mut case.operations[0] {
        *bytes = vec![0; 4096];
    }
    case.operations.insert(
        1,
        StoreOp::PutChunk {
            slot: 1,
            bytes: vec![0; 4096],
        },
    );
    assert!(matches!(
        case.validate(),
        Err(ReplayError::InvalidCase(detail)) if detail.contains("capacity")
    ));

    let mut case = base_case();
    if let StoreOp::PutChunk { slot, .. } = &mut case.operations[0] {
        *slot = 255;
    }
    assert!(matches!(
        case.validate(),
        Err(ReplayError::InvalidCase(detail)) if detail.contains("slot")
    ));

    let mut case = base_case();
    if let StoreOp::PutManifest { chunks, .. } = &mut case.operations[1] {
        chunks[0].chunk_slot = 3;
    }
    assert!(matches!(
        case.validate(),
        Err(ReplayError::InvalidCase(detail)) if detail.contains("unknown chunk slot")
    ));

    let mut case = base_case();
    case.operations.truncate(1);
    case.operations.push(StoreOp::CrashReopen);
    case.crash = Some(CrashPoint {
        step: 0,
        phase: MutationPhase::SuperblockFlush,
        timing: CrashTiming::Before,
    });
    assert!(matches!(
        case.validate(),
        Err(ReplayError::InvalidCase(detail)) if detail.contains("does not apply")
    ));
}

#[test]
fn standalone_runner_reports_success_and_invalid_input() {
    let fixture = include_str!("fixtures/v1-crash-after-superblock-flush.json");
    let output = Command::new(env!("CARGO_BIN_EXE_cairn-replay"))
        .arg("-")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            let mut stdin = child.stdin.take().unwrap();
            stdin.write_all(fixture.as_bytes())?;
            drop(stdin);
            child.wait_with_output()
        })
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout)
        .unwrap()
        .contains("resolved_fault_op"));

    let output = Command::new(env!("CARGO_BIN_EXE_cairn-replay"))
        .arg("-")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            let mut stdin = child.stdin.take().unwrap();
            stdin.write_all(b"{}")?;
            drop(stdin);
            child.wait_with_output()
        })
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn rejected_steps_record_matching_reasons() {
    let mut case = base_case();
    case.operations.insert(
        3,
        StoreOp::CommitRoot {
            manifest_slot: 0,
            generation: 1,
        },
    );
    let report = replay(&case).unwrap();
    assert!(report.steps.iter().any(|step| {
        step.outcome
            == cairn_sim::replay::StepOutcome::Rejected {
                reason: cairn_sim::replay::RejectionReason::InvalidGeneration,
            }
    }));
}

#[test]
fn invalid_cases_are_rejected_before_execution() {
    let mut case = base_case();
    case.version = 99;
    assert!(matches!(replay(&case), Err(ReplayError::InvalidCase(_))));

    let mut encoded = String::from_utf8(encode_json(&base_case()).unwrap()).unwrap();
    encoded.insert(encoded.len() - 1, ',');
    encoded.insert_str(encoded.len() - 1, "\"unknown\": true");
    assert!(matches!(
        decode_json(encoded.as_bytes()),
        Err(ReplayError::Decode(_))
    ));
    assert!(matches!(
        decode_json(&vec![b' '; MAX_REPLAY_INPUT_BYTES + 1]),
        Err(ReplayError::Decode(detail)) if detail.contains("input exceeds")
    ));
}

#[test]
fn standalone_runner_accepts_a_fixture() {
    let fixture = include_str!("fixtures/v1-basic.json");
    let output = Command::new(env!("CARGO_BIN_EXE_cairn-replay"))
        .arg("-")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            let mut stdin = child.stdin.take().unwrap();
            stdin.write_all(fixture.as_bytes())?;
            drop(stdin);
            child.wait_with_output()
        })
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout)
        .unwrap()
        .contains("recovered_root"));

    let output = Command::new(env!("CARGO_BIN_EXE_cairn-replay"))
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/v1-basic.json"
        ))
        .output()
        .unwrap();
    assert!(output.status.success());
}
