use std::collections::HashMap;

use cairn_sim::replay::{
    decode_json, replay, CrashTiming, MutationPhase, RejectionReason, StepOutcome, StoreOp,
};
use cairn_sim::state_machine::{failure_artifact, failure_class, generate};

const SEEDS: u64 = 10_000;

#[test]
fn generated_single_node_corpus_is_deterministic() {
    let mut duplicate = false;
    let mut wrong_manifest_len = false;
    let mut accepted_generation = false;
    let mut rejected_generation = false;
    let mut no_crash = false;
    let mut crash_phases = [false; 5];
    let mut crash_timings = [false; 2];
    let mut crash_targets = [false; 3];

    for seed in 0..SEEDS {
        let case = generate(seed);
        let mut known_chunks = HashMap::<u8, Vec<u8>>::new();
        for operation in &case.operations {
            match operation {
                StoreOp::PutChunk { slot, bytes } => {
                    if known_chunks.values().any(|known| known == bytes) {
                        duplicate = true;
                    }
                    known_chunks.insert(*slot, bytes.clone());
                }
                StoreOp::PutManifest { chunks, .. } => {
                    wrong_manifest_len |= chunks.iter().any(|chunk| {
                        known_chunks
                            .get(&chunk.chunk_slot)
                            .is_some_and(|bytes| bytes.len() != chunk.len as usize)
                    });
                }
                StoreOp::CommitRoot { .. } | StoreOp::CrashReopen => {}
            }
        }

        if let Some(point) = &case.crash {
            crash_timings[usize::from(matches!(point.timing, CrashTiming::After))] = true;
            crash_phases[match point.phase {
                MutationPhase::RecordHeaderWrite => 0,
                MutationPhase::RecordPayloadWrite => 1,
                MutationPhase::RecordFlush => 2,
                MutationPhase::SuperblockWrite => 3,
                MutationPhase::SuperblockFlush => 4,
            }] = true;
            crash_targets[match case.operations[usize::from(point.step)] {
                StoreOp::PutChunk { .. } => 0,
                StoreOp::PutManifest { .. } => 1,
                StoreOp::CommitRoot { .. } => 2,
                StoreOp::CrashReopen => unreachable!("crash target cannot be reopen"),
            }] = true;
        } else {
            no_crash = true;
        }

        match replay(&case) {
            Ok(report) => {
                for step in report.steps {
                    match step.outcome {
                        StepOutcome::Accepted => accepted_generation = true,
                        StepOutcome::Rejected {
                            reason: RejectionReason::InvalidGeneration,
                        } => rejected_generation = true,
                        _ => {}
                    }
                }
            }
            Err(error) => {
                let class = failure_class(&error);
                let artifact = failure_artifact(&case, &error, |candidate| {
                    replay(candidate)
                        .as_ref()
                        .err()
                        .is_some_and(|candidate_error| failure_class(candidate_error) == class)
                });
                let minimized = decode_json(artifact.minimized_case.as_bytes())
                    .expect("failure artifact must be accepted by cairn-replay");
                assert_eq!(
                    failure_class(&replay(&minimized).expect_err("failure must still reproduce")),
                    class
                );
                panic!(
                    "generated seed {seed} found a replay failure: {error}\nminimized case:\n{}",
                    artifact.minimized_case
                );
            }
        }
    }

    assert!(duplicate, "corpus lost duplicate chunk writes");
    assert!(
        wrong_manifest_len,
        "corpus lost mis-sized manifest references"
    );
    assert!(accepted_generation, "corpus lost accepted generations");
    assert!(rejected_generation, "corpus lost rejected generations");
    assert!(no_crash, "corpus lost no-crash cases");
    assert!(
        crash_phases.into_iter().all(|seen| seen),
        "crash phase coverage regressed"
    );
    assert!(
        crash_timings.into_iter().all(|seen| seen),
        "crash timing coverage regressed"
    );
    assert!(
        crash_targets.into_iter().all(|seen| seen),
        "crash target coverage regressed"
    );
}
