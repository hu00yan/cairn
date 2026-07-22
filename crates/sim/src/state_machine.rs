use crate::replay::{
    encode_json, ChunkSpec, CoreFailureKind, CrashPoint, CrashTiming, DeviceFailureKind, DiskSpec,
    DivergenceKind, LatencySpec, MutationPhase, ReplayCase, ReplayError, StoreOp,
};

const MAX_GENERATED_CHUNK: usize = 48;
const MAX_GENERATED_ROUNDS: usize = 6;

/// A small deterministic generator used by the simulator corpus.
///
/// This deliberately lives in the repository instead of depending on a random
/// number crate. A seed is enough to reproduce a case on every platform, and
/// the generated case is already a valid input for `cairn-replay`.
#[derive(Clone, Copy, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        debug_assert!(bound > 0);
        (self.next_u64() % bound as u64) as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureClass {
    InvalidCase,
    Decode,
    Encode,
    Divergence {
        step: usize,
        kind: DivergenceKind,
    },
    Core {
        kind: CoreFailureKind,
        device: Option<DeviceFailureKind>,
    },
    UntriggeredCrash,
}

pub fn failure_class(error: &ReplayError) -> FailureClass {
    match error {
        ReplayError::InvalidCase(_) => FailureClass::InvalidCase,
        ReplayError::Decode(_) => FailureClass::Decode,
        ReplayError::Encode(_) => FailureClass::Encode,
        ReplayError::Divergence { step, kind, .. } => FailureClass::Divergence {
            step: *step,
            kind: *kind,
        },
        ReplayError::Core { kind, device, .. } => FailureClass::Core {
            kind: *kind,
            device: *device,
        },
        ReplayError::UntriggeredCrash => FailureClass::UntriggeredCrash,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureArtifact {
    pub seed: u64,
    pub failure_class: FailureClass,
    pub error: String,
    pub minimized_case: String,
}

/// Generate a bounded single-node command trace.
pub fn generate(seed: u64) -> ReplayCase {
    let mut rng = SplitMix64::new(seed);
    let mut operations = Vec::new();
    let mut chunks = Vec::<(u8, usize)>::new();
    let mut payloads = Vec::<Vec<u8>>::new();

    let rounds = 2 + rng.below(MAX_GENERATED_ROUNDS - 1);
    for round in 0..rounds {
        let puts = 1 + rng.below(3);
        for put in 0..puts {
            let slot = ((round * 3 + put + rng.below(3)) % 12) as u8;
            let bytes =
                if !payloads.is_empty() && (seed % 5 + (round as u64 + put as u64) % 5) % 5 == 0 {
                    payloads[rng.below(payloads.len())].clone()
                } else {
                    let len = 1 + rng.below(MAX_GENERATED_CHUNK);
                    let mut bytes = Vec::with_capacity(len);
                    for index in 0..len {
                        bytes.push(
                            (seed as u8)
                                .wrapping_add((round * 17 + put * 5 + index) as u8)
                                .rotate_left((index % 7) as u32),
                        );
                    }
                    bytes
                };
            operations.push(StoreOp::PutChunk {
                slot,
                bytes: bytes.clone(),
            });
            payloads.push(bytes.clone());
            chunks.retain(|(old_slot, _)| *old_slot != slot);
            chunks.push((slot, bytes.len()));
        }

        let manifest_slot = (16 + round) as u8;
        let reference_count = rng.below(chunks.len().min(4) + 1);
        let mut refs = Vec::with_capacity(reference_count);
        for _ in 0..reference_count {
            let (chunk_slot, actual_len) = chunks[rng.below(chunks.len())];
            let len = if rng.below(4) == 0 {
                actual_len.saturating_add(1) as u32
            } else {
                actual_len as u32
            };
            refs.push(ChunkSpec { chunk_slot, len });
        }
        operations.push(StoreOp::PutManifest {
            slot: manifest_slot,
            chunks: refs,
        });

        let generation = match rng.below(5) {
            0 => 0,
            1 => 1,
            2 => 2,
            3 => u64::from(round as u16 + 1),
            _ => 100 + seed % 500,
        };
        operations.push(StoreOp::CommitRoot {
            manifest_slot,
            generation,
        });
    }

    // End every generated trace with a valid commit. This gives the crash
    // planner a mutation-rich operation to cut without relying on the random
    // prefix being accepted.
    let final_bytes = seed.to_le_bytes().into_iter().cycle().take(96).collect();
    let final_action = rng.below(3);
    operations.push(StoreOp::PutChunk {
        slot: 31,
        bytes: final_bytes,
    });
    if final_action >= 1 {
        operations.push(StoreOp::PutManifest {
            slot: 31,
            chunks: vec![ChunkSpec {
                chunk_slot: 31,
                len: 96,
            }],
        });
    }
    if final_action == 2 {
        operations.push(StoreOp::CommitRoot {
            manifest_slot: 31,
            generation: 1_000_000 + seed % 1_000,
        });
    }
    let final_step = operations.len() as u16 - 1;
    operations.push(StoreOp::CrashReopen);

    let crash = if seed % 3 != 0 {
        Some(CrashPoint {
            step: final_step,
            phase: match if final_action == 2 {
                rng.below(5)
            } else {
                rng.below(3)
            } {
                0 => MutationPhase::RecordHeaderWrite,
                1 => MutationPhase::RecordPayloadWrite,
                2 => MutationPhase::RecordFlush,
                3 => MutationPhase::SuperblockWrite,
                _ => MutationPhase::SuperblockFlush,
            },
            timing: if rng.below(2) == 0 {
                CrashTiming::Before
            } else {
                CrashTiming::After
            },
        })
    } else {
        None
    };

    ReplayCase {
        version: crate::replay::REPLAY_VERSION,
        seed: Some(seed),
        disk: DiskSpec {
            capacity_bytes: 128 * 1024,
            latency: LatencySpec {
                read_ticks: rng.below(4) as u64,
                write_ticks: rng.below(4) as u64,
                flush_data_ticks: rng.below(4) as u64,
                flush_all_ticks: rng.below(4) as u64,
                jitter_ticks: rng.below(4) as u64,
                seed,
            },
        },
        operations,
        crash,
    }
}

fn candidate_without(case: &ReplayCase, remove: usize) -> Option<ReplayCase> {
    if remove >= case.operations.len().saturating_sub(1)
        || case
            .crash
            .as_ref()
            .is_some_and(|point| usize::from(point.step) == remove)
    {
        return None;
    }
    let mut candidate = case.clone();
    candidate.operations.remove(remove);
    if let Some(point) = &mut candidate.crash {
        if usize::from(point.step) > remove {
            point.step -= 1;
        }
    }
    Some(candidate)
}

fn valid(candidate: &ReplayCase) -> bool {
    candidate.validate().is_ok()
}

/// Deterministically shrink a failing case while keeping it replayable.
///
/// The predicate must return true when the candidate still reproduces the
/// failure. The fixed ordering makes the minimized JSON stable across runs.
pub fn minimize<F>(original: &ReplayCase, mut reproduces: F) -> ReplayCase
where
    F: FnMut(&ReplayCase) -> bool,
{
    let mut case = original.clone();

    loop {
        let mut changed = false;
        for index in 0..case.operations.len().saturating_sub(1) {
            let Some(candidate) = candidate_without(&case, index) else {
                continue;
            };
            if valid(&candidate) && reproduces(&candidate) {
                case = candidate;
                changed = true;
                break;
            }
        }
        if !changed {
            break;
        }
    }

    loop {
        let mut changed = false;
        for index in 0..case.operations.len() {
            let replacement = match &case.operations[index] {
                StoreOp::PutChunk { slot, bytes } if !bytes.is_empty() => Some(StoreOp::PutChunk {
                    slot: *slot,
                    bytes: bytes[..bytes.len() / 2].to_vec(),
                }),
                StoreOp::CommitRoot { manifest_slot, .. } => Some(StoreOp::CommitRoot {
                    manifest_slot: *manifest_slot,
                    generation: 1,
                }),
                _ => None,
            };
            let Some(replacement) = replacement else {
                continue;
            };
            let mut candidate = case.clone();
            candidate.operations[index] = replacement;
            if valid(&candidate) && reproduces(&candidate) {
                case = candidate;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    case
}

pub fn failure_artifact<F>(case: &ReplayCase, error: &ReplayError, reproduces: F) -> FailureArtifact
where
    F: FnMut(&ReplayCase) -> bool,
{
    let minimized = minimize(case, reproduces);
    let minimized_case = String::from_utf8(
        encode_json(&minimized).expect("minimized state-machine case must remain valid"),
    )
    .expect("JSON is UTF-8");
    FailureArtifact {
        seed: case.seed.unwrap_or_default(),
        failure_class: failure_class(error),
        error: error.to_string(),
        minimized_case,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_fingerprint_keeps_distinct_roots_distinct() {
        let roots = [
            ReplayError::Core {
                kind: CoreFailureKind::Reopen,
                device: Some(DeviceFailureKind::Injected(
                    cairn_device::FaultKind::ShortIo,
                )),
                detail: "write failed: short IO".into(),
            },
            ReplayError::Core {
                kind: CoreFailureKind::ManifestProbe,
                device: Some(DeviceFailureKind::Injected(
                    cairn_device::FaultKind::MediaError,
                )),
                detail: "write failed: media error".into(),
            },
            ReplayError::Divergence {
                step: 1,
                kind: DivergenceKind::RootValues,
                detail: "root values differ".into(),
            },
            ReplayError::Divergence {
                step: 1,
                kind: DivergenceKind::ChunkIds,
                detail: "chunk IDs differ".into(),
            },
        ];
        for pair in roots.windows(2) {
            assert_ne!(failure_class(&pair[0]), failure_class(&pair[1]));
        }
    }

    #[test]
    fn generation_is_deterministic_and_replayable() {
        for seed in [0, 1, 2, 7, 41, 999, u64::MAX] {
            let first = generate(seed);
            assert_eq!(first, generate(seed));
            assert!(first.validate().is_ok(), "seed {seed}");
        }
    }

    #[test]
    fn shrinking_is_idempotent_for_a_synthetic_failure() {
        let original = generate(41);
        let first = minimize(&original, |case| case.operations.len() > 2);
        let second = minimize(&first, |case| case.operations.len() > 2);
        assert_eq!(first, second);
    }

    #[test]
    fn generation_has_cross_platform_golden_hashes() {
        let expected = [
            (
                0,
                "c12e5a292f2a30cd42dc2ce3b6b524f10ef2327d37f4b4eb6bc3d21ca5d7dd49",
            ),
            (
                1,
                "6819546d42f42ceeab5959b8898d1c7d8a1992cc858fea7ce2e69b3675c0849c",
            ),
            (
                41,
                "f6eb93ebaaf6950b532e50032542d7512b008227502dc0af5f95d145e44846c9",
            ),
            (
                u64::MAX,
                "236d3ba408a0166911b1794fc8a50f8d1760eca18a64f74178d31af18f8cc725",
            ),
        ];
        for (seed, expected) in expected {
            assert_eq!(
                blake3::hash(&encode_json(&generate(seed)).unwrap())
                    .to_hex()
                    .as_str(),
                expected
            );
        }
    }
}
