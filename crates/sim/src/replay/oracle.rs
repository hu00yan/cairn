use std::collections::HashMap;
use std::fmt;

use super::{CrashTiming, MutationPhase, RejectionReason, ReplayCase, StepOutcome, StoreOp};

type ObjectId = [u8; 32];

const CHUNK_DOMAIN: &[u8] = b"cairn/chunk/v1\0";
const MANIFEST_DOMAIN: &[u8] = b"cairn/manifest/v1\0";

#[derive(Clone, Debug)]
pub(crate) struct OraclePlan {
    pub(crate) steps: Vec<OracleStep>,
    pub(crate) snapshot: OracleSnapshot,
}

#[derive(Clone, Debug)]
pub(crate) struct OracleStep {
    pub(crate) outcome: StepOutcome,
    pub(crate) chunk_id: Option<ObjectId>,
    pub(crate) manifest_id: Option<ObjectId>,
    pub(crate) root: Option<OracleRoot>,
}

fn oracle_step(outcome: StepOutcome) -> OracleStep {
    OracleStep {
        outcome,
        chunk_id: None,
        manifest_id: None,
        root: None,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OracleSnapshot {
    pub(crate) root: Option<OracleRoot>,
    pub(crate) known_chunks: HashMap<ObjectId, Vec<u8>>,
    pub(crate) visible_chunks: HashMap<ObjectId, Vec<u8>>,
    pub(crate) known_manifests: HashMap<ObjectId, OracleManifest>,
    pub(crate) visible_manifests: HashMap<ObjectId, OracleManifest>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OracleRoot {
    pub(crate) generation: u64,
    pub(crate) manifest: ObjectId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OracleManifest {
    pub(crate) chunks: Vec<OracleChunkRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OracleChunkRef {
    pub(crate) id: ObjectId,
    pub(crate) len: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OracleError(String);

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl OraclePlan {
    pub(crate) fn from_case(case: &ReplayCase) -> Result<Self, OracleError> {
        let mut state = OracleState::default();
        let crash = case.crash.as_ref();
        let mut steps = Vec::with_capacity(case.operations.len());

        for (step, operation) in case.operations.iter().enumerate() {
            if crash.is_some_and(|point| usize::from(point.step) == step) {
                let point = crash.expect("checked above");
                let mutations = state.mutation_count(operation)?;
                let phase = phase_index(operation, point.phase).ok_or_else(|| {
                    OracleError(format!(
                        "crash phase does not apply to operation at step {step}"
                    ))
                })?;
                if mutations == 0 || phase >= mutations {
                    return Err(OracleError(format!(
                        "crash point targets an operation with no mutation at step {step}"
                    )));
                }

                let before = state.clone();
                let execution = state.execute(operation)?;
                if !matches!(execution.outcome, StepOutcome::Accepted) {
                    return Err(OracleError(format!(
                        "crash target was not accepted at step {step}"
                    )));
                }
                let publishes = matches!(
                    (operation, point.phase, point.timing),
                    (
                        StoreOp::CommitRoot { .. },
                        MutationPhase::SuperblockFlush,
                        CrashTiming::After
                    )
                );
                if !publishes {
                    let known_chunks = state.known_chunks.clone();
                    let known_manifests = state.known_manifests.clone();
                    state = before;
                    state.known_chunks = known_chunks;
                    state.known_manifests = known_manifests;
                }
                steps.push(OracleStep {
                    outcome: StepOutcome::InjectedCrash,
                    ..execution
                });
            } else {
                steps.push(state.execute(operation)?);
            }
        }

        Ok(Self {
            steps,
            snapshot: state.snapshot(),
        })
    }
}

#[derive(Clone, Debug, Default)]
struct OracleState {
    chunk_slots: HashMap<u8, ObjectId>,
    manifest_slots: HashMap<u8, ObjectId>,
    pending_chunks: HashMap<ObjectId, Vec<u8>>,
    pending_manifests: HashMap<ObjectId, OracleManifest>,
    visible_chunks: HashMap<ObjectId, Vec<u8>>,
    visible_manifests: HashMap<ObjectId, OracleManifest>,
    known_chunks: HashMap<ObjectId, Vec<u8>>,
    known_manifests: HashMap<ObjectId, OracleManifest>,
    root: Option<OracleRoot>,
}

impl OracleState {
    fn execute(&mut self, operation: &StoreOp) -> Result<OracleStep, OracleError> {
        match operation {
            StoreOp::PutChunk { slot, bytes } => {
                let id = chunk_id(bytes);
                self.known_chunks.entry(id).or_insert_with(|| bytes.clone());
                self.chunk_slots.insert(*slot, id);
                if !self.pending_chunks.contains_key(&id) && !self.visible_chunks.contains_key(&id)
                {
                    self.pending_chunks.insert(id, bytes.clone());
                }
                let mut step = oracle_step(StepOutcome::Accepted);
                step.chunk_id = Some(id);
                Ok(step)
            }
            StoreOp::PutManifest { slot, chunks } => {
                let refs = chunks
                    .iter()
                    .map(|chunk| {
                        let id = *self.chunk_slots.get(&chunk.chunk_slot).ok_or_else(|| {
                            OracleError(format!("unknown chunk slot {}", chunk.chunk_slot))
                        })?;
                        Ok(OracleChunkRef { id, len: chunk.len })
                    })
                    .collect::<Result<Vec<_>, OracleError>>()?;
                let manifest = OracleManifest { chunks: refs };
                let id = manifest_id(&manifest.chunks);
                self.known_manifests.insert(id, manifest.clone());
                self.manifest_slots.insert(*slot, id);
                self.pending_manifests.insert(id, manifest);
                let mut step = oracle_step(StepOutcome::Accepted);
                step.manifest_id = Some(id);
                Ok(step)
            }
            StoreOp::CommitRoot {
                manifest_slot,
                generation,
            } => {
                if *generation == 0 || self.root.is_some_and(|root| *generation <= root.generation)
                {
                    return Ok(oracle_step(StepOutcome::Rejected {
                        reason: RejectionReason::InvalidGeneration,
                    }));
                }
                let Some(manifest_id) = self.manifest_slots.get(manifest_slot).copied() else {
                    return Ok(oracle_step(StepOutcome::Rejected {
                        reason: RejectionReason::InvalidManifest,
                    }));
                };
                let manifest = self
                    .pending_manifests
                    .get(&manifest_id)
                    .or_else(|| self.visible_manifests.get(&manifest_id))
                    .cloned();
                let Some(manifest) = manifest else {
                    return Ok(oracle_step(StepOutcome::Rejected {
                        reason: RejectionReason::InvalidManifest,
                    }));
                };
                if !manifest.chunks.iter().all(|chunk| {
                    self.pending_chunks
                        .get(&chunk.id)
                        .or_else(|| self.visible_chunks.get(&chunk.id))
                        .is_some_and(|bytes| bytes.len() == chunk.len as usize)
                }) {
                    return Ok(oracle_step(StepOutcome::Rejected {
                        reason: RejectionReason::InvalidManifest,
                    }));
                }
                self.visible_chunks.extend(self.pending_chunks.drain());
                self.visible_manifests
                    .extend(self.pending_manifests.drain());
                self.root = Some(OracleRoot {
                    generation: *generation,
                    manifest: manifest_id,
                });
                let mut step = oracle_step(StepOutcome::Accepted);
                step.root = self.root;
                Ok(step)
            }
            StoreOp::CrashReopen => {
                self.pending_chunks.clear();
                self.pending_manifests.clear();
                Ok(oracle_step(StepOutcome::Reopened))
            }
        }
    }

    fn mutation_count(&self, operation: &StoreOp) -> Result<usize, OracleError> {
        match operation {
            StoreOp::PutChunk { bytes, .. } => {
                let id = chunk_id(bytes);
                Ok(
                    if self.pending_chunks.contains_key(&id)
                        || self.visible_chunks.contains_key(&id)
                    {
                        0
                    } else {
                        3
                    },
                )
            }
            StoreOp::PutManifest { chunks, .. } => {
                for chunk in chunks {
                    if !self.chunk_slots.contains_key(&chunk.chunk_slot) {
                        return Err(OracleError(format!(
                            "unknown chunk slot {}",
                            chunk.chunk_slot
                        )));
                    }
                }
                Ok(3)
            }
            StoreOp::CommitRoot { .. } => {
                let mut candidate = self.clone();
                Ok(
                    if matches!(candidate.execute(operation)?.outcome, StepOutcome::Accepted) {
                        5
                    } else {
                        0
                    },
                )
            }
            StoreOp::CrashReopen => Ok(0),
        }
    }

    fn snapshot(self) -> OracleSnapshot {
        OracleSnapshot {
            root: self.root,
            known_chunks: self.known_chunks,
            visible_chunks: self.visible_chunks,
            known_manifests: self.known_manifests,
            visible_manifests: self.visible_manifests,
        }
    }
}

fn phase_index(operation: &StoreOp, phase: MutationPhase) -> Option<usize> {
    match operation {
        StoreOp::PutChunk { .. } | StoreOp::PutManifest { .. } => match phase {
            MutationPhase::RecordHeaderWrite => Some(0),
            MutationPhase::RecordPayloadWrite => Some(1),
            MutationPhase::RecordFlush => Some(2),
            MutationPhase::SuperblockWrite | MutationPhase::SuperblockFlush => None,
        },
        StoreOp::CommitRoot { .. } => match phase {
            MutationPhase::RecordHeaderWrite => Some(0),
            MutationPhase::RecordPayloadWrite => Some(1),
            MutationPhase::RecordFlush => Some(2),
            MutationPhase::SuperblockWrite => Some(3),
            MutationPhase::SuperblockFlush => Some(4),
        },
        StoreOp::CrashReopen => None,
    }
}

fn chunk_id(bytes: &[u8]) -> ObjectId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CHUNK_DOMAIN);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn manifest_id(chunks: &[OracleChunkRef]) -> ObjectId {
    let total_len = chunks.iter().map(|chunk| u64::from(chunk.len)).sum::<u64>();
    let mut body = Vec::with_capacity(16 + chunks.len() * 36);
    body.extend_from_slice(&1u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    body.extend_from_slice(&total_len.to_le_bytes());
    for chunk in chunks {
        body.extend_from_slice(&chunk.id);
        body.extend_from_slice(&chunk.len.to_le_bytes());
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(MANIFEST_DOMAIN);
    hasher.update(&body);
    *hasher.finalize().as_bytes()
}
