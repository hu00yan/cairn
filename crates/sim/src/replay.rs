use std::collections::{HashMap, HashSet};
use std::fmt;

use cairn_core::{
    ChunkRef as CoreChunkRef, Error as CoreError, Root as CoreRoot, Store, RECORDS_START,
};
use cairn_device::{DeviceError, Fault, LatencyProfile, SimConfig, SimDisk};
use cairn_model::{ChunkRef as ModelChunkRef, Model};
use serde::{Deserialize, Serialize};

mod oracle;

use oracle::{OraclePlan, OracleSnapshot};

pub const REPLAY_VERSION: u16 = 1;
pub const MAX_REPLAY_INPUT_BYTES: usize = 1024 * 1024;
const MIN_CAPACITY: u32 = 16 * 1024;
const MAX_CAPACITY: u32 = 1024 * 1024;
const MAX_OPERATIONS: usize = 64;
const MAX_SLOTS: usize = 32;
const MAX_PAYLOAD: usize = 4 * 1024;
const MAX_TOTAL_PAYLOAD: usize = 64 * 1024;
const MAX_MANIFEST_CHUNKS: usize = 8;
const FORMAT_MUTATIONS: u64 = 3;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReplayCase {
    pub version: u16,
    #[serde(default)]
    pub seed: Option<u64>,
    pub disk: DiskSpec,
    pub operations: Vec<StoreOp>,
    #[serde(default)]
    pub crash: Option<CrashPoint>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DiskSpec {
    pub capacity_bytes: u32,
    #[serde(default)]
    pub latency: LatencySpec,
}

impl Default for DiskSpec {
    fn default() -> Self {
        Self {
            capacity_bytes: 256 * 1024,
            latency: LatencySpec::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LatencySpec {
    #[serde(default)]
    pub read_ticks: u64,
    #[serde(default)]
    pub write_ticks: u64,
    #[serde(default)]
    pub flush_data_ticks: u64,
    #[serde(default)]
    pub flush_all_ticks: u64,
    #[serde(default)]
    pub jitter_ticks: u64,
    #[serde(default)]
    pub seed: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreOp {
    PutChunk { slot: u8, bytes: Vec<u8> },
    PutManifest { slot: u8, chunks: Vec<ChunkSpec> },
    CommitRoot { manifest_slot: u8, generation: u64 },
    CrashReopen,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ChunkSpec {
    pub chunk_slot: u8,
    pub len: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CrashPoint {
    pub step: u16,
    pub phase: MutationPhase,
    pub timing: CrashTiming,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MutationPhase {
    RecordHeaderWrite,
    RecordPayloadWrite,
    RecordFlush,
    SuperblockWrite,
    SuperblockFlush,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CrashTiming {
    Before,
    After,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct ReplayReport {
    pub version: u16,
    pub steps: Vec<StepReport>,
    pub recovered_root: Option<RootReport>,
    pub resolved_fault_op: Option<u64>,
    pub durable_digest: [u8; 32],
    pub pending_writes: usize,
    pub op_index: u64,
    pub faults_remaining: usize,
    pub virtual_time: u64,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct StepReport {
    pub step: u16,
    pub outcome: StepOutcome,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StepOutcome {
    Accepted,
    Rejected { reason: RejectionReason },
    InjectedCrash,
    Reopened,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    Capacity,
    InvalidInput,
    InvalidGeneration,
    InvalidManifest,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct RootReport {
    pub generation: u64,
    pub manifest: [u8; 32],
}

#[derive(Debug, Eq, PartialEq)]
pub enum ReplayError {
    InvalidCase(String),
    Decode(String),
    Encode(String),
    Divergence {
        step: usize,
        kind: DivergenceKind,
        detail: String,
    },
    Core {
        kind: CoreFailureKind,
        device: Option<DeviceFailureKind>,
        detail: String,
    },
    UntriggeredCrash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DivergenceKind {
    ChunkIds,
    OracleChunkId,
    ModelRejectedChunk,
    CoreRejectedChunk,
    ManifestIds,
    OracleManifestId,
    ModelRejectedManifest,
    CoreRejectedManifest,
    ModelRejectedRoot,
    CoreRejectedRoot,
    OracleStepOutcome,
    UnexpectedRejection,
    RejectionReasons,
    ModelPlanningChunk,
    ModelPlanningManifest,
    RootValues,
    OracleRootMissing,
    OracleRootValues,
    RecoveredRoots,
    RecoveredChunkVisibility,
    RecoveredManifestVisibility,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreFailureKind {
    Format,
    Reopen,
    FinalReportReopen,
    ReadAfterRecovery,
    ManifestProbeOpen,
    ManifestProbe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeviceFailureKind {
    OutOfBounds,
    InvalidConfig,
    Io,
    Injected(cairn_device::FaultKind),
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCase(detail) => write!(f, "invalid replay case: {detail}"),
            Self::Decode(detail) => write!(f, "replay decode failed: {detail}"),
            Self::Encode(detail) => write!(f, "replay encode failed: {detail}"),
            Self::Divergence { step, detail, .. } => {
                write!(f, "core/model divergence at step {step}: {detail}")
            }
            Self::Core { detail, .. } => write!(f, "core recovery failed: {detail}"),
            Self::UntriggeredCrash => write!(f, "configured crash point was not triggered"),
        }
    }
}

impl std::error::Error for ReplayError {}

impl ReplayCase {
    pub fn validate(&self) -> Result<(), ReplayError> {
        if self.version != REPLAY_VERSION {
            return Err(ReplayError::InvalidCase(format!(
                "unsupported version {}, expected {REPLAY_VERSION}",
                self.version
            )));
        }
        if !(MIN_CAPACITY..=MAX_CAPACITY).contains(&self.disk.capacity_bytes) {
            return Err(ReplayError::InvalidCase(format!(
                "capacity must be between {MIN_CAPACITY} and {MAX_CAPACITY} bytes"
            )));
        }
        if self.operations.is_empty() || self.operations.len() > MAX_OPERATIONS {
            return Err(ReplayError::InvalidCase(format!(
                "operations must contain 1..={MAX_OPERATIONS} entries"
            )));
        }
        if !matches!(self.operations.last(), Some(StoreOp::CrashReopen)) {
            return Err(ReplayError::InvalidCase(
                "last operation must be crash_reopen".into(),
            ));
        }
        if self
            .operations
            .iter()
            .enumerate()
            .any(|(index, operation)| {
                index + 1 != self.operations.len() && matches!(operation, StoreOp::CrashReopen)
            })
        {
            return Err(ReplayError::InvalidCase(
                "crash_reopen may only appear as the final operation".into(),
            ));
        }
        if self.crash.as_ref().is_some_and(|point| {
            usize::from(point.step) >= self.operations.len() - 1
                || matches!(
                    self.operations[usize::from(point.step)],
                    StoreOp::CrashReopen
                )
                || usize::from(point.step) + 1 != self.operations.len() - 1
        }) {
            return Err(ReplayError::InvalidCase(
                "crash point must target the operation immediately before crash_reopen".into(),
            ));
        }

        let mut slots = HashSet::new();
        let mut defined_chunks = HashSet::new();
        let mut defined_manifests = HashSet::new();
        let mut total_payload = 0usize;
        let mut required_capacity = RECORDS_START;
        for (step, operation) in self.operations.iter().enumerate() {
            match operation {
                StoreOp::PutChunk { slot, bytes } => {
                    validate_slot(*slot, "chunk", step)?;
                    slots.insert((0, *slot));
                    defined_chunks.insert(*slot);
                    if bytes.len() > MAX_PAYLOAD {
                        return Err(ReplayError::InvalidCase(
                            "chunk payload exceeds 4 KiB".into(),
                        ));
                    }
                    total_payload = total_payload
                        .checked_add(bytes.len())
                        .ok_or_else(|| ReplayError::InvalidCase("payload size overflow".into()))?;
                    required_capacity = required_capacity_for_record(
                        required_capacity,
                        32usize.checked_add(bytes.len()).ok_or_else(|| {
                            ReplayError::InvalidCase("chunk record size overflow".into())
                        })?,
                    )?;
                }
                StoreOp::PutManifest { slot, chunks } => {
                    validate_slot(*slot, "manifest", step)?;
                    slots.insert((1, *slot));
                    defined_manifests.insert(*slot);
                    if chunks.len() > MAX_MANIFEST_CHUNKS {
                        return Err(ReplayError::InvalidCase(
                            "manifest contains too many chunks".into(),
                        ));
                    }
                    for chunk in chunks {
                        validate_slot(chunk.chunk_slot, "chunk", step)?;
                        if !defined_chunks.contains(&chunk.chunk_slot) {
                            return Err(ReplayError::InvalidCase(format!(
                                "step {step} references unknown chunk slot {}",
                                chunk.chunk_slot
                            )));
                        }
                        slots.insert((0, chunk.chunk_slot));
                    }
                    let payload = 32usize
                        .checked_add(16)
                        .and_then(|size| size.checked_add(chunks.len().checked_mul(36)?))
                        .ok_or_else(|| {
                            ReplayError::InvalidCase("manifest record size overflow".into())
                        })?;
                    required_capacity = required_capacity_for_record(required_capacity, payload)?;
                }
                StoreOp::CommitRoot { manifest_slot, .. } => {
                    validate_slot(*manifest_slot, "manifest", step)?;
                    if !defined_manifests.contains(manifest_slot) {
                        return Err(ReplayError::InvalidCase(format!(
                            "step {step} references unknown manifest slot {manifest_slot}"
                        )));
                    }
                    slots.insert((1, *manifest_slot));
                    required_capacity = required_capacity_for_record(required_capacity, 40)?;
                }
                StoreOp::CrashReopen => {}
            }
        }
        if slots.len() > MAX_SLOTS {
            return Err(ReplayError::InvalidCase(format!(
                "case references more than {MAX_SLOTS} slots"
            )));
        }
        if total_payload > MAX_TOTAL_PAYLOAD {
            return Err(ReplayError::InvalidCase(
                "case payload exceeds 64 KiB".into(),
            ));
        }
        if u64::from(self.disk.capacity_bytes) < required_capacity {
            return Err(ReplayError::InvalidCase(format!(
                "capacity {} is too small for the case's worst-case record footprint {required_capacity}",
                self.disk.capacity_bytes
            )));
        }
        OraclePlan::from_case(self).map_err(|error| ReplayError::InvalidCase(error.to_string()))?;
        Ok(())
    }
}

pub fn decode_json(input: &[u8]) -> Result<ReplayCase, ReplayError> {
    if input.len() > MAX_REPLAY_INPUT_BYTES {
        return Err(ReplayError::Decode(format!(
            "input exceeds {MAX_REPLAY_INPUT_BYTES} bytes"
        )));
    }
    let case: ReplayCase =
        serde_json::from_slice(input).map_err(|error| ReplayError::Decode(error.to_string()))?;
    case.validate()?;
    Ok(case)
}

pub fn encode_json(case: &ReplayCase) -> Result<Vec<u8>, ReplayError> {
    case.validate()?;
    serde_json::to_vec_pretty(case).map_err(|error| ReplayError::Encode(error.to_string()))
}

pub fn encode_report(report: &ReplayReport) -> Result<Vec<u8>, ReplayError> {
    serde_json::to_vec_pretty(report).map_err(|error| ReplayError::Encode(error.to_string()))
}

pub fn replay(case: &ReplayCase) -> Result<ReplayReport, ReplayError> {
    case.validate()?;
    let oracle =
        OraclePlan::from_case(case).map_err(|error| ReplayError::InvalidCase(error.to_string()))?;
    let fault = resolve_fault(case)?;
    let resolved_fault_op = fault.as_ref().map(fault_operation);
    let config = SimConfig {
        atomic_write_size: 1,
        allow_reordering: false,
        allow_torn_writes: false,
        latency: LatencyProfile {
            read_ticks: case.disk.latency.read_ticks,
            write_ticks: case.disk.latency.write_ticks,
            flush_data_ticks: case.disk.latency.flush_data_ticks,
            flush_all_ticks: case.disk.latency.flush_all_ticks,
            jitter_ticks: case.disk.latency.jitter_ticks,
            seed: case.disk.latency.seed,
        },
    };
    let disk = SimDisk::new(case.disk.capacity_bytes as usize, config);
    let disk = match fault {
        Some(Fault::CrashBeforeOp(op)) => disk.with_fault(Fault::CrashBeforeOp(op)),
        Some(Fault::CrashAfterOp(op)) => disk.with_fault(Fault::CrashAfterOp(op)),
        None => disk,
        _ => unreachable!("resolve_fault only returns crash faults"),
    };
    let mut core = Store::format(disk).map_err(|error| ReplayError::Core {
        kind: CoreFailureKind::Format,
        device: device_failure_kind(&error),
        detail: format!("{error:?}"),
    })?;
    let mut model = Model::default();
    let mut chunk_slots = HashMap::<u8, [u8; 32]>::new();
    let mut manifest_slots = HashMap::<u8, [u8; 32]>::new();
    let mut reports = Vec::with_capacity(case.operations.len());
    let mut fault_triggered = false;
    let mut recovery_required = false;

    for (index, operation) in case.operations.iter().enumerate() {
        if recovery_required && !matches!(operation, StoreOp::CrashReopen) {
            return Err(ReplayError::InvalidCase(
                "only crash_reopen is allowed after an injected crash".into(),
            ));
        }
        let outcome = match operation {
            StoreOp::PutChunk { slot, bytes } => {
                let core_result = core.put_bytes(bytes);
                if is_injected_crash(&core_result) {
                    fault_triggered = true;
                    recovery_required = true;
                    StepOutcome::InjectedCrash
                } else {
                    let model_result = model.put_bytes(bytes.clone());
                    match (core_result, model_result) {
                        (Ok(core_id), Ok(model_id)) => {
                            if core_id != model_id {
                                return Err(divergence(index, "chunk IDs differ"));
                            }
                            if oracle.steps[index].chunk_id != Some(core_id) {
                                return Err(divergence(index, "oracle chunk ID differs"));
                            }
                            chunk_slots.insert(*slot, core_id);
                            StepOutcome::Accepted
                        }
                        (Err(core_error), Err(model_error)) => compare_rejections(
                            index,
                            &core_error,
                            &model_error,
                            RejectionContext::PutChunk,
                        )?,
                        (Ok(_), Err(error)) => {
                            return Err(divergence(
                                index,
                                &format!("model rejected chunk: {error:?}"),
                            ))
                        }
                        (Err(error), Ok(_)) => {
                            return Err(divergence(
                                index,
                                &format!("core rejected chunk: {error:?}"),
                            ))
                        }
                    }
                }
            }
            StoreOp::PutManifest { slot, chunks } => {
                let core_chunks = resolve_core_chunks(index, chunks, &chunk_slots)?;
                let model_chunks = resolve_model_chunks(index, chunks, &chunk_slots)?;
                let core_result = core.put_manifest(&core_chunks);
                if is_injected_crash(&core_result) {
                    fault_triggered = true;
                    recovery_required = true;
                    StepOutcome::InjectedCrash
                } else {
                    let model_result = model.put_manifest(&model_chunks);
                    match (core_result, model_result) {
                        (Ok(core_id), Ok(model_id)) => {
                            if core_id != model_id {
                                return Err(divergence(index, "manifest IDs differ"));
                            }
                            if oracle.steps[index].manifest_id != Some(core_id) {
                                return Err(divergence(index, "oracle manifest ID differs"));
                            }
                            manifest_slots.insert(*slot, core_id);
                            StepOutcome::Accepted
                        }
                        (Err(core_error), Err(model_error)) => compare_rejections(
                            index,
                            &core_error,
                            &model_error,
                            RejectionContext::PutManifest,
                        )?,
                        (Ok(_), Err(error)) => {
                            return Err(divergence(
                                index,
                                &format!("model rejected manifest: {error:?}"),
                            ))
                        }
                        (Err(error), Ok(_)) => {
                            return Err(divergence(
                                index,
                                &format!("core rejected manifest: {error:?}"),
                            ))
                        }
                    }
                }
            }
            StoreOp::CommitRoot {
                manifest_slot,
                generation,
            } => {
                let manifest = *manifest_slots.get(manifest_slot).ok_or_else(|| {
                    ReplayError::InvalidCase(format!(
                        "step {index} references unknown manifest slot {manifest_slot}"
                    ))
                })?;
                let core_result = core.commit_root(manifest, *generation);
                if is_injected_crash(&core_result) {
                    fault_triggered = true;
                    recovery_required = true;
                    StepOutcome::InjectedCrash
                } else {
                    let model_result = model.commit_root(manifest, *generation);
                    match (core_result, model_result) {
                        (Ok(core_root), Ok(model_root)) => {
                            compare_roots(index, &core_root, &model_root)?;
                            compare_oracle_root(index, &core_root, oracle.steps[index].root)?;
                            StepOutcome::Accepted
                        }
                        (Err(core_error), Err(model_error)) => compare_rejections(
                            index,
                            &core_error,
                            &model_error,
                            RejectionContext::CommitRoot,
                        )?,
                        (Ok(_), Err(error)) => {
                            return Err(divergence(
                                index,
                                &format!("model rejected root: {error:?}"),
                            ))
                        }
                        (Err(error), Ok(_)) => {
                            return Err(divergence(
                                index,
                                &format!("core rejected root: {error:?}"),
                            ))
                        }
                    }
                }
            }
            StoreOp::CrashReopen => {
                let device = core.into_device();
                let mut device = device;
                device.crash();
                let probe_device = device.clone();
                core = Store::open(device).map_err(|error| ReplayError::Core {
                    kind: CoreFailureKind::Reopen,
                    device: device_failure_kind(&error),
                    detail: format!("reopen failed: {error:?}"),
                })?;
                compare_visible_state(index, &mut core, &oracle.snapshot, &probe_device)?;
                recovery_required = false;
                StepOutcome::Reopened
            }
        };
        if outcome != oracle.steps[index].outcome {
            return Err(divergence(
                index,
                &format!(
                    "oracle expected {:?}, execution returned {:?}",
                    oracle.steps[index].outcome, outcome
                ),
            ));
        }
        reports.push(StepReport {
            step: index as u16,
            outcome,
        });
    }

    if case.crash.is_some() && !fault_triggered {
        return Err(ReplayError::UntriggeredCrash);
    }
    let device = core.into_device();
    let recovered = Store::open(device).map_err(|error| ReplayError::Core {
        kind: CoreFailureKind::FinalReportReopen,
        device: device_failure_kind(&error),
        detail: format!("final report reopen failed: {error:?}"),
    })?;
    let recovered_root = recovered.current_root().map(root_report);
    let device = recovered.into_device();
    Ok(ReplayReport {
        version: REPLAY_VERSION,
        steps: reports,
        recovered_root,
        resolved_fault_op,
        durable_digest: *blake3::hash(device.durable_bytes()).as_bytes(),
        pending_writes: device.pending_writes(),
        op_index: device.op_index(),
        faults_remaining: device.faults_remaining(),
        virtual_time: device.virtual_time(),
    })
}

#[derive(Clone, Copy)]
enum RejectionContext {
    PutChunk,
    PutManifest,
    CommitRoot,
}

fn compare_rejections(
    step: usize,
    core: &CoreError,
    model: &cairn_model::Error,
    context: RejectionContext,
) -> Result<StepOutcome, ReplayError> {
    let core_reason = core_rejection_reason(core, context).ok_or_else(|| {
        divergence(
            step,
            &format!("core returned an unexpected rejection: {core:?}"),
        )
    })?;
    let model_reason = model_rejection_reason(model, context);
    if core_reason != model_reason {
        return Err(divergence(
            step,
            &format!("rejection reasons differ: core={core_reason:?}, model={model_reason:?}"),
        ));
    }
    Ok(StepOutcome::Rejected {
        reason: core_reason,
    })
}

fn core_rejection_reason(error: &CoreError, context: RejectionContext) -> Option<RejectionReason> {
    match error {
        CoreError::InvalidInput(message) if *message == "device full" => {
            Some(RejectionReason::Capacity)
        }
        CoreError::InvalidInput(message)
            if matches!(context, RejectionContext::CommitRoot)
                && (*message == "root generation must be non-zero"
                    || *message == "root generation must increase") =>
        {
            Some(RejectionReason::InvalidGeneration)
        }
        CoreError::InvalidInput(_) => Some(RejectionReason::InvalidInput),
        CoreError::Corruption(_) | CoreError::NotFound(_) => {
            if matches!(context, RejectionContext::CommitRoot) {
                Some(RejectionReason::InvalidManifest)
            } else {
                Some(RejectionReason::InvalidInput)
            }
        }
        CoreError::Device(_)
        | CoreError::RequiresRecovery
        | CoreError::Unformatted
        | CoreError::UnsupportedFormat => None,
    }
}

fn model_rejection_reason(
    error: &cairn_model::Error,
    context: RejectionContext,
) -> RejectionReason {
    match error {
        cairn_model::Error::InvalidGeneration => RejectionReason::InvalidGeneration,
        cairn_model::Error::InvalidManifest(_) | cairn_model::Error::NotFound(_)
            if matches!(context, RejectionContext::CommitRoot) =>
        {
            RejectionReason::InvalidManifest
        }
        cairn_model::Error::InvalidManifest(_) => RejectionReason::InvalidInput,
        cairn_model::Error::InvalidObjectId(_) | cairn_model::Error::ConflictingObject(_) => {
            RejectionReason::InvalidInput
        }
        cairn_model::Error::NotFound(_) => RejectionReason::InvalidInput,
    }
}

fn validate_slot(slot: u8, kind: &str, step: usize) -> Result<(), ReplayError> {
    if usize::from(slot) >= MAX_SLOTS {
        return Err(ReplayError::InvalidCase(format!(
            "step {step} {kind} slot {slot} is outside 0..{MAX_SLOTS}"
        )));
    }
    Ok(())
}

fn required_capacity_for_record(current: u64, payload_len: usize) -> Result<u64, ReplayError> {
    let record_len = 32u64
        .checked_add(
            u64::try_from(payload_len)
                .map_err(|_| ReplayError::InvalidCase("record payload size overflow".into()))?,
        )
        .ok_or_else(|| ReplayError::InvalidCase("record size overflow".into()))?;
    let end = current
        .checked_add(record_len)
        .ok_or_else(|| ReplayError::InvalidCase("record offset overflow".into()))?;
    end.checked_add(7)
        .map(|value| value & !7)
        .ok_or_else(|| ReplayError::InvalidCase("record alignment overflow".into()))
}

fn resolve_fault(case: &ReplayCase) -> Result<Option<Fault>, ReplayError> {
    let Some(point) = &case.crash else {
        return Ok(None);
    };
    let mut model = Model::default();
    let mut chunk_slots = HashMap::<u8, [u8; 32]>::new();
    let mut manifest_slots = HashMap::<u8, [u8; 32]>::new();
    let mut operation_cursor = FORMAT_MUTATIONS;
    for (index, operation) in case.operations.iter().enumerate() {
        let mutations = plan_operation(
            index,
            operation,
            &mut model,
            &mut chunk_slots,
            &mut manifest_slots,
        )?;
        if index == usize::from(point.step) {
            let offset = phase_offset(operation, point.phase).ok_or_else(|| {
                ReplayError::InvalidCase(format!(
                    "phase {:?} does not apply to step {index}",
                    point.phase
                ))
            })?;
            if mutations == 0 || offset >= mutations {
                return Err(ReplayError::InvalidCase(
                    "crash point targets a step without the requested mutation phase".into(),
                ));
            }
            let operation_id = operation_cursor + offset;
            return Ok(Some(match point.timing {
                CrashTiming::Before => Fault::CrashBeforeOp(operation_id),
                CrashTiming::After => Fault::CrashAfterOp(operation_id),
            }));
        }
        operation_cursor += mutations;
    }
    Err(ReplayError::InvalidCase(
        "crash point step is out of range".into(),
    ))
}

fn plan_operation(
    step: usize,
    operation: &StoreOp,
    model: &mut Model,
    chunk_slots: &mut HashMap<u8, [u8; 32]>,
    manifest_slots: &mut HashMap<u8, [u8; 32]>,
) -> Result<u64, ReplayError> {
    match operation {
        StoreOp::PutChunk { slot, bytes } => {
            let id = cairn_model::chunk_id(bytes);
            let is_new = model.get(&id).is_none() && model.pending(&id).is_none();
            model.put_bytes(bytes.clone()).map_err(|error| {
                divergence(step, &format!("model planning chunk failed: {error:?}"))
            })?;
            chunk_slots.insert(*slot, id);
            Ok(if is_new { 3 } else { 0 })
        }
        StoreOp::PutManifest { slot, chunks } => {
            let model_chunks = resolve_model_chunks(step, chunks, chunk_slots)?;
            let id = model.put_manifest(&model_chunks).map_err(|error| {
                divergence(step, &format!("model planning manifest failed: {error:?}"))
            })?;
            manifest_slots.insert(*slot, id);
            Ok(3)
        }
        StoreOp::CommitRoot {
            manifest_slot,
            generation,
        } => {
            let manifest = *manifest_slots.get(manifest_slot).ok_or_else(|| {
                ReplayError::InvalidCase(format!(
                    "step {step} references unknown manifest slot {manifest_slot}"
                ))
            })?;
            let result = model.commit_root(manifest, *generation);
            Ok(if result.is_ok() { 5 } else { 0 })
        }
        StoreOp::CrashReopen => Ok(0),
    }
}

fn phase_offset(operation: &StoreOp, phase: MutationPhase) -> Option<u64> {
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

fn resolve_core_chunks(
    step: usize,
    chunks: &[ChunkSpec],
    slots: &HashMap<u8, [u8; 32]>,
) -> Result<Vec<CoreChunkRef>, ReplayError> {
    chunks
        .iter()
        .map(|chunk| {
            Ok(CoreChunkRef {
                id: *slots.get(&chunk.chunk_slot).ok_or_else(|| {
                    ReplayError::InvalidCase(format!(
                        "step {step} references unknown chunk slot {}",
                        chunk.chunk_slot
                    ))
                })?,
                len: chunk.len,
            })
        })
        .collect()
}

fn resolve_model_chunks(
    step: usize,
    chunks: &[ChunkSpec],
    slots: &HashMap<u8, [u8; 32]>,
) -> Result<Vec<ModelChunkRef>, ReplayError> {
    resolve_core_chunks(step, chunks, slots).map(|chunks| {
        chunks
            .into_iter()
            .map(|chunk| ModelChunkRef {
                id: chunk.id,
                len: chunk.len,
            })
            .collect()
    })
}

fn compare_roots(
    step: usize,
    core: &CoreRoot,
    model: &cairn_model::Root,
) -> Result<(), ReplayError> {
    if core.generation != model.generation || core.manifest != model.manifest {
        return Err(divergence(step, "root values differ"));
    }
    Ok(())
}

fn compare_oracle_root(
    step: usize,
    core: &CoreRoot,
    oracle: Option<oracle::OracleRoot>,
) -> Result<(), ReplayError> {
    let Some(oracle) = oracle else {
        return Err(divergence(step, "oracle did not publish an accepted root"));
    };
    if core.generation != oracle.generation || core.manifest != oracle.manifest {
        return Err(divergence(step, "oracle root values differ"));
    }
    Ok(())
}

fn compare_visible_state(
    step: usize,
    core: &mut Store<SimDisk>,
    oracle: &OracleSnapshot,
    probe_device: &SimDisk,
) -> Result<(), ReplayError> {
    let core_root = core.current_root().map(root_report);
    let oracle_root = oracle.root.map(|root| RootReport {
        generation: root.generation,
        manifest: root.manifest,
    });
    if core_root != oracle_root {
        return Err(divergence(step, "recovered roots differ"));
    }
    for id in oracle.known_chunks.keys() {
        let core_bytes = match core.get_bytes(id) {
            Ok(bytes) => Some(bytes),
            Err(CoreError::NotFound(_)) => None,
            Err(error) => {
                return Err(ReplayError::Core {
                    kind: CoreFailureKind::ReadAfterRecovery,
                    device: device_failure_kind(&error),
                    detail: format!("read after recovery failed: {error:?}"),
                })
            }
        };
        let oracle_bytes = oracle.visible_chunks.get(id).map(Vec::as_slice);
        if core_bytes.as_deref() != oracle_bytes {
            return Err(divergence(step, "recovered chunk visibility differs"));
        }
    }
    let Some(current_generation) = core.current_root().map(|root| root.generation) else {
        return Ok(());
    };
    if current_generation == u64::MAX {
        return Ok(());
    }
    let generation = current_generation + 1;
    for id in oracle.known_manifests.keys() {
        let actual = probe_manifest(probe_device, *id, generation)?;
        let expected = oracle_manifest_visibility(oracle, id);
        if actual != expected {
            return Err(divergence(step, "recovered manifest visibility differs"));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ManifestVisibility {
    Hidden,
    Valid,
    Invalid,
}

fn oracle_manifest_visibility(oracle: &OracleSnapshot, id: &[u8; 32]) -> ManifestVisibility {
    let Some(manifest) = oracle.visible_manifests.get(id) else {
        return ManifestVisibility::Hidden;
    };
    if manifest.chunks.iter().all(|chunk| {
        oracle
            .visible_chunks
            .get(&chunk.id)
            .is_some_and(|bytes| bytes.len() == chunk.len as usize)
    }) {
        ManifestVisibility::Valid
    } else {
        ManifestVisibility::Invalid
    }
}

fn probe_manifest(
    device: &SimDisk,
    manifest: [u8; 32],
    generation: u64,
) -> Result<ManifestVisibility, ReplayError> {
    let mut probe = Store::open(device.clone()).map_err(|error| ReplayError::Core {
        kind: CoreFailureKind::ManifestProbeOpen,
        device: device_failure_kind(&error),
        detail: format!("manifest probe open failed: {error:?}"),
    })?;
    match probe.commit_root(manifest, generation) {
        Ok(_) => Ok(ManifestVisibility::Valid),
        Err(CoreError::NotFound(id)) if id == manifest => Ok(ManifestVisibility::Hidden),
        Err(CoreError::NotFound(_)) | Err(CoreError::Corruption(_)) => {
            Ok(ManifestVisibility::Invalid)
        }
        Err(error) => Err(ReplayError::Core {
            kind: CoreFailureKind::ManifestProbe,
            device: device_failure_kind(&error),
            detail: format!("manifest probe failed: {error:?}"),
        }),
    }
}

fn root_report(root: cairn_core::Root) -> RootReport {
    RootReport {
        generation: root.generation,
        manifest: root.manifest,
    }
}

fn is_injected_crash<T>(result: &Result<T, CoreError>) -> bool {
    matches!(
        result,
        Err(CoreError::Device(DeviceError::Injected {
            kind: cairn_device::FaultKind::Crashed,
            ..
        }))
    )
}

fn device_failure_kind(error: &CoreError) -> Option<DeviceFailureKind> {
    match error {
        CoreError::Device(DeviceError::OutOfBounds { .. }) => Some(DeviceFailureKind::OutOfBounds),
        CoreError::Device(DeviceError::InvalidConfig(_)) => Some(DeviceFailureKind::InvalidConfig),
        CoreError::Device(DeviceError::Io { .. }) => Some(DeviceFailureKind::Io),
        CoreError::Device(DeviceError::Injected { kind, .. }) => {
            Some(DeviceFailureKind::Injected(*kind))
        }
        _ => None,
    }
}

fn divergence(step: usize, detail: &str) -> ReplayError {
    ReplayError::Divergence {
        step,
        kind: match detail {
            "chunk IDs differ" => DivergenceKind::ChunkIds,
            "oracle chunk ID differs" => DivergenceKind::OracleChunkId,
            "manifest IDs differ" => DivergenceKind::ManifestIds,
            "oracle manifest ID differs" => DivergenceKind::OracleManifestId,
            "root values differ" => DivergenceKind::RootValues,
            "oracle did not publish an accepted root" => DivergenceKind::OracleRootMissing,
            "oracle root values differ" => DivergenceKind::OracleRootValues,
            "recovered roots differ" => DivergenceKind::RecoveredRoots,
            "recovered chunk visibility differs" => DivergenceKind::RecoveredChunkVisibility,
            "recovered manifest visibility differs" => DivergenceKind::RecoveredManifestVisibility,
            detail if detail.starts_with("model rejected chunk:") => {
                DivergenceKind::ModelRejectedChunk
            }
            detail if detail.starts_with("core rejected chunk:") => {
                DivergenceKind::CoreRejectedChunk
            }
            detail if detail.starts_with("model rejected manifest:") => {
                DivergenceKind::ModelRejectedManifest
            }
            detail if detail.starts_with("core rejected manifest:") => {
                DivergenceKind::CoreRejectedManifest
            }
            detail if detail.starts_with("model rejected root:") => {
                DivergenceKind::ModelRejectedRoot
            }
            detail if detail.starts_with("core rejected root:") => DivergenceKind::CoreRejectedRoot,
            detail if detail.starts_with("oracle expected") => DivergenceKind::OracleStepOutcome,
            detail if detail.starts_with("core returned an unexpected rejection:") => {
                DivergenceKind::UnexpectedRejection
            }
            detail if detail.starts_with("rejection reasons differ:") => {
                DivergenceKind::RejectionReasons
            }
            detail if detail.starts_with("model planning chunk failed:") => {
                DivergenceKind::ModelPlanningChunk
            }
            detail if detail.starts_with("model planning manifest failed:") => {
                DivergenceKind::ModelPlanningManifest
            }
            _ => DivergenceKind::Other,
        },
        detail: detail.into(),
    }
}

fn fault_operation(fault: &Fault) -> u64 {
    match fault {
        Fault::CrashBeforeOp(operation)
        | Fault::CrashAfterOp(operation)
        | Fault::FailOp(operation)
        | Fault::TimeoutOp(operation)
        | Fault::DropWrite(operation)
        | Fault::ShortWrite { op: operation, .. }
        | Fault::TearWrite { op: operation, .. } => *operation,
        Fault::ReadFail { .. } => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_probe_distinguishes_hidden_and_missing_chunk() {
        let disk = SimDisk::new(64 * 1024, SimConfig::default());
        let mut store = Store::format(disk).unwrap();
        let missing_chunk = [0x5a; 32];
        let invalid_manifest = store
            .put_manifest(&[CoreChunkRef {
                id: missing_chunk,
                len: 1,
            }])
            .unwrap();
        let chunk = store.put_bytes(b"ok").unwrap();
        let valid_manifest = store
            .put_manifest(&[CoreChunkRef { id: chunk, len: 2 }])
            .unwrap();
        store.commit_root(valid_manifest, 1).unwrap();
        let mut device = store.into_device();
        device.crash();

        assert_eq!(
            probe_manifest(&device, invalid_manifest, 2).unwrap(),
            ManifestVisibility::Invalid
        );
        assert_eq!(
            probe_manifest(&device, [0; 32], 2).unwrap(),
            ManifestVisibility::Hidden
        );
    }
}
