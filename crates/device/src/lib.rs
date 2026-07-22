use std::cmp::Reverse;
use std::fmt;
use std::io;
use std::sync::Mutex;

#[cfg(any(unix, windows))]
mod file_device;

#[cfg(any(unix, windows))]
pub use file_device::FileDevice;

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeviceError {
    OutOfBounds {
        offset: u64,
        len: usize,
        capacity: u64,
    },
    InvalidConfig(&'static str),
    Io {
        operation: IoOperation,
        kind: io::ErrorKind,
    },
    Injected {
        at: InjectionSite,
        kind: FaultKind,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InjectionSite {
    Operation(u64),
    Event(u64),
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoOperation {
    Open,
    Metadata,
    Read,
    Write,
    SetLen,
    SyncData,
    SyncAll,
    SyncDirectory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultKind {
    Failed,
    Crashed,
    Dropped,
    MediaError,
    ReadFailed,
    ShortIo,
    Timeout,
}

impl fmt::Display for DeviceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DeviceError {}

pub trait BlockDevice: Send + Sync + 'static {
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), DeviceError>;
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DeviceError>;
    fn flush_data(&mut self) -> Result<(), DeviceError>;
    fn flush_all(&mut self) -> Result<(), DeviceError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeviceEventKind {
    Read,
    Write,
    FlushData,
    FlushAll,
    Crash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeviceEventOutcome {
    Completed,
    Dropped,
    Short { bytes: usize },
    Torn { durable_prefix: usize },
    Failed,
    Crashed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceEvent {
    pub sequence: u64,
    pub kind: DeviceEventKind,
    pub offset: u64,
    pub len: usize,
    pub virtual_time: u64,
    pub outcome: DeviceEventOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteRange {
    pub offset: u64,
    pub len: u64,
}

impl ByteRange {
    fn intersects(self, offset: u64, len: usize) -> bool {
        let end = offset.saturating_add(len as u64);
        let range_end = self.offset.saturating_add(self.len);
        offset < range_end && self.offset < end
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeviceFaultAction {
    Fail,
    Timeout,
    Drop,
    Short { bytes: usize },
    Tear { durable_prefix: usize },
    CrashBefore,
    CrashAfter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeviceFaultRule {
    pub kind: DeviceEventKind,
    pub event_sequence: u64,
    pub range: Option<ByteRange>,
    pub action: DeviceFaultAction,
}

impl DeviceFaultRule {
    fn validate(self, capacity: u64, allow_torn_writes: bool) -> Result<(), DeviceError> {
        if self.range.is_some_and(|range| range.len == 0) {
            return Err(DeviceError::InvalidConfig(
                "device fault range must be non-empty",
            ));
        }
        if self.range.is_some_and(|range| {
            range
                .offset
                .checked_add(range.len)
                .is_none_or(|end| end > capacity)
        }) {
            return Err(DeviceError::InvalidConfig(
                "device fault range exceeds device capacity",
            ));
        }
        match self.kind {
            DeviceEventKind::Read => match self.action {
                DeviceFaultAction::Fail | DeviceFaultAction::Timeout => Ok(()),
                _ => Err(DeviceError::InvalidConfig(
                    "read faults only support fail and timeout",
                )),
            },
            DeviceEventKind::Write => match self.action {
                DeviceFaultAction::Fail
                | DeviceFaultAction::Timeout
                | DeviceFaultAction::Drop
                | DeviceFaultAction::Short { .. }
                | DeviceFaultAction::CrashBefore
                | DeviceFaultAction::CrashAfter => Ok(()),
                DeviceFaultAction::Tear { .. } if allow_torn_writes => Ok(()),
                DeviceFaultAction::Tear { .. } => Err(DeviceError::InvalidConfig(
                    "torn writes require allow_torn_writes",
                )),
            },
            DeviceEventKind::FlushData | DeviceEventKind::FlushAll => {
                if self.range.is_some() {
                    return Err(DeviceError::InvalidConfig(
                        "flush fault rules cannot have a byte range",
                    ));
                }
                match self.action {
                    DeviceFaultAction::Fail
                    | DeviceFaultAction::Timeout
                    | DeviceFaultAction::Drop
                    | DeviceFaultAction::CrashBefore
                    | DeviceFaultAction::CrashAfter => Ok(()),
                    _ => Err(DeviceError::InvalidConfig(
                        "flush faults only support fail, timeout, drop, and crash",
                    )),
                }
            }
            DeviceEventKind::Crash => Err(DeviceError::InvalidConfig(
                "crash is a trace event, not a fault rule target",
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub atomic_write_size: usize,
    pub allow_reordering: bool,
    pub allow_torn_writes: bool,
    pub latency: LatencyProfile,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            atomic_write_size: 1,
            allow_reordering: false,
            allow_torn_writes: false,
            latency: LatencyProfile::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LatencyProfile {
    pub read_ticks: u64,
    pub write_ticks: u64,
    pub flush_data_ticks: u64,
    pub flush_all_ticks: u64,
    pub jitter_ticks: u64,
    pub seed: u64,
}

impl SimConfig {
    pub fn checked(self) -> Result<Self, DeviceError> {
        if self.atomic_write_size == 0 {
            return Err(DeviceError::InvalidConfig(
                "atomic_write_size must be non-zero",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub enum Fault {
    CrashBeforeOp(u64),
    CrashAfterOp(u64),
    FailOp(u64),
    TimeoutOp(u64),
    DropWrite(u64),
    TearWrite { op: u64, durable_prefix: usize },
    ReadFail { offset: u64, len: u64 },
    ShortWrite { op: u64, bytes: usize },
}

#[derive(Clone, Debug)]
struct PendingWrite {
    operation_id: u64,
    offset: u64,
    data: Vec<u8>,
    tear_prefix: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default)]
struct EventClock {
    next_sequence: u64,
    virtual_time: u64,
}

#[derive(Debug)]
pub struct SimDisk {
    durable: Vec<u8>,
    volatile: Vec<u8>,
    pending: Vec<PendingWrite>,
    pub config: SimConfig,
    op_index: u64,
    faults: Vec<Fault>,
    flush_subset: Option<Vec<u64>>,
    flush_order: Option<Vec<u64>>,
    clock: Mutex<EventClock>,
    trace: Mutex<Vec<DeviceEvent>>,
    bad_ranges: Vec<(u64, u64)>,
    device_faults: Vec<DeviceFaultRule>,
}

impl SimDisk {
    pub fn new(len: usize, config: SimConfig) -> Self {
        let config = config
            .checked()
            .expect("SimConfig.atomic_write_size must be non-zero");
        Self {
            durable: vec![0; len],
            volatile: vec![0; len],
            pending: Vec::new(),
            config,
            op_index: 0,
            faults: Vec::new(),
            flush_subset: None,
            flush_order: None,
            clock: Mutex::new(EventClock::default()),
            trace: Mutex::new(Vec::new()),
            bad_ranges: Vec::new(),
            device_faults: Vec::new(),
        }
    }
    pub fn with_fault(mut self, fault: Fault) -> Self {
        assert!(
            self.device_faults.is_empty(),
            "legacy operation faults cannot be mixed with device event faults"
        );
        self.faults.push(fault);
        self
    }
    pub fn with_faults(mut self, faults: Vec<Fault>) -> Self {
        assert!(
            self.device_faults.is_empty(),
            "legacy operation faults cannot be mixed with device event faults"
        );
        self.faults.extend(faults);
        self
    }
    pub fn faults_remaining(&self) -> usize {
        self.faults.len()
    }
    pub fn durable_bytes(&self) -> &[u8] {
        &self.durable
    }
    pub fn pending_writes(&self) -> usize {
        self.pending.len()
    }
    /// Returns the mutation/flush operation cursor used by one-shot faults.
    /// Reads are observational and deliberately do not advance this cursor.
    pub fn op_index(&self) -> u64 {
        self.op_index
    }
    pub fn virtual_time(&self) -> u64 {
        self.clock
            .lock()
            .expect("device clock mutex poisoned")
            .virtual_time
    }
    pub fn trace(&self) -> Vec<DeviceEvent> {
        self.trace
            .lock()
            .expect("device trace mutex poisoned")
            .clone()
    }
    pub fn add_bad_range(&mut self, offset: u64, len: usize) -> Result<(), DeviceError> {
        self.bounds(offset, len)?;
        self.bad_ranges.push((offset, len as u64));
        Ok(())
    }
    pub fn try_with_device_faults(
        mut self,
        faults: Vec<DeviceFaultRule>,
    ) -> Result<Self, DeviceError> {
        if !self.faults.is_empty() {
            return Err(DeviceError::InvalidConfig(
                "legacy operation faults cannot be mixed with device event faults",
            ));
        }
        for fault in faults {
            fault.validate(self.len(), self.config.allow_torn_writes)?;
            if self.device_faults.iter().any(|existing| {
                existing.kind == fault.kind
                    && existing.event_sequence == fault.event_sequence
                    && ranges_overlap(existing.range, fault.range)
            }) {
                return Err(DeviceError::InvalidConfig(
                    "device fault rules overlap at the same event",
                ));
            }
            self.device_faults.push(fault);
        }
        Ok(self)
    }
    pub fn with_device_faults(self, faults: Vec<DeviceFaultRule>) -> Self {
        self.try_with_device_faults(faults)
            .expect("invalid device event fault rule")
    }
    pub fn try_new(len: usize, config: SimConfig) -> Result<Self, DeviceError> {
        config.clone().checked()?;
        Ok(Self::new(len, config))
    }
    pub fn set_flush_order(&mut self, operation_ids: Vec<u64>) -> Result<(), DeviceError> {
        self.validate_flush_ids(&operation_ids)?;
        self.flush_order = Some(operation_ids);
        Ok(())
    }
    pub fn corrupt_durable_range(&mut self, offset: u64, len: usize) -> Result<(), DeviceError> {
        let start = self.bounds(offset, len)?;
        for byte in &mut self.durable[start..start + len] {
            *byte ^= 0xa5;
        }
        Ok(())
    }
    pub fn set_flush_subset(&mut self, operation_ids: Vec<u64>) -> Result<(), DeviceError> {
        self.validate_flush_ids(&operation_ids)?;
        self.flush_subset = Some(operation_ids);
        Ok(())
    }
    fn validate_flush_ids(&self, operation_ids: &[u64]) -> Result<(), DeviceError> {
        for (index, &operation_id) in operation_ids.iter().enumerate() {
            if !self
                .pending
                .iter()
                .any(|write| write.operation_id == operation_id)
            {
                return Err(DeviceError::InvalidConfig(
                    "flush script operation ID does not match a pending write",
                ));
            }
            if operation_ids[..index].contains(&operation_id) {
                return Err(DeviceError::InvalidConfig(
                    "flush script contains a duplicate operation ID",
                ));
            }
        }
        Ok(())
    }
    pub fn crash(&mut self) {
        let (sequence, virtual_time) = self.begin_event(DeviceEventKind::Crash);
        self.record_event(
            sequence,
            DeviceEventKind::Crash,
            0,
            0,
            virtual_time,
            DeviceEventOutcome::Completed,
        );
        self.volatile.clone_from(&self.durable);
        self.pending.clear();
        self.flush_subset = None;
        self.flush_order = None;
    }
    fn bounds(&self, offset: u64, len: usize) -> Result<usize, DeviceError> {
        let end = offset
            .checked_add(len as u64)
            .ok_or(DeviceError::OutOfBounds {
                offset,
                len,
                capacity: self.len(),
            })?;
        if end > self.len() {
            return Err(DeviceError::OutOfBounds {
                offset,
                len,
                capacity: self.len(),
            });
        }
        Ok(offset as usize)
    }
    fn before(&mut self) -> Result<u64, DeviceError> {
        let op = self.op_index;
        self.op_index = self
            .op_index
            .checked_add(1)
            .ok_or(DeviceError::InvalidConfig("operation index exhausted"))?;
        let mut crash = false;
        let mut failed = false;
        let mut timed_out = false;
        self.faults.retain(|fault| match fault {
            Fault::CrashBeforeOp(n) if *n == op => {
                crash = true;
                false
            }
            Fault::FailOp(n) if *n == op => {
                failed = true;
                false
            }
            Fault::TimeoutOp(n) if *n == op => {
                timed_out = true;
                false
            }
            _ => true,
        });
        if crash {
            self.crash();
            return Err(DeviceError::Injected {
                at: InjectionSite::Operation(op),
                kind: FaultKind::Crashed,
            });
        }
        if failed {
            return Err(DeviceError::Injected {
                at: InjectionSite::Operation(op),
                kind: FaultKind::Failed,
            });
        }
        if timed_out {
            return Err(DeviceError::Injected {
                at: InjectionSite::Operation(op),
                kind: FaultKind::Timeout,
            });
        }
        Ok(op)
    }
    fn apply_pending(&mut self) {
        if self.config.allow_reordering {
            self.pending
                .sort_by_key(|w| (w.offset, Reverse(w.operation_id)));
        }
        let writes = std::mem::take(&mut self.pending);
        let subset = self.flush_subset.take();
        let order = self
            .flush_order
            .take()
            .unwrap_or_else(|| writes.iter().map(|write| write.operation_id).collect());
        let mut writes: Vec<Option<PendingWrite>> = writes.into_iter().map(Some).collect();
        for operation_id in order {
            let Some(index) = writes.iter().position(|write| {
                write
                    .as_ref()
                    .is_some_and(|write| write.operation_id == operation_id)
            }) else {
                continue;
            };
            let Some(w) = writes[index].take() else {
                continue;
            };
            if subset
                .as_ref()
                .is_some_and(|ids| !ids.contains(&operation_id))
            {
                self.pending.push(w);
                continue;
            }
            let end = w
                .tear_prefix
                .map_or(w.data.len(), |prefix| prefix.min(w.data.len()));
            let mut cursor = 0;
            while cursor < end {
                let absolute_offset = w.offset as usize + cursor;
                let unit_remaining =
                    self.config.atomic_write_size - absolute_offset % self.config.atomic_write_size;
                let segment_len = unit_remaining.min(w.data.len() - cursor);
                if cursor + segment_len > end {
                    break;
                }
                self.durable[absolute_offset..absolute_offset + segment_len]
                    .copy_from_slice(&w.data[cursor..cursor + segment_len]);
                cursor += segment_len;
            }
        }
        self.pending.extend(writes.into_iter().flatten());
        // `volatile` already contains every issued write. Only crash resets it.
    }

    fn durable_prefix(&self, offset: u64, len: usize, requested: usize) -> usize {
        let end = requested.min(len);
        let mut cursor = 0;
        while cursor < end {
            let absolute_offset = offset as usize + cursor;
            let unit_remaining =
                self.config.atomic_write_size - absolute_offset % self.config.atomic_write_size;
            let segment_len = unit_remaining.min(len - cursor);
            if cursor + segment_len > end {
                break;
            }
            cursor += segment_len;
        }
        cursor
    }

    fn latency(&self, kind: DeviceEventKind, sequence: u64) -> u64 {
        let base = match kind {
            DeviceEventKind::Read => self.config.latency.read_ticks,
            DeviceEventKind::Write => self.config.latency.write_ticks,
            DeviceEventKind::FlushData => self.config.latency.flush_data_ticks,
            DeviceEventKind::FlushAll => self.config.latency.flush_all_ticks,
            DeviceEventKind::Crash => 0,
        };
        let jitter = if self.config.latency.jitter_ticks == 0 {
            0
        } else {
            let mut z = self.config.latency.seed ^ sequence.wrapping_mul(0x9e3779b97f4a7c15);
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            (z ^ (z >> 31)) % self.config.latency.jitter_ticks.saturating_add(1)
        };
        base.saturating_add(jitter)
    }

    fn begin_event(&self, kind: DeviceEventKind) -> (u64, u64) {
        let mut clock = self.clock.lock().expect("device clock mutex poisoned");
        let sequence = clock.next_sequence;
        clock.next_sequence = clock
            .next_sequence
            .checked_add(1)
            .expect("device event sequence exhausted");
        clock.virtual_time = clock
            .virtual_time
            .saturating_add(self.latency(kind, sequence));
        (sequence, clock.virtual_time)
    }

    fn record_event(
        &self,
        sequence: u64,
        kind: DeviceEventKind,
        offset: u64,
        len: usize,
        virtual_time: u64,
        outcome: DeviceEventOutcome,
    ) {
        let mut trace = self.trace.lock().expect("device trace mutex poisoned");
        trace.push(DeviceEvent {
            sequence,
            kind,
            offset,
            len,
            virtual_time,
            outcome,
        });
        trace.sort_by_key(|event| event.sequence);
    }

    fn overlaps_bad_range(&self, offset: u64, len: usize) -> bool {
        let end = offset.saturating_add(len as u64);
        self.bad_ranges.iter().any(|(bad_offset, bad_len)| {
            let bad_end = bad_offset.saturating_add(*bad_len);
            offset < bad_end && *bad_offset < end
        })
    }

    fn device_fault(
        &self,
        kind: DeviceEventKind,
        sequence: u64,
        offset: u64,
        len: usize,
    ) -> Option<DeviceFaultAction> {
        self.device_faults.iter().find_map(|rule| {
            (rule.kind == kind
                && rule.event_sequence == sequence
                && rule.range.is_none_or(|range| range.intersects(offset, len)))
            .then_some(rule.action)
        })
    }

    fn apply_after_fault(&mut self, op: u64) -> Result<(), DeviceError> {
        let mut crashed = false;
        self.faults.retain(|fault| match fault {
            Fault::CrashAfterOp(n) if *n == op => {
                crashed = true;
                false
            }
            _ => true,
        });
        if crashed {
            self.crash();
            return Err(DeviceError::Injected {
                at: InjectionSite::Operation(op),
                kind: FaultKind::Crashed,
            });
        }
        Ok(())
    }
}

fn ranges_overlap(left: Option<ByteRange>, right: Option<ByteRange>) -> bool {
    match (left, right) {
        (None, _) | (_, None) => true,
        (Some(left), Some(right)) => {
            left.offset < right.offset.saturating_add(right.len)
                && right.offset < left.offset.saturating_add(left.len)
        }
    }
}

impl Clone for SimDisk {
    fn clone(&self) -> Self {
        Self {
            durable: self.durable.clone(),
            volatile: self.volatile.clone(),
            pending: self.pending.clone(),
            config: self.config.clone(),
            op_index: self.op_index,
            faults: self.faults.clone(),
            flush_subset: self.flush_subset.clone(),
            flush_order: self.flush_order.clone(),
            clock: Mutex::new(*self.clock.lock().expect("device clock mutex poisoned")),
            trace: Mutex::new(
                self.trace
                    .lock()
                    .expect("device trace mutex poisoned")
                    .clone(),
            ),
            bad_ranges: self.bad_ranges.clone(),
            device_faults: self.device_faults.clone(),
        }
    }
}

impl BlockDevice for SimDisk {
    fn len(&self) -> u64 {
        self.durable.len() as u64
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), DeviceError> {
        let (sequence, virtual_time) = self.begin_event(DeviceEventKind::Read);
        let mut event_outcome = DeviceEventOutcome::Completed;
        let result = (|| {
            let start = self.bounds(offset, buf.len())?;
            match self.device_fault(DeviceEventKind::Read, sequence, offset, buf.len()) {
                None => {}
                Some(DeviceFaultAction::Fail) => {
                    event_outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::ReadFailed,
                    });
                }
                Some(DeviceFaultAction::Timeout) => {
                    event_outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Timeout,
                    });
                }
                Some(_) => {
                    event_outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::InvalidConfig("read fault action"));
                }
            }
            if self.overlaps_bad_range(offset, buf.len()) {
                event_outcome = DeviceEventOutcome::Failed;
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::MediaError,
                });
            }
            for fault in &self.faults {
                if let Fault::ReadFail {
                    offset: fault_offset,
                    len: fault_len,
                } = fault
                {
                    let request_end = offset.saturating_add(buf.len() as u64);
                    let fault_end = fault_offset.saturating_add(*fault_len);
                    if offset < fault_end && *fault_offset < request_end {
                        event_outcome = DeviceEventOutcome::Failed;
                        return Err(DeviceError::Injected {
                            at: InjectionSite::Event(sequence),
                            kind: FaultKind::ReadFailed,
                        });
                    }
                }
            }
            buf.copy_from_slice(&self.volatile[start..start + buf.len()]);
            Ok(())
        })();
        if result.is_err() {
            if matches!(
                result.as_ref(),
                Err(DeviceError::Injected {
                    kind: FaultKind::Crashed,
                    ..
                })
            ) {
                event_outcome = DeviceEventOutcome::Crashed;
            } else if matches!(event_outcome, DeviceEventOutcome::Completed) {
                event_outcome = DeviceEventOutcome::Failed;
            }
        }
        self.record_event(
            sequence,
            DeviceEventKind::Read,
            offset,
            buf.len(),
            virtual_time,
            event_outcome,
        );
        result
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DeviceError> {
        let (sequence, virtual_time) = self.begin_event(DeviceEventKind::Write);
        let mut event_outcome = DeviceEventOutcome::Completed;
        let result = (|| {
            let op = self.before()?;
            let start = self.bounds(offset, buf.len())?;
            let scripted = self.device_fault(DeviceEventKind::Write, sequence, offset, buf.len());
            match scripted {
                Some(DeviceFaultAction::CrashBefore) => {
                    event_outcome = DeviceEventOutcome::Crashed;
                    self.crash();
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Crashed,
                    });
                }
                Some(DeviceFaultAction::Fail) => {
                    event_outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Failed,
                    });
                }
                Some(DeviceFaultAction::Timeout) => {
                    event_outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Timeout,
                    });
                }
                _ => {}
            }
            if self.overlaps_bad_range(offset, buf.len()) {
                event_outcome = DeviceEventOutcome::Failed;
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::MediaError,
                });
            }
            let dropped = matches!(scripted, Some(DeviceFaultAction::Drop))
                || if let Some(index) = self
                    .faults
                    .iter()
                    .position(|fault| matches!(fault, Fault::DropWrite(n) if *n == op))
                {
                    self.faults.remove(index);
                    true
                } else {
                    false
                };
            if dropped {
                event_outcome = DeviceEventOutcome::Dropped;
                let result = self.apply_after_fault(op);
                if result.is_err() {
                    event_outcome = DeviceEventOutcome::Crashed;
                }
                return result;
            }
            let data = buf.to_vec();
            let legacy_short = self
                .faults
                .iter()
                .position(|fault| {
                    matches!(fault, Fault::ShortWrite { op: fault_op, .. } if *fault_op == op)
                })
                .map(|index| {
                    let Fault::ShortWrite { bytes, .. } = self.faults.remove(index) else {
                        unreachable!("fault position matched ShortWrite")
                    };
                    bytes
                });
            let scripted_short = matches!(scripted, Some(DeviceFaultAction::Short { .. }));
            let short_write = match scripted {
                Some(DeviceFaultAction::Short { bytes }) => Some(bytes),
                _ => legacy_short,
            }
            .map(|bytes| {
                let written = bytes.min(data.len());
                self.volatile[start..start + written].copy_from_slice(&data[..written]);
                self.pending.push(PendingWrite {
                    operation_id: op,
                    offset,
                    data: data[..written].to_vec(),
                    tear_prefix: None,
                });
                event_outcome = DeviceEventOutcome::Short { bytes: written };
                DeviceError::Injected {
                    at: if scripted_short {
                        InjectionSite::Event(sequence)
                    } else {
                        InjectionSite::Operation(op)
                    },
                    kind: FaultKind::ShortIo,
                }
            });
            if let Some(error) = short_write {
                if self.apply_after_fault(op).is_err() {
                    event_outcome = DeviceEventOutcome::Crashed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Operation(op),
                        kind: FaultKind::Crashed,
                    });
                }
                return Err(error);
            }
            let mut tear_prefix = None;
            if let Some(DeviceFaultAction::Tear { durable_prefix }) = scripted {
                if self.config.allow_torn_writes {
                    tear_prefix = Some(durable_prefix);
                }
            }
            if let Some(index) = self
                .faults
                .iter()
                .position(|fault| matches!(fault, Fault::TearWrite { op: n, .. } if *n == op))
            {
                let Fault::TearWrite { durable_prefix, .. } = self.faults.remove(index) else {
                    unreachable!("fault position matched TearWrite")
                };
                if self.config.allow_torn_writes {
                    tear_prefix = Some(durable_prefix);
                }
            }
            self.volatile[start..start + data.len()].copy_from_slice(&data);
            self.pending.push(PendingWrite {
                operation_id: op,
                offset,
                data,
                tear_prefix,
            });
            if let Some(durable_prefix) = tear_prefix {
                event_outcome = DeviceEventOutcome::Torn {
                    durable_prefix: self.durable_prefix(offset, buf.len(), durable_prefix),
                };
            }
            self.apply_after_fault(op)?;
            if matches!(scripted, Some(DeviceFaultAction::CrashAfter)) {
                event_outcome = DeviceEventOutcome::Crashed;
                self.crash();
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::Crashed,
                });
            }
            Ok(())
        })();
        if result.is_err() {
            if matches!(
                result.as_ref(),
                Err(DeviceError::Injected {
                    kind: FaultKind::Crashed,
                    ..
                })
            ) {
                event_outcome = DeviceEventOutcome::Crashed;
            } else if matches!(event_outcome, DeviceEventOutcome::Completed) {
                event_outcome = DeviceEventOutcome::Failed;
            }
        }
        self.record_event(
            sequence,
            DeviceEventKind::Write,
            offset,
            buf.len(),
            virtual_time,
            event_outcome,
        );
        result
    }
    fn flush_data(&mut self) -> Result<(), DeviceError> {
        let (sequence, virtual_time) = self.begin_event(DeviceEventKind::FlushData);
        let mut event_outcome = DeviceEventOutcome::Completed;
        let result = (|| {
            let op = self.before()?;
            match self.device_fault(DeviceEventKind::FlushData, sequence, 0, 0) {
                Some(DeviceFaultAction::CrashBefore) => {
                    event_outcome = DeviceEventOutcome::Crashed;
                    self.crash();
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Crashed,
                    });
                }
                Some(DeviceFaultAction::Fail) => {
                    event_outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Failed,
                    });
                }
                Some(DeviceFaultAction::Timeout) => {
                    event_outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Timeout,
                    });
                }
                Some(DeviceFaultAction::Drop) => {
                    event_outcome = DeviceEventOutcome::Dropped;
                    self.apply_after_fault(op)?;
                    return Ok(());
                }
                Some(DeviceFaultAction::CrashAfter) => {}
                Some(_) => return Err(DeviceError::InvalidConfig("flush fault action")),
                None => {}
            }
            self.apply_pending();
            self.apply_after_fault(op)?;
            if matches!(
                self.device_fault(DeviceEventKind::FlushData, sequence, 0, 0),
                Some(DeviceFaultAction::CrashAfter)
            ) {
                event_outcome = DeviceEventOutcome::Crashed;
                self.crash();
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::Crashed,
                });
            }
            Ok(())
        })();
        if result.is_err() {
            if matches!(
                result.as_ref(),
                Err(DeviceError::Injected {
                    kind: FaultKind::Crashed,
                    ..
                })
            ) {
                event_outcome = DeviceEventOutcome::Crashed;
            } else if matches!(event_outcome, DeviceEventOutcome::Completed) {
                event_outcome = DeviceEventOutcome::Failed;
            }
        }
        self.record_event(
            sequence,
            DeviceEventKind::FlushData,
            0,
            0,
            virtual_time,
            event_outcome,
        );
        result
    }
    fn flush_all(&mut self) -> Result<(), DeviceError> {
        let (sequence, virtual_time) = self.begin_event(DeviceEventKind::FlushAll);
        let mut event_outcome = DeviceEventOutcome::Completed;
        let result = (|| {
            let op = self.before()?;
            match self.device_fault(DeviceEventKind::FlushAll, sequence, 0, 0) {
                Some(DeviceFaultAction::CrashBefore) => {
                    event_outcome = DeviceEventOutcome::Crashed;
                    self.crash();
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Crashed,
                    });
                }
                Some(DeviceFaultAction::Fail) => {
                    event_outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Failed,
                    });
                }
                Some(DeviceFaultAction::Timeout) => {
                    event_outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Timeout,
                    });
                }
                Some(DeviceFaultAction::Drop) => {
                    event_outcome = DeviceEventOutcome::Dropped;
                    self.apply_after_fault(op)?;
                    return Ok(());
                }
                Some(DeviceFaultAction::CrashAfter) => {}
                Some(_) => return Err(DeviceError::InvalidConfig("flush fault action")),
                None => {}
            }
            self.flush_subset = None;
            self.flush_order = None;
            self.apply_pending();
            self.apply_after_fault(op)?;
            if matches!(
                self.device_fault(DeviceEventKind::FlushAll, sequence, 0, 0),
                Some(DeviceFaultAction::CrashAfter)
            ) {
                event_outcome = DeviceEventOutcome::Crashed;
                self.crash();
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::Crashed,
                });
            }
            Ok(())
        })();
        if result.is_err() {
            if matches!(
                result.as_ref(),
                Err(DeviceError::Injected {
                    kind: FaultKind::Crashed,
                    ..
                })
            ) {
                event_outcome = DeviceEventOutcome::Crashed;
            } else if matches!(event_outcome, DeviceEventOutcome::Completed) {
                event_outcome = DeviceEventOutcome::Failed;
            }
        }
        self.record_event(
            sequence,
            DeviceEventKind::FlushAll,
            0,
            0,
            virtual_time,
            event_outcome,
        );
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn data_is_not_durable_before_flush_and_crash_discards_it() {
        let mut d = SimDisk::new(16, SimConfig::default());
        d.write_at(2, b"abc").unwrap();
        let mut got = [0; 3];
        d.read_at(2, &mut got).unwrap();
        assert_eq!(&got, b"abc");
        d.crash();
        d.read_at(2, &mut got).unwrap();
        assert_eq!(&got, &[0; 3]);
    }
    #[test]
    fn flush_makes_bytes_recoverable() {
        let mut d = SimDisk::new(16, SimConfig::default());
        d.write_at(2, b"abc").unwrap();
        d.flush_data().unwrap();
        d.crash();
        let mut got = [0; 3];
        d.read_at(2, &mut got).unwrap();
        assert_eq!(&got, b"abc");
    }

    #[test]
    fn crash_boundaries_never_make_an_unflushed_write_durable() {
        let mut after_write =
            SimDisk::new(8, SimConfig::default()).with_fault(Fault::CrashAfterOp(0));
        assert!(matches!(
            after_write.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                kind: FaultKind::Crashed,
                ..
            })
        ));
        assert_eq!(&after_write.durable_bytes()[..4], b"\0\0\0\0");

        let mut before_flush =
            SimDisk::new(8, SimConfig::default()).with_fault(Fault::CrashBeforeOp(1));
        before_flush.write_at(0, b"abcd").unwrap();
        assert!(matches!(
            before_flush.flush_data(),
            Err(DeviceError::Injected {
                kind: FaultKind::Crashed,
                ..
            })
        ));
        assert_eq!(&before_flush.durable_bytes()[..4], b"\0\0\0\0");

        let mut after_flush =
            SimDisk::new(8, SimConfig::default()).with_fault(Fault::CrashAfterOp(1));
        after_flush.write_at(0, b"abcd").unwrap();
        assert!(matches!(
            after_flush.flush_data(),
            Err(DeviceError::Injected {
                kind: FaultKind::Crashed,
                ..
            })
        ));
        assert_eq!(&after_flush.durable_bytes()[..4], b"abcd");
    }
    #[test]
    fn dropped_and_torn_writes_are_deterministic() {
        let mut d = SimDisk::new(
            16,
            SimConfig {
                allow_torn_writes: true,
                ..Default::default()
            },
        )
        .with_fault(Fault::TearWrite {
            op: 0,
            durable_prefix: 2,
        });
        d.write_at(0, b"abcd").unwrap();
        d.flush_data().unwrap();
        assert_eq!(&d.durable_bytes()[..4], b"ab\0\0");
    }

    #[test]
    fn flush_subset_keeps_latest_volatile_view() {
        let mut d = SimDisk::new(8, SimConfig::default());
        d.write_at(0, b"aaaa").unwrap();
        d.write_at(0, b"bbbb").unwrap();
        d.set_flush_subset(vec![1]).unwrap();
        d.flush_data().unwrap();
        let mut got = [0; 4];
        d.read_at(0, &mut got).unwrap();
        assert_eq!(&got, b"bbbb");
        assert_eq!(&d.durable_bytes()[..4], b"bbbb");
    }

    #[test]
    fn flush_subset_uses_operation_ids_after_reordering() {
        let mut d = SimDisk::new(
            8,
            SimConfig {
                allow_reordering: true,
                ..Default::default()
            },
        );
        d.write_at(4, b"aaaa").unwrap(); // operation ID 0
        d.write_at(0, b"bbbb").unwrap(); // operation ID 1; sorted first
        d.set_flush_subset(vec![0]).unwrap();
        d.flush_data().unwrap();

        assert_eq!(&d.durable_bytes()[..4], b"\0\0\0\0");
        assert_eq!(&d.durable_bytes()[4..8], b"aaaa");
        assert_eq!(d.pending_writes(), 1);
    }

    #[test]
    fn flush_order_uses_operation_ids_after_reordering() {
        let mut d = SimDisk::new(
            4,
            SimConfig {
                allow_reordering: true,
                ..Default::default()
            },
        );
        d.write_at(0, b"aaaa").unwrap();
        d.write_at(0, b"bbbb").unwrap();
        d.set_flush_order(vec![0, 1]).unwrap();
        d.flush_data().unwrap();

        assert_eq!(d.durable_bytes(), b"bbbb");
    }

    #[test]
    fn flush_subset_uses_stable_operation_ids_when_writes_reorder() {
        let mut d = SimDisk::new(
            8,
            SimConfig {
                allow_reordering: true,
                ..Default::default()
            },
        );
        d.write_at(4, b"bbbb").unwrap(); // operation 0
        d.write_at(0, b"aaaa").unwrap(); // operation 1; sorts before operation 0
        d.set_flush_subset(vec![0]).unwrap();
        d.flush_data().unwrap();

        assert_eq!(&d.durable_bytes()[..8], b"\0\0\0\0bbbb");
    }

    #[test]
    fn flush_order_uses_stable_operation_ids_when_writes_reorder() {
        let mut d = SimDisk::new(
            4,
            SimConfig {
                allow_reordering: true,
                ..Default::default()
            },
        );
        d.write_at(0, b"aaaa").unwrap(); // operation 0
        d.write_at(0, b"bbbb").unwrap(); // operation 1; sorts before operation 0
        d.set_flush_order(vec![0, 1]).unwrap();
        d.flush_data().unwrap();

        assert_eq!(d.durable_bytes(), b"bbbb");
    }

    #[test]
    fn atomic_size_limits_torn_persistence_to_units() {
        let mut d = SimDisk::new(
            8,
            SimConfig {
                atomic_write_size: 4,
                allow_torn_writes: true,
                ..Default::default()
            },
        )
        .with_fault(Fault::TearWrite {
            op: 0,
            durable_prefix: 2,
        });
        d.write_at(0, b"abcd").unwrap();
        d.flush_data().unwrap();
        assert_eq!(&d.durable_bytes()[..4], b"\0\0\0\0");
    }

    #[test]
    fn atomic_size_uses_absolute_units_for_unaligned_writes() {
        let mut d = SimDisk::new(
            8,
            SimConfig {
                atomic_write_size: 4,
                allow_torn_writes: true,
                ..Default::default()
            },
        )
        .with_fault(Fault::TearWrite {
            op: 0,
            durable_prefix: 2,
        });
        d.write_at(2, b"abcd").unwrap();
        d.flush_data().unwrap();

        assert_eq!(d.durable_bytes(), b"\0\0ab\0\0\0\0");
    }

    #[test]
    fn torn_write_property_only_persists_complete_atomic_units() {
        for requested_prefix in 0..=8 {
            let mut d = SimDisk::new(
                8,
                SimConfig {
                    atomic_write_size: 2,
                    allow_torn_writes: true,
                    ..Default::default()
                },
            )
            .with_fault(Fault::TearWrite {
                op: 0,
                durable_prefix: requested_prefix,
            });
            d.write_at(0, b"abcdefgh").unwrap();
            d.flush_data().unwrap();
            let expected_len = if requested_prefix == 8 {
                8
            } else {
                requested_prefix / 2 * 2
            };
            assert_eq!(
                &d.durable_bytes()[..expected_len],
                &b"abcdefgh"[..expected_len]
            );
            assert!(d.durable_bytes()[expected_len..]
                .iter()
                .all(|byte| *byte == 0));
        }
    }

    #[test]
    fn virtual_latency_is_deterministic_and_has_no_wall_clock_wait() {
        let mut d = SimDisk::new(
            16,
            SimConfig {
                latency: LatencyProfile {
                    read_ticks: 2,
                    write_ticks: 5,
                    flush_data_ticks: 7,
                    flush_all_ticks: 11,
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        assert_eq!(d.virtual_time(), 0);
        d.write_at(0, b"x").unwrap();
        assert_eq!(d.virtual_time(), 5);
        d.flush_data().unwrap();
        assert_eq!(d.virtual_time(), 12);
        let mut byte = [0; 1];
        d.read_at(0, &mut byte).unwrap();
        assert_eq!(d.virtual_time(), 14);
        d.flush_all().unwrap();
        assert_eq!(d.virtual_time(), 25);
    }

    #[test]
    fn device_trace_and_seeded_jitter_are_deterministic() {
        let config = SimConfig {
            latency: LatencyProfile {
                read_ticks: 2,
                write_ticks: 5,
                flush_data_ticks: 7,
                flush_all_ticks: 11,
                jitter_ticks: 3,
                seed: 41,
            },
            ..Default::default()
        };
        let mut first = SimDisk::new(16, config.clone());
        let mut second = SimDisk::new(16, config);
        for disk in [&mut first, &mut second] {
            disk.write_at(2, b"abc").unwrap();
            disk.flush_data().unwrap();
            let mut byte = [0; 1];
            disk.read_at(2, &mut byte).unwrap();
            disk.crash();
        }
        assert_eq!(first.virtual_time(), second.virtual_time());
        assert_eq!(first.trace(), second.trace());
        assert_eq!(first.trace()[0].kind, DeviceEventKind::Write);
        assert_eq!(first.trace()[0].offset, 2);
        assert_eq!(first.trace()[0].len, 3);
    }

    #[test]
    fn bad_ranges_are_persistent_physical_media_errors() {
        let mut disk = SimDisk::new(16, SimConfig::default());
        disk.add_bad_range(2, 2).unwrap();
        assert!(matches!(
            disk.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                kind: FaultKind::MediaError,
                ..
            })
        ));
        assert_eq!(disk.durable_bytes(), &[0; 16]);
        assert!(matches!(
            disk.read_at(2, &mut [0; 1]),
            Err(DeviceError::Injected {
                kind: FaultKind::MediaError,
                ..
            })
        ));
        disk.crash();
        assert!(matches!(
            disk.read_at(3, &mut [0; 1]),
            Err(DeviceError::Injected {
                kind: FaultKind::MediaError,
                ..
            })
        ));
    }

    #[test]
    fn device_fault_rules_match_events_and_physical_ranges() {
        let mut disk =
            SimDisk::new(16, SimConfig::default()).with_device_faults(vec![DeviceFaultRule {
                kind: DeviceEventKind::Write,
                event_sequence: 0,
                range: Some(ByteRange { offset: 2, len: 2 }),
                action: DeviceFaultAction::Short { bytes: 2 },
            }]);
        assert!(matches!(
            disk.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                at: InjectionSite::Event(0),
                kind: FaultKind::ShortIo,
                ..
            })
        ));
        disk.flush_data().unwrap();
        assert_eq!(&disk.durable_bytes()[..4], &[b'a', b'b', 0, 0]);
        assert_eq!(disk.trace()[0].sequence, 0);
        assert_eq!(disk.trace()[0].kind, DeviceEventKind::Write);
        assert_eq!(
            disk.trace()[0].outcome,
            DeviceEventOutcome::Short { bytes: 2 }
        );
    }

    #[test]
    fn device_fault_rules_cover_persistence_and_crash_boundaries() {
        let mut dropped =
            SimDisk::new(8, SimConfig::default()).with_device_faults(vec![DeviceFaultRule {
                kind: DeviceEventKind::Write,
                event_sequence: 0,
                range: None,
                action: DeviceFaultAction::Drop,
            }]);
        dropped.write_at(0, b"x").unwrap();
        dropped.flush_data().unwrap();
        assert_eq!(dropped.durable_bytes()[0], 0);
        assert_eq!(dropped.trace()[0].outcome, DeviceEventOutcome::Dropped);

        let failed_read =
            SimDisk::new(8, SimConfig::default()).with_device_faults(vec![DeviceFaultRule {
                kind: DeviceEventKind::Read,
                event_sequence: 0,
                range: Some(ByteRange { offset: 1, len: 1 }),
                action: DeviceFaultAction::Fail,
            }]);
        assert!(matches!(
            failed_read.read_at(1, &mut [0]),
            Err(DeviceError::Injected {
                kind: FaultKind::ReadFailed,
                ..
            })
        ));

        let mut crashed =
            SimDisk::new(8, SimConfig::default()).with_device_faults(vec![DeviceFaultRule {
                kind: DeviceEventKind::FlushData,
                event_sequence: 1,
                range: None,
                action: DeviceFaultAction::CrashAfter,
            }]);
        crashed.write_at(0, b"x").unwrap();
        assert!(matches!(
            crashed.flush_data(),
            Err(DeviceError::Injected {
                kind: FaultKind::Crashed,
                ..
            })
        ));
        assert_eq!(crashed.durable_bytes()[0], b'x');
        assert_eq!(crashed.trace()[1].outcome, DeviceEventOutcome::Crashed);
    }

    #[test]
    fn device_fault_rules_are_validated_at_installation() {
        let invalid_read =
            SimDisk::new(8, SimConfig::default()).try_with_device_faults(vec![DeviceFaultRule {
                kind: DeviceEventKind::Read,
                event_sequence: 0,
                range: None,
                action: DeviceFaultAction::Drop,
            }]);
        assert!(matches!(invalid_read, Err(DeviceError::InvalidConfig(_))));

        let invalid_flush =
            SimDisk::new(8, SimConfig::default()).try_with_device_faults(vec![DeviceFaultRule {
                kind: DeviceEventKind::FlushData,
                event_sequence: 0,
                range: Some(ByteRange { offset: 0, len: 1 }),
                action: DeviceFaultAction::Fail,
            }]);
        assert!(matches!(invalid_flush, Err(DeviceError::InvalidConfig(_))));

        let invalid_tear =
            SimDisk::new(8, SimConfig::default()).try_with_device_faults(vec![DeviceFaultRule {
                kind: DeviceEventKind::Write,
                event_sequence: 0,
                range: None,
                action: DeviceFaultAction::Tear { durable_prefix: 1 },
            }]);
        assert!(matches!(invalid_tear, Err(DeviceError::InvalidConfig(_))));

        let invalid_range =
            SimDisk::new(8, SimConfig::default()).try_with_device_faults(vec![DeviceFaultRule {
                kind: DeviceEventKind::Write,
                event_sequence: 0,
                range: Some(ByteRange { offset: 8, len: 1 }),
                action: DeviceFaultAction::Fail,
            }]);
        assert!(matches!(invalid_range, Err(DeviceError::InvalidConfig(_))));

        let overlapping = SimDisk::new(8, SimConfig::default()).try_with_device_faults(vec![
            DeviceFaultRule {
                kind: DeviceEventKind::Write,
                event_sequence: 0,
                range: Some(ByteRange { offset: 0, len: 2 }),
                action: DeviceFaultAction::Fail,
            },
            DeviceFaultRule {
                kind: DeviceEventKind::Write,
                event_sequence: 0,
                range: Some(ByteRange { offset: 1, len: 2 }),
                action: DeviceFaultAction::Timeout,
            },
        ]);
        assert!(matches!(overlapping, Err(DeviceError::InvalidConfig(_))));

        let mixed = SimDisk::new(8, SimConfig::default())
            .with_fault(Fault::FailOp(0))
            .try_with_device_faults(Vec::new());
        assert!(matches!(mixed, Err(DeviceError::InvalidConfig(_))));
    }

    #[test]
    fn torn_trace_reports_the_atomic_prefix_that_can_reach_media() {
        let mut disk = SimDisk::new(
            16,
            SimConfig {
                atomic_write_size: 4,
                allow_torn_writes: true,
                ..Default::default()
            },
        )
        .with_device_faults(vec![DeviceFaultRule {
            kind: DeviceEventKind::Write,
            event_sequence: 0,
            range: None,
            action: DeviceFaultAction::Tear { durable_prefix: 2 },
        }]);
        disk.write_at(1, b"abcd").unwrap();
        disk.flush_data().unwrap();
        assert_eq!(
            disk.trace()[0].outcome,
            DeviceEventOutcome::Torn { durable_prefix: 0 }
        );
        assert_eq!(&disk.durable_bytes()[1..5], &[0; 4]);
    }

    #[test]
    fn invalid_flush_scripts_and_timeout_faults_are_explicit() {
        let mut d = SimDisk::try_new(8, SimConfig::default())
            .unwrap()
            .with_fault(Fault::TimeoutOp(0));
        assert!(matches!(
            d.write_at(0, b"x"),
            Err(DeviceError::Injected {
                kind: FaultKind::Timeout,
                ..
            })
        ));

        let mut d = SimDisk::new(8, SimConfig::default());
        d.write_at(0, b"x").unwrap();
        assert!(matches!(
            d.set_flush_order(vec![1]),
            Err(DeviceError::InvalidConfig(_))
        ));
        assert!(matches!(
            d.set_flush_subset(vec![0, 0]),
            Err(DeviceError::InvalidConfig(_))
        ));
    }

    #[test]
    fn chaos_experiments_cover_read_failure_corruption_and_short_write() {
        let mut d = SimDisk::try_new(16, SimConfig::default()).unwrap();
        d.write_at(0, b"abcd").unwrap();
        d.flush_data().unwrap();
        d.corrupt_durable_range(1, 1).unwrap();
        d.crash();
        let mut got = [0; 4];
        d.read_at(0, &mut got).unwrap();
        assert_eq!(&got, b"a\xc7cd");

        let failed = SimDisk::try_new(16, SimConfig::default())
            .unwrap()
            .with_fault(Fault::ReadFail { offset: 2, len: 2 });
        assert!(matches!(
            failed.read_at(2, &mut [0; 2]),
            Err(DeviceError::Injected {
                kind: FaultKind::ReadFailed,
                ..
            })
        ));

        let mut short = SimDisk::try_new(16, SimConfig::default())
            .unwrap()
            .with_fault(Fault::ShortWrite { op: 0, bytes: 2 });
        assert!(matches!(
            short.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                at: InjectionSite::Operation(0),
                kind: FaultKind::ShortIo,
                ..
            })
        ));
        short.flush_data().unwrap();
        assert_eq!(&short.durable_bytes()[..4], b"ab\0\0");
    }

    #[test]
    fn fault_schedule_consumes_multiple_one_shot_failures_in_operation_order() {
        let mut d = SimDisk::try_new(
            8,
            SimConfig {
                allow_torn_writes: true,
                ..Default::default()
            },
        )
        .unwrap()
        .with_faults(vec![
            Fault::FailOp(0),
            Fault::ShortWrite { op: 1, bytes: 2 },
            Fault::TearWrite {
                op: 2,
                durable_prefix: 2,
            },
        ]);

        assert!(matches!(
            d.write_at(0, b"abcd"),
            Err(DeviceError::Injected { .. })
        ));
        assert!(matches!(
            d.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                kind: FaultKind::ShortIo,
                ..
            })
        ));
        d.write_at(4, b"wxyz").unwrap();
        d.flush_data().unwrap();

        assert_eq!(d.faults_remaining(), 0);
        assert_eq!(&d.durable_bytes()[..8], b"ab\0\0wx\0\0");
    }

    #[test]
    fn after_faults_run_even_when_a_write_is_dropped_or_short() {
        let mut dropped = SimDisk::new(8, SimConfig::default())
            .with_faults(vec![Fault::DropWrite(0), Fault::CrashAfterOp(0)]);
        assert!(matches!(
            dropped.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                kind: FaultKind::Crashed,
                ..
            })
        ));
        assert_eq!(dropped.faults_remaining(), 0);

        let mut short = SimDisk::new(8, SimConfig::default()).with_faults(vec![
            Fault::ShortWrite { op: 0, bytes: 2 },
            Fault::CrashAfterOp(0),
        ]);
        assert!(matches!(
            short.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                kind: FaultKind::Crashed,
                ..
            })
        ));
        assert_eq!(short.faults_remaining(), 0);
    }

    #[test]
    fn flush_all_ignores_a_partial_flush_script() {
        let mut d = SimDisk::new(8, SimConfig::default());
        d.write_at(0, b"aaaa").unwrap();
        d.write_at(4, b"bbbb").unwrap();
        d.set_flush_subset(vec![0]).unwrap();
        d.flush_data().unwrap();
        assert_eq!(&d.durable_bytes()[..8], b"aaaa\0\0\0\0");
        d.flush_all().unwrap();
        assert_eq!(&d.durable_bytes()[..8], b"aaaabbbb");
    }
}
