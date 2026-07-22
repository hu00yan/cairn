use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceEventKind {
    Read,
    Write,
    FlushData,
    FlushAll,
    Crash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceEventOutcome {
    Completed,
    Dropped,
    Short { bytes: usize },
    Torn { durable_prefix: usize },
    Failed,
    Crashed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeviceEvent {
    pub sequence: u64,
    pub kind: DeviceEventKind,
    pub offset: u64,
    pub len: usize,
    pub virtual_time: u64,
    pub outcome: DeviceEventOutcome,
    pub script_rule: Option<u32>,
    pub script_effect: Option<DeviceEffect>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum EventOccurrence {
    Any,
    Exact(u64),
    Every { first: u64, period: u64 },
}

impl Default for EventOccurrence {
    fn default() -> Self {
        Self::Any
    }
}

impl EventOccurrence {
    pub fn matches(self, sequence: u64) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(expected) => expected == sequence,
            Self::Every { first, period } => {
                period != 0 && sequence >= first && (sequence - first) % period == 0
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventSelector {
    pub kind: DeviceEventKind,
    #[serde(default)]
    pub occurrence: EventOccurrence,
    #[serde(default)]
    pub range: Option<ByteRange>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum DeviceEffect {
    Fail,
    Timeout,
    Drop,
    Short { bytes: usize },
    Tear { durable_prefix: usize },
    TearAndCrashAfter { durable_prefix: usize },
    CrashBefore,
    CrashAfter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceRule {
    pub selector: EventSelector,
    pub effect: DeviceEffect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyRule {
    pub selector: EventSelector,
    pub extra_ticks: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum ReorderPolicy {
    ByOffset,
    Reverse,
    Seeded { window: u32, seed: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceScript {
    #[serde(default = "default_atomic_unit")]
    pub atomic_unit: usize,
    #[serde(default)]
    pub reorder: Option<ReorderPolicy>,
    #[serde(default)]
    pub flush_data_writes: Option<u32>,
    #[serde(default)]
    pub latency: LatencyProfile,
    #[serde(default)]
    pub bad_ranges: Vec<ByteRange>,
    #[serde(default)]
    pub rules: Vec<DeviceRule>,
    #[serde(default)]
    pub latency_rules: Vec<LatencyRule>,
}

impl Default for DeviceScript {
    fn default() -> Self {
        Self {
            atomic_unit: default_atomic_unit(),
            reorder: None,
            flush_data_writes: None,
            latency: LatencyProfile::default(),
            bad_ranges: Vec::new(),
            rules: Vec::new(),
            latency_rules: Vec::new(),
        }
    }
}

fn default_atomic_unit() -> usize {
    1
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyProfile {
    pub read_ticks: u64,
    pub write_ticks: u64,
    pub flush_data_ticks: u64,
    pub flush_all_ticks: u64,
    pub jitter_ticks: u64,
    pub seed: u64,
}

impl DeviceScript {
    fn validate(&self, capacity: u64) -> Result<(), DeviceError> {
        if self.atomic_unit == 0 {
            return Err(DeviceError::InvalidConfig("atomic_unit must be non-zero"));
        }
        for range in &self.bad_ranges {
            validate_range(*range, capacity, "bad range exceeds device capacity")?;
        }
        for rule in &self.rules {
            validate_selector(rule.selector, capacity)?;
            validate_effect(rule.selector.kind, rule.effect)?;
        }
        for rule in &self.latency_rules {
            validate_selector(rule.selector, capacity)?;
            if rule.selector.range.is_some() {
                return Err(DeviceError::InvalidConfig(
                    "latency selectors are operation-wide and cannot have a byte range",
                ));
            }
        }
        validate_no_overlapping_rules(&self.rules)
    }
}

fn validate_range(
    range: ByteRange,
    capacity: u64,
    message: &'static str,
) -> Result<(), DeviceError> {
    if range.len == 0 {
        return Err(DeviceError::InvalidConfig(
            "device ranges must be non-empty",
        ));
    }
    if range
        .offset
        .checked_add(range.len)
        .is_none_or(|end| end > capacity)
    {
        return Err(DeviceError::InvalidConfig(message));
    }
    Ok(())
}

fn validate_selector(selector: EventSelector, capacity: u64) -> Result<(), DeviceError> {
    if let Some(range) = selector.range {
        if matches!(
            selector.kind,
            DeviceEventKind::FlushData | DeviceEventKind::FlushAll
        ) {
            return Err(DeviceError::InvalidConfig(
                "flush selectors cannot have a byte range",
            ));
        }
        validate_range(range, capacity, "device selector range exceeds capacity")?;
    }
    if matches!(
        selector.occurrence,
        EventOccurrence::Every { period: 0, .. }
    ) {
        return Err(DeviceError::InvalidConfig(
            "event occurrence period must be non-zero",
        ));
    }
    if selector.kind == DeviceEventKind::Crash {
        return Err(DeviceError::InvalidConfig(
            "crash is a trace event, not a script target",
        ));
    }
    Ok(())
}

fn validate_effect(kind: DeviceEventKind, effect: DeviceEffect) -> Result<(), DeviceError> {
    let valid = match kind {
        DeviceEventKind::Read => matches!(effect, DeviceEffect::Fail | DeviceEffect::Timeout),
        DeviceEventKind::Write => matches!(
            effect,
            DeviceEffect::Fail
                | DeviceEffect::Timeout
                | DeviceEffect::Drop
                | DeviceEffect::Short { .. }
                | DeviceEffect::Tear { .. }
                | DeviceEffect::TearAndCrashAfter { .. }
                | DeviceEffect::CrashBefore
                | DeviceEffect::CrashAfter
        ),
        DeviceEventKind::FlushData | DeviceEventKind::FlushAll => matches!(
            effect,
            DeviceEffect::Fail
                | DeviceEffect::Timeout
                | DeviceEffect::Drop
                | DeviceEffect::CrashBefore
                | DeviceEffect::CrashAfter
        ),
        DeviceEventKind::Crash => false,
    };
    valid.then_some(()).ok_or(DeviceError::InvalidConfig(
        "device effect is invalid for event kind",
    ))
}

fn validate_no_overlapping_rules(rules: &[DeviceRule]) -> Result<(), DeviceError> {
    for (index, left) in rules.iter().enumerate() {
        for right in &rules[..index] {
            if left.selector.kind == right.selector.kind
                && occurrences_overlap(left.selector.occurrence, right.selector.occurrence)
            {
                return Err(DeviceError::InvalidConfig(
                    "device rules overlap at the same event",
                ));
            }
        }
    }
    Ok(())
}

fn occurrences_overlap(left: EventOccurrence, right: EventOccurrence) -> bool {
    match (left, right) {
        (EventOccurrence::Any, _) | (_, EventOccurrence::Any) => true,
        (EventOccurrence::Exact(left), EventOccurrence::Exact(right)) => left == right,
        (EventOccurrence::Exact(exact), EventOccurrence::Every { first, period })
        | (EventOccurrence::Every { first, period }, EventOccurrence::Exact(exact)) => {
            period != 0 && exact >= first && (exact - first) % period == 0
        }
        (
            EventOccurrence::Every {
                first: left_first,
                period: left_period,
            },
            EventOccurrence::Every {
                first: right_first,
                period: right_period,
            },
        ) => {
            left_period != 0
                && right_period != 0
                && left_first.abs_diff(right_first) % gcd(left_period, right_period) == 0
        }
    }
}

fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        (left, right) = (right, left % right);
    }
    left
}

#[derive(Clone, Debug)]
struct PendingWrite {
    ordinal: u64,
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
    script: DeviceScript,
    clock: Mutex<EventClock>,
    trace: Mutex<Vec<DeviceEvent>>,
    rule_hits: Arc<Mutex<Vec<bool>>>,
}

impl SimDisk {
    pub fn new(len: usize) -> Self {
        Self::from_script(len, DeviceScript::default()).expect("default device script is valid")
    }

    pub fn from_script(len: usize, script: DeviceScript) -> Result<Self, DeviceError> {
        script.validate(len as u64)?;
        let rule_count = script.rules.len();
        Ok(Self {
            durable: vec![0; len],
            volatile: vec![0; len],
            pending: Vec::new(),
            script,
            clock: Mutex::new(EventClock::default()),
            trace: Mutex::new(Vec::new()),
            rule_hits: Arc::new(Mutex::new(vec![false; rule_count])),
        })
    }

    pub fn script(&self) -> &DeviceScript {
        &self.script
    }

    pub fn durable_bytes(&self) -> &[u8] {
        &self.durable
    }

    pub fn pending_writes(&self) -> usize {
        self.pending.len()
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

    pub fn script_rule_hits(&self) -> Arc<Mutex<Vec<bool>>> {
        Arc::clone(&self.rule_hits)
    }

    pub fn add_bad_range(&mut self, offset: u64, len: usize) -> Result<(), DeviceError> {
        self.bounds(offset, len)?;
        self.script.bad_ranges.push(ByteRange {
            offset,
            len: len as u64,
        });
        Ok(())
    }

    pub fn corrupt_durable_range(&mut self, offset: u64, len: usize) -> Result<(), DeviceError> {
        let start = self.bounds(offset, len)?;
        for byte in &mut self.durable[start..start + len] {
            *byte ^= 0xa5;
        }
        Ok(())
    }

    pub fn power_loss(&mut self) {
        let (sequence, virtual_time) = self.begin_event(DeviceEventKind::Crash);
        self.discard_volatile();
        self.record_event(DeviceEvent {
            sequence,
            kind: DeviceEventKind::Crash,
            offset: 0,
            len: 0,
            virtual_time,
            outcome: DeviceEventOutcome::Completed,
            script_rule: None,
            script_effect: None,
        });
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

    fn latency(&self, kind: DeviceEventKind, sequence: u64) -> u64 {
        let base = match kind {
            DeviceEventKind::Read => self.script.latency.read_ticks,
            DeviceEventKind::Write => self.script.latency.write_ticks,
            DeviceEventKind::FlushData => self.script.latency.flush_data_ticks,
            DeviceEventKind::FlushAll => self.script.latency.flush_all_ticks,
            DeviceEventKind::Crash => 0,
        };
        let jitter = if self.script.latency.jitter_ticks == 0 {
            0
        } else {
            mix(self.script.latency.seed, sequence)
                % self.script.latency.jitter_ticks.saturating_add(1)
        };
        let extra = self
            .script
            .latency_rules
            .iter()
            .filter(|rule| {
                rule.selector.kind == kind
                    && rule.selector.occurrence.matches(sequence)
                    && rule.selector.range.is_none()
            })
            .map(|rule| rule.extra_ticks)
            .fold(0, u64::saturating_add);
        base.saturating_add(jitter).saturating_add(extra)
    }

    fn record_event(&self, event: DeviceEvent) {
        self.trace
            .lock()
            .expect("device trace mutex poisoned")
            .push(event);
        self.trace
            .lock()
            .expect("device trace mutex poisoned")
            .sort_by_key(|event| event.sequence);
    }

    fn scripted_effect(
        &self,
        kind: DeviceEventKind,
        sequence: u64,
        offset: u64,
        len: usize,
    ) -> Option<(u32, DeviceEffect)> {
        self.script
            .rules
            .iter()
            .enumerate()
            .find_map(|(index, rule)| {
                (rule.selector.kind == kind
                    && rule.selector.occurrence.matches(sequence)
                    && rule
                        .selector
                        .range
                        .is_none_or(|range| range.intersects(offset, len)))
                .then_some((index as u32, rule.effect))
                .inspect(|(index, _)| {
                    self.rule_hits
                        .lock()
                        .expect("device rule-hit mutex poisoned")[*index as usize] = true;
                })
            })
    }

    fn overlaps_bad_range(&self, offset: u64, len: usize) -> bool {
        self.script
            .bad_ranges
            .iter()
            .any(|range| range.intersects(offset, len))
    }

    fn durable_prefix(&self, offset: u64, len: usize, requested: usize) -> usize {
        let end = requested.min(len);
        let mut cursor = 0;
        while cursor < end {
            let absolute = offset as usize + cursor;
            let remaining = self.script.atomic_unit - absolute % self.script.atomic_unit;
            let segment = remaining.min(len - cursor);
            if cursor + segment > end {
                break;
            }
            cursor += segment;
        }
        cursor
    }

    fn persist_write(&mut self, write: PendingWrite) {
        let end = write
            .tear_prefix
            .map_or(write.data.len(), |prefix| prefix.min(write.data.len()));
        let mut cursor = 0;
        while cursor < end {
            let absolute = write.offset as usize + cursor;
            let remaining = self.script.atomic_unit - absolute % self.script.atomic_unit;
            let segment_len = remaining.min(write.data.len() - cursor);
            if cursor + segment_len > end {
                break;
            }
            self.durable[absolute..absolute + segment_len]
                .copy_from_slice(&write.data[cursor..cursor + segment_len]);
            cursor += segment_len;
        }
    }

    fn apply_one_pending(&mut self, ordinal: u64) {
        if let Some(index) = self
            .pending
            .iter()
            .position(|write| write.ordinal == ordinal)
        {
            let write = self.pending.remove(index);
            self.persist_write(write);
        }
    }

    fn apply_pending(&mut self, limit: Option<usize>) {
        let mut writes = std::mem::take(&mut self.pending);
        match self.script.reorder {
            None => {}
            Some(ReorderPolicy::ByOffset) => {
                writes.sort_by_key(|write| (write.offset, write.ordinal));
            }
            Some(ReorderPolicy::Reverse) => writes.reverse(),
            Some(ReorderPolicy::Seeded { window, seed }) => {
                let window = usize::try_from(window.max(1)).unwrap_or(usize::MAX);
                for chunk in writes.chunks_mut(window) {
                    chunk.sort_by_key(|write| mix(seed, write.ordinal));
                }
            }
        }
        let limit = limit.unwrap_or(writes.len());
        let remaining = writes.split_off(limit.min(writes.len()));
        for write in writes {
            self.persist_write(write);
        }
        self.pending = remaining;
    }

    fn discard_volatile(&mut self) {
        self.volatile.clone_from(&self.durable);
        self.pending.clear();
    }

    fn flush(&mut self, kind: DeviceEventKind) -> Result<(), DeviceError> {
        let (sequence, virtual_time) = self.begin_event(kind);
        let mut outcome = DeviceEventOutcome::Completed;
        let scripted = self.scripted_effect(kind, sequence, 0, 0);
        let effect = scripted.map(|(_, effect)| effect);
        let result = (|| {
            if matches!(effect, Some(DeviceEffect::CrashBefore)) {
                outcome = DeviceEventOutcome::Crashed;
                self.discard_volatile();
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::Crashed,
                });
            }
            match effect {
                Some(DeviceEffect::Fail) => {
                    outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Failed,
                    });
                }
                Some(DeviceEffect::Timeout) => {
                    outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Timeout,
                    });
                }
                Some(DeviceEffect::Drop) => {
                    outcome = DeviceEventOutcome::Dropped;
                    return Ok(());
                }
                _ => {}
            }
            let limit = (kind == DeviceEventKind::FlushData)
                .then_some(self.script.flush_data_writes)
                .flatten()
                .map(|writes| writes as usize);
            self.apply_pending(limit);
            if matches!(effect, Some(DeviceEffect::CrashAfter)) {
                outcome = DeviceEventOutcome::Crashed;
                self.discard_volatile();
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::Crashed,
                });
            }
            Ok(())
        })();
        if result.is_err() && matches!(outcome, DeviceEventOutcome::Completed) {
            outcome = DeviceEventOutcome::Failed;
        }
        self.record_event(DeviceEvent {
            sequence,
            kind,
            offset: 0,
            len: 0,
            virtual_time,
            outcome,
            script_rule: scripted.map(|(rule, _)| rule),
            script_effect: scripted.map(|(_, effect)| effect),
        });
        result
    }
}

fn mix(mut value: u64, ordinal: u64) -> u64 {
    value ^= ordinal.wrapping_mul(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

impl Clone for SimDisk {
    fn clone(&self) -> Self {
        let rule_hits = self
            .rule_hits
            .lock()
            .expect("device rule-hit mutex poisoned")
            .clone();
        Self {
            durable: self.durable.clone(),
            volatile: self.volatile.clone(),
            pending: self.pending.clone(),
            script: self.script.clone(),
            clock: Mutex::new(*self.clock.lock().expect("device clock mutex poisoned")),
            trace: Mutex::new(
                self.trace
                    .lock()
                    .expect("device trace mutex poisoned")
                    .clone(),
            ),
            rule_hits: Arc::new(Mutex::new(rule_hits)),
        }
    }
}

impl BlockDevice for SimDisk {
    fn len(&self) -> u64 {
        self.durable.len() as u64
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), DeviceError> {
        let (sequence, virtual_time) = self.begin_event(DeviceEventKind::Read);
        let mut outcome = DeviceEventOutcome::Completed;
        let mut scripted = None;
        let result = (|| {
            let start = self.bounds(offset, buf.len())?;
            if self.overlaps_bad_range(offset, buf.len()) {
                outcome = DeviceEventOutcome::Failed;
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::MediaError,
                });
            }
            scripted = self.scripted_effect(DeviceEventKind::Read, sequence, offset, buf.len());
            let effect = scripted.map(|(_, effect)| effect);
            match effect {
                None => {}
                Some(DeviceEffect::Fail) => {
                    outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::ReadFailed,
                    });
                }
                Some(DeviceEffect::Timeout) => {
                    outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Timeout,
                    });
                }
                Some(_) => unreachable!("invalid read effect was accepted"),
            }
            buf.copy_from_slice(&self.volatile[start..start + buf.len()]);
            Ok(())
        })();
        if result.is_err() && matches!(outcome, DeviceEventOutcome::Completed) {
            outcome = DeviceEventOutcome::Failed;
        }
        self.record_event(DeviceEvent {
            sequence,
            kind: DeviceEventKind::Read,
            offset,
            len: buf.len(),
            virtual_time,
            outcome,
            script_rule: scripted.map(|(rule, _)| rule),
            script_effect: scripted.map(|(_, effect)| effect),
        });
        result
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DeviceError> {
        let (sequence, virtual_time) = self.begin_event(DeviceEventKind::Write);
        let mut outcome = DeviceEventOutcome::Completed;
        let mut scripted = None;
        let result = (|| {
            let start = self.bounds(offset, buf.len())?;
            if self.overlaps_bad_range(offset, buf.len()) {
                outcome = DeviceEventOutcome::Failed;
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::MediaError,
                });
            }
            scripted = self.scripted_effect(DeviceEventKind::Write, sequence, offset, buf.len());
            let effect = scripted.map(|(_, effect)| effect);
            if matches!(effect, Some(DeviceEffect::CrashBefore)) {
                outcome = DeviceEventOutcome::Crashed;
                self.discard_volatile();
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::Crashed,
                });
            }
            match effect {
                Some(DeviceEffect::Fail) => {
                    outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Failed,
                    });
                }
                Some(DeviceEffect::Timeout) => {
                    outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::Injected {
                        at: InjectionSite::Event(sequence),
                        kind: FaultKind::Timeout,
                    });
                }
                _ => {}
            }
            let written = match effect {
                Some(DeviceEffect::Short { bytes }) => bytes.min(buf.len()),
                _ => buf.len(),
            };
            if let Some(DeviceEffect::Short { bytes }) = effect {
                if bytes >= buf.len() {
                    outcome = DeviceEventOutcome::Failed;
                    return Err(DeviceError::InvalidConfig(
                        "short write must be shorter than the requested length",
                    ));
                }
            }
            let tear_prefix = match effect {
                Some(DeviceEffect::Tear { durable_prefix })
                | Some(DeviceEffect::TearAndCrashAfter { durable_prefix }) => Some(durable_prefix),
                _ => None,
            };
            if !matches!(effect, Some(DeviceEffect::Drop)) {
                self.volatile[start..start + written].copy_from_slice(&buf[..written]);
                self.pending.push(PendingWrite {
                    ordinal: sequence,
                    offset,
                    data: buf[..written].to_vec(),
                    tear_prefix,
                });
            }
            if matches!(effect, Some(DeviceEffect::Drop)) {
                outcome = DeviceEventOutcome::Dropped;
            } else if written < buf.len() {
                outcome = DeviceEventOutcome::Short { bytes: written };
            } else if let Some(prefix) = tear_prefix {
                outcome = DeviceEventOutcome::Torn {
                    durable_prefix: self.durable_prefix(offset, written, prefix),
                };
            }
            if matches!(effect, Some(DeviceEffect::Short { .. })) {
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::ShortIo,
                });
            }
            if matches!(effect, Some(DeviceEffect::TearAndCrashAfter { .. })) {
                self.apply_one_pending(sequence);
                outcome = DeviceEventOutcome::Crashed;
                self.discard_volatile();
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::Crashed,
                });
            }
            if matches!(effect, Some(DeviceEffect::CrashAfter)) {
                outcome = DeviceEventOutcome::Crashed;
                self.discard_volatile();
                return Err(DeviceError::Injected {
                    at: InjectionSite::Event(sequence),
                    kind: FaultKind::Crashed,
                });
            }
            Ok(())
        })();
        if result.is_err() && matches!(outcome, DeviceEventOutcome::Completed) {
            outcome = DeviceEventOutcome::Failed;
        }
        self.record_event(DeviceEvent {
            sequence,
            kind: DeviceEventKind::Write,
            offset,
            len: buf.len(),
            virtual_time,
            outcome,
            script_rule: scripted.map(|(rule, _)| rule),
            script_effect: scripted.map(|(_, effect)| effect),
        });
        result
    }

    fn flush_data(&mut self) -> Result<(), DeviceError> {
        self.flush(DeviceEventKind::FlushData)
    }

    fn flush_all(&mut self) -> Result<(), DeviceError> {
        self.flush(DeviceEventKind::FlushAll)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(
        kind: DeviceEventKind,
        occurrence: EventOccurrence,
        effect: DeviceEffect,
    ) -> DeviceRule {
        DeviceRule {
            selector: EventSelector {
                kind,
                occurrence,
                range: None,
            },
            effect,
        }
    }

    #[test]
    fn volatile_data_is_lost_and_flushed_data_survives_power_loss() {
        let mut disk = SimDisk::new(16);
        disk.write_at(2, b"abc").unwrap();
        disk.power_loss();
        assert_eq!(&disk.durable_bytes()[2..5], b"\0\0\0");
        disk.write_at(2, b"abc").unwrap();
        disk.flush_data().unwrap();
        disk.power_loss();
        assert_eq!(&disk.durable_bytes()[2..5], b"abc");
    }

    #[test]
    fn script_is_event_and_range_based() {
        let script = DeviceScript {
            rules: vec![DeviceRule {
                selector: EventSelector {
                    kind: DeviceEventKind::Write,
                    occurrence: EventOccurrence::Exact(0),
                    range: Some(ByteRange { offset: 2, len: 2 }),
                },
                effect: DeviceEffect::Short { bytes: 2 },
            }],
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(16, script).unwrap();
        assert!(matches!(
            disk.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                at: InjectionSite::Event(0),
                kind: FaultKind::ShortIo
            })
        ));
        assert_eq!(disk.trace()[0].script_rule, Some(0));
        assert_eq!(
            disk.trace()[0].script_effect,
            Some(DeviceEffect::Short { bytes: 2 })
        );
        disk.flush_data().unwrap();
        assert_eq!(&disk.durable_bytes()[..4], b"ab\0\0");
    }

    #[test]
    fn torn_writes_are_atomic_unit_bounded_and_replayable() {
        let script = DeviceScript {
            atomic_unit: 2,
            rules: vec![rule(
                DeviceEventKind::Write,
                EventOccurrence::Exact(0),
                DeviceEffect::Tear { durable_prefix: 3 },
            )],
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(8, script).unwrap();
        disk.write_at(0, b"abcd").unwrap();
        disk.flush_data().unwrap();
        assert_eq!(&disk.durable_bytes()[..4], b"ab\0\0");
        assert_eq!(
            disk.trace()[0].outcome,
            DeviceEventOutcome::Torn { durable_prefix: 2 }
        );
    }

    #[test]
    fn flush_reorder_is_device_local_and_seeded() {
        let script = DeviceScript {
            reorder: Some(ReorderPolicy::Reverse),
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(4, script).unwrap();
        disk.write_at(0, b"aaaa").unwrap();
        disk.write_at(0, b"bbbb").unwrap();
        disk.flush_data().unwrap();
        assert_eq!(disk.durable_bytes(), b"aaaa");
    }

    #[test]
    fn bad_ranges_and_latency_are_device_properties() {
        let script = DeviceScript {
            latency: LatencyProfile {
                write_ticks: 5,
                flush_data_ticks: 7,
                ..Default::default()
            },
            bad_ranges: vec![ByteRange { offset: 2, len: 2 }],
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(16, script).unwrap();
        assert!(matches!(
            disk.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                kind: FaultKind::MediaError,
                ..
            })
        ));
        assert_eq!(disk.virtual_time(), 5);
        disk.write_at(0, b"a").unwrap();
        disk.flush_data().unwrap();
        assert_eq!(disk.virtual_time(), 17);
    }

    #[test]
    fn bad_range_precedes_a_write_script_rule_without_a_false_hit() {
        let script = DeviceScript {
            bad_ranges: vec![ByteRange { offset: 0, len: 4 }],
            rules: vec![rule(
                DeviceEventKind::Write,
                EventOccurrence::Exact(0),
                DeviceEffect::CrashBefore,
            )],
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(8, script).unwrap();
        assert!(matches!(
            disk.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                kind: FaultKind::MediaError,
                ..
            })
        ));
        assert_eq!(disk.script_rule_hits().lock().unwrap().as_slice(), &[false]);
        assert_eq!(disk.trace()[0].script_rule, None);
        assert_eq!(disk.trace()[0].script_effect, None);
    }

    #[test]
    fn bad_range_precedes_a_read_script_rule_without_a_false_hit() {
        let script = DeviceScript {
            bad_ranges: vec![ByteRange { offset: 0, len: 1 }],
            rules: vec![rule(
                DeviceEventKind::Read,
                EventOccurrence::Exact(0),
                DeviceEffect::Fail,
            )],
            ..Default::default()
        };
        let disk = SimDisk::from_script(8, script).unwrap();
        let mut byte = [0];
        assert!(matches!(
            disk.read_at(0, &mut byte),
            Err(DeviceError::Injected {
                kind: FaultKind::MediaError,
                ..
            })
        ));
        assert_eq!(disk.script_rule_hits().lock().unwrap().as_slice(), &[false]);
        assert_eq!(disk.trace()[0].script_rule, None);
        assert_eq!(disk.trace()[0].script_effect, None);
    }

    #[test]
    fn script_validation_rejects_ambiguous_rules() {
        let script = DeviceScript {
            rules: vec![
                rule(
                    DeviceEventKind::Write,
                    EventOccurrence::Exact(0),
                    DeviceEffect::Fail,
                ),
                rule(
                    DeviceEventKind::Write,
                    EventOccurrence::Exact(0),
                    DeviceEffect::Timeout,
                ),
            ],
            ..Default::default()
        };
        assert!(matches!(
            SimDisk::from_script(8, script),
            Err(DeviceError::InvalidConfig(_))
        ));

        let script = DeviceScript {
            rules: vec![
                DeviceRule {
                    selector: EventSelector {
                        kind: DeviceEventKind::Write,
                        occurrence: EventOccurrence::Exact(0),
                        range: Some(ByteRange { offset: 0, len: 1 }),
                    },
                    effect: DeviceEffect::Fail,
                },
                DeviceRule {
                    selector: EventSelector {
                        kind: DeviceEventKind::Write,
                        occurrence: EventOccurrence::Exact(0),
                        range: Some(ByteRange { offset: 2, len: 1 }),
                    },
                    effect: DeviceEffect::Timeout,
                },
            ],
            ..Default::default()
        };
        assert!(matches!(
            SimDisk::from_script(8, script),
            Err(DeviceError::InvalidConfig(_))
        ));
    }

    #[test]
    fn virtual_latency_and_trace_have_no_wall_clock_dependency() {
        let script = DeviceScript {
            latency: LatencyProfile {
                read_ticks: 2,
                write_ticks: 5,
                flush_data_ticks: 7,
                flush_all_ticks: 11,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(16, script).unwrap();
        disk.write_at(0, b"x").unwrap();
        disk.flush_data().unwrap();
        let mut byte = [0];
        disk.read_at(0, &mut byte).unwrap();
        disk.flush_all().unwrap();
        assert_eq!(disk.virtual_time(), 25);
        assert_eq!(disk.trace().len(), 4);
    }

    #[test]
    fn partial_flush_and_crash_preserve_only_the_device_durable_prefix() {
        let script = DeviceScript {
            flush_data_writes: Some(1),
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(8, script).unwrap();
        disk.write_at(0, b"aaaa").unwrap();
        disk.write_at(4, b"bbbb").unwrap();
        disk.flush_data().unwrap();
        assert_eq!(disk.durable_bytes(), b"aaaa\0\0\0\0");
        assert_eq!(disk.pending_writes(), 1);
        disk.power_loss();
        assert_eq!(disk.durable_bytes(), b"aaaa\0\0\0\0");
        assert_eq!(disk.pending_writes(), 0);
    }

    #[test]
    fn crash_before_is_recorded_as_the_failed_target_event() {
        let script = DeviceScript {
            rules: vec![rule(
                DeviceEventKind::Write,
                EventOccurrence::Exact(0),
                DeviceEffect::CrashBefore,
            )],
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(8, script).unwrap();
        assert!(matches!(
            disk.write_at(0, b"x"),
            Err(DeviceError::Injected {
                at: InjectionSite::Event(0),
                kind: FaultKind::Crashed
            })
        ));
        assert_eq!(disk.trace().len(), 1);
        assert_eq!(disk.trace()[0].outcome, DeviceEventOutcome::Crashed);
        assert_eq!(disk.trace()[0].kind, DeviceEventKind::Write);
    }

    #[test]
    fn tear_and_crash_persists_complete_atomic_units_before_power_loss() {
        let script = DeviceScript {
            atomic_unit: 2,
            rules: vec![rule(
                DeviceEventKind::Write,
                EventOccurrence::Exact(0),
                DeviceEffect::TearAndCrashAfter { durable_prefix: 3 },
            )],
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(8, script).unwrap();
        assert!(matches!(
            disk.write_at(0, b"abcd"),
            Err(DeviceError::Injected {
                kind: FaultKind::Crashed,
                ..
            })
        ));
        assert_eq!(&disk.durable_bytes()[..4], b"ab\0\0");
    }

    #[test]
    fn oversized_short_write_is_rejected_before_mutation() {
        let script = DeviceScript {
            rules: vec![rule(
                DeviceEventKind::Write,
                EventOccurrence::Exact(0),
                DeviceEffect::Short { bytes: 2 },
            )],
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(8, script).unwrap();
        assert!(matches!(
            disk.write_at(0, b"x"),
            Err(DeviceError::InvalidConfig(_))
        ));
        assert_eq!(disk.durable_bytes(), &[0; 8]);
    }

    #[test]
    fn full_length_short_write_is_rejected_before_mutation() {
        let script = DeviceScript {
            rules: vec![rule(
                DeviceEventKind::Write,
                EventOccurrence::Exact(0),
                DeviceEffect::Short { bytes: 1 },
            )],
            ..Default::default()
        };
        let mut disk = SimDisk::from_script(8, script).unwrap();
        assert!(matches!(
            disk.write_at(0, b"x"),
            Err(DeviceError::InvalidConfig(_))
        ));
        assert_eq!(disk.durable_bytes(), &[0; 8]);
        assert_eq!(disk.pending_writes(), 0);
    }

    #[test]
    fn script_json_rejects_unknown_nested_fields() {
        let json = r#"{
            "atomic_unit": 1,
            "rules": [{
                "selector": {"kind": "write", "occurrence": "any", "rang": null},
                "effect": "fail"
            }]
        }"#;
        assert!(serde_json::from_str::<DeviceScript>(json).is_err());

        let json = r#"{
            "rules": [{
                "selector": {
                    "kind": "write",
                    "occurrence": {"every": {"first": 0, "period": 2, "extra": 1}},
                    "range": null
                },
                "effect": "fail"
            }]
        }"#;
        assert!(serde_json::from_str::<DeviceScript>(json).is_err());

        let json = r#"{
            "reorder": {"seeded": {"window": 2, "seed": 1, "extra": 1}}
        }"#;
        assert!(serde_json::from_str::<DeviceScript>(json).is_err());
    }

    #[test]
    fn byte_level_reference_model_matches_ten_thousand_schedules() {
        for seed in 0..10_000u64 {
            let mut disk = SimDisk::new(64);
            let mut durable = vec![0; 64];
            let mut volatile = durable.clone();
            let mut pending = Vec::<(usize, Vec<u8>)>::new();
            let mut state = seed;
            for _ in 0..32 {
                state = mix(state, 1);
                let offset = (state % 56) as usize;
                let len = 1 + (mix(state, 2) % 8) as usize;
                match state % 6 {
                    0 | 1 => {
                        let bytes: Vec<u8> = (0..len)
                            .map(|index| mix(state, index as u64) as u8)
                            .collect();
                        disk.write_at(offset as u64, &bytes).unwrap();
                        volatile[offset..offset + len].copy_from_slice(&bytes);
                        pending.push((offset, bytes));
                    }
                    2 => {
                        let mut actual = vec![0; len];
                        disk.read_at(offset as u64, &mut actual).unwrap();
                        assert_eq!(actual, volatile[offset..offset + len]);
                    }
                    3 => {
                        disk.flush_data().unwrap();
                        for (write_offset, bytes) in pending.drain(..) {
                            durable[write_offset..write_offset + bytes.len()]
                                .copy_from_slice(&bytes);
                        }
                    }
                    4 => {
                        disk.flush_all().unwrap();
                        for (write_offset, bytes) in pending.drain(..) {
                            durable[write_offset..write_offset + bytes.len()]
                                .copy_from_slice(&bytes);
                        }
                    }
                    _ => {
                        disk.power_loss();
                        volatile.clone_from(&durable);
                        pending.clear();
                    }
                }
                assert_eq!(disk.durable_bytes(), durable);
            }
        }
    }
}
