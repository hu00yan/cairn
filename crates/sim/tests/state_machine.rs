use cairn_device::{
    BlockDevice, DeviceEffect, DeviceEventKind, DeviceEventOutcome, DeviceScript, ReorderPolicy,
    SimDisk,
};
use cairn_sim::replay::replay;
use cairn_sim::state_machine::{
    failure_artifact, failure_class, generate, generate_device_script, FailureClass,
};

#[test]
fn generated_cases_are_deterministic_and_replayable() {
    for seed in 0..1_000 {
        let case = generate(seed);
        assert_eq!(case, generate(seed));
        case.validate().unwrap();
        replay(&case).unwrap();
    }
}

#[test]
fn generator_exposes_device_script_and_virtual_time() {
    let case = generate(41);
    assert!(case.disk.script.latency.jitter_ticks > 0);
    let report = replay(&case).unwrap();
    assert!(report.device_events > 0);
    assert!(report.virtual_time > 0);
}

#[test]
fn failure_artifact_is_stable() {
    let case = generate(41);
    let error = cairn_sim::replay::ReplayError::InvalidCase("synthetic".into());
    let first = failure_artifact(&case, &error, |candidate| candidate.operations.len() > 2);
    let second = failure_artifact(&case, &error, |candidate| candidate.operations.len() > 2);
    assert_eq!(first, second);
    assert_eq!(failure_class(&error), FailureClass::InvalidCase);
}

#[test]
fn device_script_corpus_exercises_faults_and_is_replayable() {
    let mut effects = [false; 16];
    let mut kinds = [false; 4];
    let mut bad_range_was_observed = false;
    for seed in 0..10_000u64 {
        let script = generate_device_script(seed);
        let rule = script.rules[0];
        let mut first = SimDisk::from_script(64, script.clone()).unwrap();
        let mut second = SimDisk::from_script(64, script.clone()).unwrap();
        run_device_schedule(&mut first, &script);
        run_device_schedule(&mut second, &script);
        let mut expected = ExpectedDisk::new(64);
        run_reference_schedule(&mut expected, &script);
        assert_eq!(first.durable_bytes(), expected.durable);
        assert_eq!(first.pending_writes(), expected.pending.len());
        assert!(!expected.pending.is_empty());
        let event = first
            .trace()
            .into_iter()
            .find(|event| {
                event.kind == rule.selector.kind && rule.selector.occurrence.matches(event.sequence)
            })
            .expect("generated rule target must be observed");
        assert_eq!(event.kind, rule.selector.kind);
        assert_effect_outcome(rule.effect, event.outcome);
        effects[(seed % 16) as usize] = true;
        kinds[event_kind_index(event.kind)] = true;
        if !script.bad_ranges.is_empty()
            && first.trace().into_iter().any(|event| {
                event.kind == DeviceEventKind::Read
                    && event.offset == 48
                    && event.outcome == DeviceEventOutcome::Failed
            })
        {
            bad_range_was_observed = true;
        }
        let durable_before_power_loss = first.durable_bytes().to_vec();
        let expected_durable_before_power_loss = expected.durable.clone();
        first.power_loss();
        second.power_loss();
        expected.power_loss();
        assert_eq!(first.durable_bytes(), expected.durable);
        assert_eq!(first.pending_writes(), 0);
        assert_eq!(second.pending_writes(), 0);
        assert_eq!(expected.pending.len(), 0);
        assert_eq!(first.durable_bytes(), durable_before_power_loss);
        assert_eq!(expected.durable, expected_durable_before_power_loss);
        assert_eq!(first.durable_bytes(), second.durable_bytes());
        assert_eq!(first.trace(), second.trace());
    }
    assert!(effects.into_iter().all(|seen| seen));
    assert!(kinds.into_iter().all(|seen| seen));
    assert!(bad_range_was_observed);
}

fn assert_effect_outcome(effect: DeviceEffect, outcome: DeviceEventOutcome) {
    match (effect, outcome) {
        (DeviceEffect::Fail | DeviceEffect::Timeout, DeviceEventOutcome::Failed)
        | (DeviceEffect::Drop, DeviceEventOutcome::Dropped)
        | (DeviceEffect::Short { .. }, DeviceEventOutcome::Short { .. })
        | (DeviceEffect::Tear { .. }, DeviceEventOutcome::Torn { .. })
        | (
            DeviceEffect::CrashBefore
            | DeviceEffect::CrashAfter
            | DeviceEffect::TearAndCrashAfter { .. },
            DeviceEventOutcome::Crashed,
        ) => {}
        (effect, outcome) => panic!("effect {effect:?} produced {outcome:?}"),
    }
}

fn event_kind_index(kind: DeviceEventKind) -> usize {
    match kind {
        DeviceEventKind::Read => 0,
        DeviceEventKind::Write => 1,
        DeviceEventKind::FlushData => 2,
        DeviceEventKind::FlushAll => 3,
        DeviceEventKind::Crash => unreachable!("power loss is not a generated rule target"),
    }
}

fn run_device_schedule(disk: &mut SimDisk, script: &DeviceScript) {
    let _ = disk.write_at(0, b"abcd");
    let _ = disk.write_at(0, b"efgh");
    let _ = disk.flush_data();
    let _ = disk.flush_all();
    let mut bytes = [0; 4];
    let _ = disk.read_at(0, &mut bytes);
    let _ = disk.read_at(8, &mut bytes);
    if !script.bad_ranges.is_empty() {
        let _ = disk.read_at(48, &mut bytes);
    }
    let _ = disk.write_at(0, b"ijkl");
}

#[derive(Clone, Debug)]
struct ExpectedPending {
    ordinal: u64,
    offset: usize,
    data: Vec<u8>,
    tear_prefix: Option<usize>,
}

#[derive(Clone, Debug)]
struct ExpectedDisk {
    durable: Vec<u8>,
    volatile: Vec<u8>,
    pending: Vec<ExpectedPending>,
}

impl ExpectedDisk {
    fn new(len: usize) -> Self {
        Self {
            durable: vec![0; len],
            volatile: vec![0; len],
            pending: Vec::new(),
        }
    }

    fn write(&mut self, script: &DeviceScript, sequence: u64, offset: usize, data: &[u8]) {
        let effect = scripted_effect(script, DeviceEventKind::Write, sequence, offset, data.len());
        if matches!(effect, Some(DeviceEffect::CrashBefore)) {
            self.power_loss();
            return;
        }
        if matches!(
            effect,
            Some(DeviceEffect::Fail | DeviceEffect::Timeout | DeviceEffect::Drop)
        ) {
            return;
        }
        let written = match effect {
            Some(DeviceEffect::Short { bytes }) => bytes,
            _ => data.len(),
        };
        self.volatile[offset..offset + written].copy_from_slice(&data[..written]);
        let tear_prefix = match effect {
            Some(DeviceEffect::Tear { durable_prefix })
            | Some(DeviceEffect::TearAndCrashAfter { durable_prefix }) => Some(durable_prefix),
            _ => None,
        };
        self.pending.push(ExpectedPending {
            ordinal: sequence,
            offset,
            data: data[..written].to_vec(),
            tear_prefix,
        });
        if matches!(effect, Some(DeviceEffect::TearAndCrashAfter { .. })) {
            self.apply_one(script, sequence);
            self.power_loss();
        } else if matches!(effect, Some(DeviceEffect::CrashAfter)) {
            self.power_loss();
        }
    }

    fn flush(&mut self, script: &DeviceScript, sequence: u64, kind: DeviceEventKind) {
        let effect = scripted_effect(script, kind, sequence, 0, 0);
        if matches!(effect, Some(DeviceEffect::CrashBefore)) {
            self.power_loss();
            return;
        }
        if matches!(
            effect,
            Some(DeviceEffect::Fail | DeviceEffect::Timeout | DeviceEffect::Drop)
        ) {
            return;
        }
        let limit = (kind == DeviceEventKind::FlushData)
            .then_some(script.flush_data_writes)
            .flatten()
            .map(|limit| limit as usize);
        self.apply_pending(script, limit);
        if matches!(effect, Some(DeviceEffect::CrashAfter)) {
            self.power_loss();
        }
    }

    fn apply_one(&mut self, script: &DeviceScript, ordinal: u64) {
        if let Some(index) = self
            .pending
            .iter()
            .position(|write| write.ordinal == ordinal)
        {
            let write = self.pending.remove(index);
            self.persist(script, write);
        }
    }

    fn apply_pending(&mut self, script: &DeviceScript, limit: Option<usize>) {
        let mut writes = std::mem::take(&mut self.pending);
        match script.reorder {
            None => {}
            Some(ReorderPolicy::ByOffset) => {
                writes.sort_by_key(|write| (write.offset, write.ordinal));
            }
            Some(ReorderPolicy::Reverse) => writes.reverse(),
            Some(ReorderPolicy::Seeded { window, seed }) => {
                let window = window.max(1) as usize;
                for chunk in writes.chunks_mut(window) {
                    chunk.sort_by_key(|write| mix(seed, write.ordinal));
                }
            }
        }
        let count = limit.unwrap_or(writes.len()).min(writes.len());
        for write in writes.drain(..count) {
            self.persist(script, write);
        }
        self.pending = writes;
    }

    fn persist(&mut self, script: &DeviceScript, write: ExpectedPending) {
        let end = write
            .tear_prefix
            .unwrap_or(write.data.len())
            .min(write.data.len());
        let mut cursor = 0;
        while cursor < end {
            let remaining = script.atomic_unit - (write.offset + cursor) % script.atomic_unit;
            let segment = remaining.min(write.data.len() - cursor);
            if cursor + segment > end {
                break;
            }
            self.durable[write.offset + cursor..write.offset + cursor + segment]
                .copy_from_slice(&write.data[cursor..cursor + segment]);
            cursor += segment;
        }
    }

    fn power_loss(&mut self) {
        self.volatile.clone_from(&self.durable);
        self.pending.clear();
    }
}

fn run_reference_schedule(expected: &mut ExpectedDisk, script: &DeviceScript) {
    expected.write(script, 0, 0, b"abcd");
    expected.write(script, 1, 0, b"efgh");
    expected.flush(script, 2, DeviceEventKind::FlushData);
    expected.flush(script, 3, DeviceEventKind::FlushAll);
    expected.write(script, 7, 0, b"ijkl");
}

fn scripted_effect(
    script: &DeviceScript,
    kind: DeviceEventKind,
    sequence: u64,
    offset: usize,
    len: usize,
) -> Option<DeviceEffect> {
    script.rules.iter().find_map(|rule| {
        (rule.selector.kind == kind
            && rule.selector.occurrence.matches(sequence)
            && rule.selector.range.is_none_or(|range| {
                let end = offset as u64 + len as u64;
                let range_end = range.offset.saturating_add(range.len);
                (offset as u64) < range_end && range.offset < end
            }))
        .then_some(rule.effect)
    })
}

fn mix(mut value: u64, ordinal: u64) -> u64 {
    value ^= ordinal.wrapping_mul(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}
