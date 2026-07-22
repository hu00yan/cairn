use std::fmt;
use std::sync::Mutex;

use crate::{
    BlockDevice, DeviceError, DeviceEvent, DeviceEventOutcome, DeviceScript, FaultKind, SimDisk,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaType {
    Hdd,
    Ssd,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JitterConfig {
    pub max_delay: u64,
    pub seed: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SsdSlowdownSegment {
    pub after_bytes: u64,
    pub extra_delay: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaRange {
    pub offset: u64,
    pub len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaConfig {
    pub media_type: MediaType,
    pub capacity: u64,
    /// Logical block size in bytes; capacity and I/O offsets are byte-based.
    pub logical_block_size: u64,
    pub base_latency: u64,
    #[serde(default)]
    pub hdd_seek_latency: u64,
    #[serde(default)]
    pub ssd_slowdown: Vec<SsdSlowdownSegment>,
    #[serde(default)]
    pub jitter: Option<JitterConfig>,
    pub device_script: DeviceScript,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaOperation {
    Read,
    Write,
    FlushData,
    FlushAll,
    Crash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MediaError {
    Device(DeviceError),
    InvalidConfig(&'static str),
}

impl From<DeviceError> for MediaError {
    fn from(error: DeviceError) -> Self {
        Self::Device(error)
    }
}

impl fmt::Display for MediaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for MediaError {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaTraceOutcome {
    Completed,
    Failed { kind: MediaFailureKind },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaFailureKind {
    Bounds,
    BadMedia,
    ReadFailed,
    Timeout,
    Injected,
    Crashed,
    ShortOrTorn,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MediaTraceEvent {
    pub sequence: u64,
    pub operation: MediaOperation,
    pub offset: u64,
    pub len: usize,
    pub virtual_time: u64,
    pub delay: u64,
    pub outcome: MediaTraceOutcome,
    pub device_event: DeviceEvent,
    pub device_error: Option<String>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ProfileState {
    hdd_head: u64,
    cumulative_writes: u64,
    virtual_time: u64,
}

#[derive(Debug)]
pub struct MediaModel {
    config: MediaConfig,
    disk: SimDisk,
    event_lock: Mutex<()>,
    profile: Mutex<ProfileState>,
    media_trace: Mutex<Vec<MediaTraceEvent>>,
}

impl MediaModel {
    pub fn new(config: MediaConfig) -> Result<Self, MediaError> {
        config.validate()?;
        let capacity = usize::try_from(config.capacity)
            .map_err(|_| MediaError::InvalidConfig("capacity does not fit usize"))?;
        let mut compiled_script = config.device_script.clone();
        compiled_script.latency.read_ticks = compiled_script
            .latency
            .read_ticks
            .saturating_add(config.base_latency);
        compiled_script.latency.write_ticks = compiled_script
            .latency
            .write_ticks
            .saturating_add(config.base_latency);
        compiled_script.latency.flush_data_ticks = compiled_script
            .latency
            .flush_data_ticks
            .saturating_add(config.base_latency);
        compiled_script.latency.flush_all_ticks = compiled_script
            .latency
            .flush_all_ticks
            .saturating_add(config.base_latency);
        if let Some(jitter) = config.jitter {
            compiled_script.latency.jitter_ticks = jitter.max_delay;
            compiled_script.latency.seed = jitter.seed;
        }
        let disk = SimDisk::from_script(capacity, compiled_script)?;
        Ok(Self {
            config,
            disk,
            event_lock: Mutex::new(()),
            profile: Mutex::new(ProfileState::default()),
            media_trace: Mutex::new(Vec::new()),
        })
    }

    pub fn config(&self) -> &MediaConfig {
        &self.config
    }
    pub const fn len(&self) -> u64 {
        self.config.capacity
    }
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn durable_bytes(&self) -> &[u8] {
        self.disk.durable_bytes()
    }
    pub fn pending_writes(&self) -> usize {
        self.disk.pending_writes()
    }
    pub fn trace(&self) -> Vec<DeviceEvent> {
        let _event_lock = self.event_lock.lock().expect("media event mutex poisoned");
        self.disk.trace()
    }
    pub fn profile_trace(&self) -> Vec<MediaTraceEvent> {
        let _event_lock = self.event_lock.lock().expect("media event mutex poisoned");
        self.media_trace
            .lock()
            .expect("media trace mutex poisoned")
            .clone()
    }
    pub fn virtual_time(&self) -> u64 {
        let _event_lock = self.event_lock.lock().expect("media event mutex poisoned");
        self.total_virtual_time()
    }

    pub fn crash(&mut self) {
        let _event_lock = self.event_lock.lock().expect("media event mutex poisoned");
        self.disk.power_loss();
        let event = self.latest_event();
        self.record_profile(MediaOperation::Crash, 0, 0, 0, &event, None);
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), MediaError> {
        let _event_lock = self.event_lock.lock().expect("media event mutex poisoned");
        let valid = self.checked_range(offset, buf.len()).is_ok();
        let delay = self.position_delay(offset, buf.len(), false, valid);
        let result = self.disk.read_at(offset, buf).map_err(MediaError::Device);
        let event = self.latest_event();
        self.update_head(&event);
        self.record_profile(
            MediaOperation::Read,
            offset,
            buf.len(),
            delay,
            &event,
            media_device_error(result.as_ref().err()),
        );
        result
    }

    pub fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), MediaError> {
        let _event_lock = self.event_lock.lock().expect("media event mutex poisoned");
        let valid = self.checked_range(offset, buf.len()).is_ok();
        let delay = self.position_delay(offset, buf.len(), true, valid);
        let result = self.disk.write_at(offset, buf).map_err(MediaError::Device);
        let event = self.latest_event();
        if valid {
            let mut profile = self.profile.lock().expect("media profile mutex poisoned");
            profile.cumulative_writes = profile.cumulative_writes.saturating_add(buf.len() as u64);
        }
        self.update_head(&event);
        self.record_profile(
            MediaOperation::Write,
            offset,
            buf.len(),
            delay,
            &event,
            media_device_error(result.as_ref().err()),
        );
        result
    }

    pub fn flush_data(&mut self) -> Result<(), MediaError> {
        let _event_lock = self.event_lock.lock().expect("media event mutex poisoned");
        let result = self.disk.flush_data().map_err(MediaError::Device);
        let event = self.latest_event();
        self.record_profile(
            MediaOperation::FlushData,
            0,
            0,
            0,
            &event,
            media_device_error(result.as_ref().err()),
        );
        result
    }

    pub fn flush_all(&mut self) -> Result<(), MediaError> {
        let _event_lock = self.event_lock.lock().expect("media event mutex poisoned");
        let result = self.disk.flush_all().map_err(MediaError::Device);
        let event = self.latest_event();
        self.record_profile(
            MediaOperation::FlushAll,
            0,
            0,
            0,
            &event,
            media_device_error(result.as_ref().err()),
        );
        result
    }

    fn position_delay(&self, offset: u64, len: usize, write: bool, valid: bool) -> u64 {
        let mut profile = self.profile.lock().expect("media profile mutex poisoned");
        let mut delay = 0;
        if valid && self.config.media_type == MediaType::Hdd {
            delay = offset
                .abs_diff(profile.hdd_head)
                .saturating_mul(self.config.hdd_seek_latency);
        }
        if valid && self.config.media_type == MediaType::Ssd && write {
            let end = profile.cumulative_writes.saturating_add(len as u64);
            delay = self
                .config
                .ssd_slowdown
                .iter()
                .filter(|segment| end >= segment.after_bytes)
                .fold(delay, |total, segment| {
                    total.saturating_add(segment.extra_delay)
                });
        }
        profile.virtual_time = profile.virtual_time.saturating_add(delay);
        delay
    }

    fn record_profile(
        &self,
        operation: MediaOperation,
        offset: u64,
        len: usize,
        delay: u64,
        event: &DeviceEvent,
        error: Option<&DeviceError>,
    ) {
        let kind = failure_kind(event, error);
        let outcome = kind.map_or(MediaTraceOutcome::Completed, |kind| {
            MediaTraceOutcome::Failed { kind }
        });
        let virtual_time = self.total_virtual_time();
        let mut trace = self.media_trace.lock().expect("media trace mutex poisoned");
        trace.push(MediaTraceEvent {
            sequence: event.sequence,
            operation,
            offset,
            len,
            virtual_time,
            delay,
            outcome,
            device_event: event.clone(),
            device_error: error.map(ToString::to_string),
        });
    }

    fn latest_event(&self) -> DeviceEvent {
        self.disk
            .trace()
            .pop()
            .expect("SimDisk operation must emit a device event")
    }

    fn total_virtual_time(&self) -> u64 {
        self.disk.virtual_time().saturating_add(
            self.profile
                .lock()
                .expect("media profile mutex poisoned")
                .virtual_time,
        )
    }

    fn update_head(&self, event: &DeviceEvent) {
        if self.config.media_type != MediaType::Hdd {
            return;
        }
        let transferred = transferred_len(event);
        if transferred != 0 {
            self.profile
                .lock()
                .expect("media profile mutex poisoned")
                .hdd_head = event.offset.saturating_add(transferred as u64);
        }
    }

    fn checked_range(&self, offset: u64, len: usize) -> Result<(), MediaError> {
        offset
            .checked_add(len as u64)
            .filter(|end| *end <= self.len())
            .map(|_| ())
            .ok_or(MediaError::Device(DeviceError::OutOfBounds {
                offset,
                len,
                capacity: self.len(),
            }))
    }
}

impl MediaConfig {
    fn validate(&self) -> Result<(), MediaError> {
        if self.capacity == 0 || self.logical_block_size == 0 {
            return Err(MediaError::InvalidConfig(
                "capacity and logical_block_size must be non-zero",
            ));
        }
        if self.jitter.is_some()
            && (self.device_script.latency.jitter_ticks != 0
                || self.device_script.latency.seed != 0)
        {
            return Err(MediaError::InvalidConfig(
                "media jitter and device script jitter cannot both be configured",
            ));
        }
        if !self.capacity.is_multiple_of(self.logical_block_size) {
            return Err(MediaError::InvalidConfig(
                "capacity must be a multiple of logical_block_size",
            ));
        }
        if self.media_type == MediaType::Hdd && !self.ssd_slowdown.is_empty() {
            return Err(MediaError::InvalidConfig(
                "ssd slowdown is only valid for SSD media",
            ));
        }
        if self.media_type == MediaType::Ssd && self.hdd_seek_latency != 0 {
            return Err(MediaError::InvalidConfig(
                "hdd seek latency is only valid for HDD media",
            ));
        }
        if self
            .ssd_slowdown
            .windows(2)
            .any(|w| w[0].after_bytes >= w[1].after_bytes)
        {
            return Err(MediaError::InvalidConfig(
                "ssd slowdown thresholds must increase",
            ));
        }
        Ok(())
    }
}

fn transferred_len(event: &DeviceEvent) -> usize {
    match event.script_effect {
        Some(crate::DeviceEffect::CrashBefore) => return 0,
        Some(
            crate::DeviceEffect::CrashAfter
            | crate::DeviceEffect::Tear { .. }
            | crate::DeviceEffect::TearAndCrashAfter { .. },
        ) => return event.len,
        _ => {}
    }
    match event.outcome {
        DeviceEventOutcome::Completed => event.len,
        DeviceEventOutcome::Dropped => 0,
        DeviceEventOutcome::Short { bytes } => bytes,
        DeviceEventOutcome::Torn { durable_prefix } => durable_prefix,
        DeviceEventOutcome::Failed | DeviceEventOutcome::Crashed => 0,
    }
}

fn failure_kind(event: &DeviceEvent, error: Option<&DeviceError>) -> Option<MediaFailureKind> {
    match event.outcome {
        DeviceEventOutcome::Completed => None,
        DeviceEventOutcome::Dropped => Some(MediaFailureKind::Injected),
        DeviceEventOutcome::Short { .. } | DeviceEventOutcome::Torn { .. } => {
            Some(MediaFailureKind::ShortOrTorn)
        }
        DeviceEventOutcome::Crashed => Some(MediaFailureKind::Crashed),
        DeviceEventOutcome::Failed => Some(match error {
            Some(DeviceError::OutOfBounds { .. }) => MediaFailureKind::Bounds,
            Some(DeviceError::Injected { kind, .. }) => match kind {
                FaultKind::MediaError => MediaFailureKind::BadMedia,
                FaultKind::ReadFailed => MediaFailureKind::ReadFailed,
                FaultKind::Timeout => MediaFailureKind::Timeout,
                FaultKind::Crashed => MediaFailureKind::Crashed,
                FaultKind::ShortIo => MediaFailureKind::ShortOrTorn,
                FaultKind::Failed | FaultKind::Dropped => MediaFailureKind::Injected,
            },
            _ => MediaFailureKind::Injected,
        }),
    }
}

fn media_device_error(error: Option<&MediaError>) -> Option<&DeviceError> {
    error.and_then(|error| match error {
        MediaError::Device(device_error) => Some(device_error),
        MediaError::InvalidConfig(_) => None,
    })
}

impl BlockDevice for MediaModel {
    fn len(&self) -> u64 {
        self.len()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), DeviceError> {
        MediaModel::read_at(self, offset, buf).map_err(media_error_to_device)
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DeviceError> {
        MediaModel::write_at(self, offset, buf).map_err(media_error_to_device)
    }

    fn flush_data(&mut self) -> Result<(), DeviceError> {
        MediaModel::flush_data(self).map_err(media_error_to_device)
    }

    fn flush_all(&mut self) -> Result<(), DeviceError> {
        MediaModel::flush_all(self).map_err(media_error_to_device)
    }
}

fn media_error_to_device(error: MediaError) -> DeviceError {
    match error {
        MediaError::Device(error) => error,
        MediaError::InvalidConfig(message) => {
            unreachable!("validated MediaModel produced runtime configuration error: {message}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeviceEffect, DeviceEventKind, DeviceEventOutcome, DeviceRule, EventSelector};

    fn config(media_type: MediaType) -> MediaConfig {
        MediaConfig {
            media_type,
            capacity: 64,
            logical_block_size: 4,
            base_latency: 10,
            hdd_seek_latency: if media_type == MediaType::Hdd { 2 } else { 0 },
            ssd_slowdown: Vec::new(),
            jitter: None,
            device_script: DeviceScript::default(),
        }
    }

    #[test]
    fn hdd_seek_is_from_the_previous_position() {
        let mut model = MediaModel::new(config(MediaType::Hdd)).unwrap();
        model.write_at(4, &[1, 2, 3, 4]).unwrap();
        let mut read = [0; 4];
        model.read_at(20, &mut read).unwrap();

        assert_eq!(model.virtual_time(), 52);
    }

    #[test]
    fn ssd_slowdown_is_applied_by_cumulative_write_threshold() {
        let mut cfg = config(MediaType::Ssd);
        cfg.ssd_slowdown = vec![
            SsdSlowdownSegment {
                after_bytes: 4,
                extra_delay: 7,
            },
            SsdSlowdownSegment {
                after_bytes: 8,
                extra_delay: 11,
            },
        ];
        let mut model = MediaModel::new(cfg).unwrap();
        model.write_at(0, &[0; 3]).unwrap();
        model.write_at(4, &[0; 2]).unwrap();
        model.write_at(8, &[0; 4]).unwrap();

        assert_eq!(model.virtual_time(), 55);
    }

    #[test]
    fn jitter_is_reproducible_from_seed_and_sequence() {
        let mut cfg = config(MediaType::Ssd);
        cfg.jitter = Some(JitterConfig {
            max_delay: 9,
            seed: 0x1234,
        });
        let mut first = MediaModel::new(cfg.clone()).unwrap();
        let mut second = MediaModel::new(cfg).unwrap();
        first.write_at(0, &[1, 2, 3, 4]).unwrap();
        second.write_at(0, &[1, 2, 3, 4]).unwrap();
        first.flush_all().unwrap();
        second.flush_all().unwrap();
        assert_eq!(first.trace(), second.trace());
    }

    #[test]
    fn media_jitter_is_applied_without_mutating_config() {
        let mut cfg = config(MediaType::Ssd);
        cfg.jitter = Some(JitterConfig {
            max_delay: 9,
            seed: 0x1234,
        });
        let model = MediaModel::new(cfg.clone()).unwrap();
        assert_eq!(model.config(), &cfg);
        assert!(model.config().jitter.is_some());
    }

    #[test]
    fn bad_block_and_bounds_are_distinct_errors() {
        let mut cfg = config(MediaType::Ssd);
        cfg.device_script.bad_ranges = vec![crate::ByteRange { offset: 8, len: 4 }];
        let mut model = MediaModel::new(cfg).unwrap();
        assert!(matches!(
            model.write_at(8, &[1; 4]),
            Err(MediaError::Device(crate::DeviceError::Injected { .. }))
        ));
        assert_eq!(
            model.read_at(63, &mut [0; 2]),
            Err(MediaError::Device(crate::DeviceError::OutOfBounds {
                offset: 63,
                len: 2,
                capacity: 64
            }))
        );
        assert_eq!(
            model
                .trace()
                .iter()
                .map(|event| &event.outcome)
                .collect::<Vec<_>>(),
            vec![
                &crate::DeviceEventOutcome::Failed,
                &crate::DeviceEventOutcome::Failed,
            ]
        );
        assert_eq!(model.trace()[0].kind, crate::DeviceEventKind::Write);
        assert_eq!(model.trace()[1].kind, crate::DeviceEventKind::Read);
        assert_eq!(
            model.profile_trace()[0].outcome,
            MediaTraceOutcome::Failed {
                kind: MediaFailureKind::BadMedia
            }
        );
        assert_eq!(
            model.profile_trace()[1].outcome,
            MediaTraceOutcome::Failed {
                kind: MediaFailureKind::Bounds
            }
        );
    }

    #[test]
    fn invalid_range_is_traced_without_moving_hdd_head() {
        let model = MediaModel::new(config(MediaType::Hdd)).unwrap();
        assert!(model.read_at(64, &mut [0]).is_err());
        model.read_at(4, &mut [0]).unwrap();
        // The rejected request still consumes the device's base operation
        // latency, but it must not move the HDD head.
        assert_eq!(model.virtual_time(), 28);
        assert_eq!(model.trace()[0].kind, crate::DeviceEventKind::Read);
    }

    #[test]
    fn crash_discards_volatile_data_but_flush_makes_it_durable() {
        let mut model = MediaModel::new(config(MediaType::Ssd)).unwrap();
        model.write_at(0, &[9; 4]).unwrap();
        assert_eq!(&model.durable_bytes()[..4], &[0; 4]);
        model.crash();
        let mut read = [0; 4];
        model.read_at(0, &mut read).unwrap();
        assert_eq!(read, [0; 4]);

        model.write_at(0, &[7; 4]).unwrap();
        model.flush_data().unwrap();
        model.write_at(0, &[3; 4]).unwrap();
        model.crash();
        model.read_at(0, &mut read).unwrap();
        assert_eq!(read, [7; 4]);
    }

    #[test]
    fn injected_fault_keeps_device_error_details_and_trace_metadata() {
        let mut cfg = config(MediaType::Ssd);
        cfg.device_script.rules.push(DeviceRule {
            selector: EventSelector {
                kind: DeviceEventKind::Write,
                occurrence: crate::EventOccurrence::Any,
                range: None,
            },
            effect: DeviceEffect::Fail,
        });
        let mut model = MediaModel::new(cfg).unwrap();
        assert!(matches!(
            model.write_at(0, &[1]),
            Err(MediaError::Device(crate::DeviceError::Injected { .. }))
        ));
        let event = &model.trace()[0];
        assert_eq!(event.outcome, DeviceEventOutcome::Failed);
        assert_eq!(event.script_effect, Some(DeviceEffect::Fail));
    }

    #[test]
    fn profile_delay_saturates_without_overflow() {
        let mut cfg = config(MediaType::Hdd);
        cfg.base_latency = u64::MAX;
        cfg.hdd_seek_latency = u64::MAX;
        cfg.device_script.latency.read_ticks = u64::MAX;
        let model = MediaModel::new(cfg).unwrap();
        model.read_at(1, &mut [0]).unwrap();
        assert_eq!(model.virtual_time(), u64::MAX);
    }

    #[test]
    fn config_roundtrip_preserves_uncompiled_media_config() {
        let mut cfg = config(MediaType::Ssd);
        cfg.base_latency = 17;
        cfg.device_script.latency.write_ticks = 3;
        let encoded = serde_json::to_string(&cfg).unwrap();
        let decoded: MediaConfig = serde_json::from_str(&encoded).unwrap();
        let model = MediaModel::new(decoded).unwrap();

        assert_eq!(model.config(), &cfg);
        assert_eq!(model.disk.script().latency.write_ticks, 20);
    }

    #[test]
    fn media_and_device_jitter_cannot_be_merged() {
        let mut cfg = config(MediaType::Ssd);
        cfg.jitter = Some(JitterConfig {
            max_delay: 1,
            seed: 7,
        });
        cfg.device_script.latency.jitter_ticks = 1;
        assert!(matches!(
            MediaModel::new(cfg.clone()),
            Err(MediaError::InvalidConfig(
                "media jitter and device script jitter cannot both be configured"
            ))
        ));
        cfg.device_script.latency.jitter_ticks = 0;
        cfg.device_script.latency.seed = 7;
        cfg.jitter = Some(JitterConfig {
            max_delay: 0,
            seed: 8,
        });
        assert!(matches!(
            MediaModel::new(cfg),
            Err(MediaError::InvalidConfig(
                "media jitter and device script jitter cannot both be configured"
            ))
        ));
    }

    #[test]
    fn ssd_slowdown_counts_attempted_valid_write_bytes() {
        let mut cfg = config(MediaType::Ssd);
        cfg.base_latency = 0;
        cfg.ssd_slowdown = vec![
            SsdSlowdownSegment {
                after_bytes: 1,
                extra_delay: 5,
            },
            SsdSlowdownSegment {
                after_bytes: 3,
                extra_delay: 7,
            },
        ];
        cfg.device_script.rules = vec![
            DeviceRule {
                selector: EventSelector {
                    kind: DeviceEventKind::Write,
                    occurrence: crate::EventOccurrence::Exact(0),
                    range: None,
                },
                effect: DeviceEffect::Fail,
            },
            DeviceRule {
                selector: EventSelector {
                    kind: DeviceEventKind::Write,
                    occurrence: crate::EventOccurrence::Exact(1),
                    range: None,
                },
                effect: DeviceEffect::Drop,
            },
            DeviceRule {
                selector: EventSelector {
                    kind: DeviceEventKind::Write,
                    occurrence: crate::EventOccurrence::Exact(2),
                    range: None,
                },
                effect: DeviceEffect::Short { bytes: 1 },
            },
        ];
        let mut model = MediaModel::new(cfg).unwrap();
        assert!(model.write_at(0, &[1, 2]).is_err());
        assert!(model.write_at(4, &[1, 2]).is_ok());
        assert!(model.write_at(8, &[1, 2]).is_err());

        assert_eq!(model.virtual_time(), 29);
        assert_eq!(
            model.profile_trace()[2].outcome,
            MediaTraceOutcome::Failed {
                kind: MediaFailureKind::ShortOrTorn,
            }
        );
    }

    #[test]
    fn hdd_head_uses_transferred_interval_for_contiguous_reads() {
        let mut cfg = config(MediaType::Hdd);
        cfg.base_latency = 0;
        let mut model = MediaModel::new(cfg).unwrap();
        model.write_at(8, &[1, 2, 3, 4]).unwrap();
        let mut first = [0; 4];
        model.read_at(12, &mut first).unwrap();
        let mut second = [0; 4];
        model.read_at(16, &mut second).unwrap();

        assert_eq!(model.virtual_time(), 16);
        assert_eq!(model.profile_trace()[1].sequence, model.trace()[1].sequence);
        assert_eq!(model.profile_trace()[2].sequence, model.trace()[2].sequence);
    }

    #[test]
    fn hdd_head_uses_full_transfer_for_crash_after() {
        let mut cfg = config(MediaType::Hdd);
        cfg.base_latency = 0;
        cfg.hdd_seek_latency = 1;
        cfg.device_script.rules = vec![DeviceRule {
            selector: EventSelector {
                kind: DeviceEventKind::Write,
                occurrence: crate::EventOccurrence::Exact(0),
                range: None,
            },
            effect: DeviceEffect::CrashAfter,
        }];
        let mut model = MediaModel::new(cfg).unwrap();
        assert!(model.write_at(8, &[1; 4]).is_err());
        model.read_at(12, &mut [0]).unwrap();
        assert_eq!(model.profile_trace()[1].delay, 0);
    }

    #[test]
    fn block_device_trait_returns_device_errors_and_preserves_effects() {
        let mut cfg = config(MediaType::Ssd);
        cfg.device_script.rules.push(DeviceRule {
            selector: EventSelector {
                kind: DeviceEventKind::Write,
                occurrence: crate::EventOccurrence::Exact(0),
                range: None,
            },
            effect: DeviceEffect::Drop,
        });
        let mut model = MediaModel::new(cfg).unwrap();
        let device: &mut dyn BlockDevice = &mut model;
        device.write_at(0, &[9; 4]).unwrap();
        assert_eq!(device.len(), 64);
        assert_eq!(model.trace()[0].outcome, DeviceEventOutcome::Dropped);
    }
}
