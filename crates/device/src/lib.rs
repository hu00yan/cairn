use std::cmp::Reverse;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeviceError {
    OutOfBounds {
        offset: u64,
        len: usize,
        capacity: u64,
    },
    InvalidConfig(&'static str),
    Injected {
        op: u64,
        kind: FaultKind,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FaultKind {
    Failed,
    Crashed,
    Dropped,
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
    offset: u64,
    data: Vec<u8>,
    issued_at: u64,
    tear_prefix: Option<usize>,
}

#[derive(Debug)]
pub struct SimDisk {
    durable: Vec<u8>,
    volatile: Vec<u8>,
    pending: Vec<PendingWrite>,
    pub config: SimConfig,
    op_index: u64,
    faults: Vec<Fault>,
    flush_subset: Option<Vec<usize>>,
    flush_order: Option<Vec<usize>>,
    virtual_time: AtomicU64,
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
            virtual_time: AtomicU64::new(0),
        }
    }
    pub fn with_fault(mut self, fault: Fault) -> Self {
        self.faults.push(fault);
        self
    }
    pub fn with_faults(mut self, faults: Vec<Fault>) -> Self {
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
        self.virtual_time.load(Ordering::Relaxed)
    }
    pub fn try_new(len: usize, config: SimConfig) -> Result<Self, DeviceError> {
        config.clone().checked()?;
        Ok(Self::new(len, config))
    }
    pub fn set_flush_order(&mut self, pending_indices: Vec<usize>) -> Result<(), DeviceError> {
        self.validate_flush_indices(&pending_indices)?;
        self.flush_order = Some(pending_indices);
        Ok(())
    }
    pub fn corrupt_durable_range(&mut self, offset: u64, len: usize) -> Result<(), DeviceError> {
        let start = self.bounds(offset, len)?;
        for byte in &mut self.durable[start..start + len] {
            *byte ^= 0xa5;
        }
        Ok(())
    }
    pub fn set_flush_subset(&mut self, pending_indices: Vec<usize>) -> Result<(), DeviceError> {
        self.validate_flush_indices(&pending_indices)?;
        self.flush_subset = Some(pending_indices);
        Ok(())
    }
    fn validate_flush_indices(&self, indices: &[usize]) -> Result<(), DeviceError> {
        let mut seen = vec![false; self.pending.len()];
        for &index in indices {
            if index >= self.pending.len() {
                return Err(DeviceError::InvalidConfig(
                    "flush script index exceeds pending writes",
                ));
            }
            if seen[index] {
                return Err(DeviceError::InvalidConfig(
                    "flush script contains a duplicate index",
                ));
            }
            seen[index] = true;
        }
        Ok(())
    }
    pub fn crash(&mut self) {
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
                op,
                kind: FaultKind::Crashed,
            });
        }
        if failed {
            return Err(DeviceError::Injected {
                op,
                kind: FaultKind::Failed,
            });
        }
        if timed_out {
            return Err(DeviceError::Injected {
                op,
                kind: FaultKind::Timeout,
            });
        }
        Ok(op)
    }
    fn apply_pending(&mut self) {
        if self.config.allow_reordering {
            self.pending
                .sort_by_key(|w| (w.offset, Reverse(w.issued_at)));
        }
        let writes = std::mem::take(&mut self.pending);
        let subset = self.flush_subset.take();
        let order = self
            .flush_order
            .take()
            .unwrap_or_else(|| (0..writes.len()).collect());
        let mut writes: Vec<Option<PendingWrite>> = writes.into_iter().map(Some).collect();
        for index in order {
            let Some(w) = writes.get_mut(index).and_then(Option::take) else {
                continue;
            };
            if subset.as_ref().is_some_and(|s| !s.contains(&index)) {
                self.pending.push(w);
                continue;
            }
            let mut end = w.data.len();
            if let Some(prefix) = w.tear_prefix {
                end = prefix.min(end);
                if end < self.config.atomic_write_size {
                    end = 0;
                } else {
                    end -= end % self.config.atomic_write_size;
                }
            }
            self.durable[w.offset as usize..w.offset as usize + end]
                .copy_from_slice(&w.data[..end]);
        }
        self.pending.extend(writes.into_iter().flatten());
        // `volatile` already contains every issued write. Only crash resets it.
    }

    fn add_latency(&self, ticks: u64) {
        self.virtual_time
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |now| {
                Some(now.saturating_add(ticks))
            })
            .expect("virtual time update cannot fail");
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
                op,
                kind: FaultKind::Crashed,
            });
        }
        Ok(())
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
            virtual_time: AtomicU64::new(self.virtual_time()),
        }
    }
}

impl BlockDevice for SimDisk {
    fn len(&self) -> u64 {
        self.durable.len() as u64
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), DeviceError> {
        self.add_latency(self.config.latency.read_ticks);
        let start = self.bounds(offset, buf.len())?;
        for fault in &self.faults {
            if let Fault::ReadFail {
                offset: fault_offset,
                len: fault_len,
            } = fault
            {
                let request_end = offset.saturating_add(buf.len() as u64);
                let fault_end = fault_offset.saturating_add(*fault_len);
                if offset < fault_end && *fault_offset < request_end {
                    return Err(DeviceError::Injected {
                        op: self.op_index,
                        kind: FaultKind::ReadFailed,
                    });
                }
            }
        }
        buf.copy_from_slice(&self.volatile[start..start + buf.len()]);
        Ok(())
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DeviceError> {
        self.add_latency(self.config.latency.write_ticks);
        let op = self.before()?;
        let start = self.bounds(offset, buf.len())?;
        let dropped = if let Some(index) = self
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
            return self.apply_after_fault(op);
        }
        let data = buf.to_vec();
        let short_write = if let Some(index) = self.faults.iter().position(
            |fault| matches!(fault, Fault::ShortWrite { op: fault_op, .. } if *fault_op == op),
        ) {
            let Fault::ShortWrite { bytes, .. } = self.faults.remove(index) else {
                unreachable!("fault position matched ShortWrite")
            };
            let written = bytes.min(data.len());
            self.volatile[start..start + written].copy_from_slice(&data[..written]);
            self.pending.push(PendingWrite {
                offset,
                data: data[..written].to_vec(),
                issued_at: op,
                tear_prefix: None,
            });
            Some(DeviceError::Injected {
                op,
                kind: FaultKind::ShortIo,
            })
        } else {
            None
        };
        if let Some(error) = short_write {
            self.apply_after_fault(op)?;
            return Err(error);
        }
        let mut tear_prefix = None;
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
            offset,
            data,
            issued_at: op,
            tear_prefix,
        });
        self.apply_after_fault(op)
    }
    fn flush_data(&mut self) -> Result<(), DeviceError> {
        self.add_latency(self.config.latency.flush_data_ticks);
        let op = self.before()?;
        self.apply_pending();
        self.apply_after_fault(op)
    }
    fn flush_all(&mut self) -> Result<(), DeviceError> {
        self.add_latency(self.config.latency.flush_all_ticks);
        let op = self.before()?;
        self.flush_subset = None;
        self.flush_order = None;
        self.apply_pending();
        self.apply_after_fault(op)
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
