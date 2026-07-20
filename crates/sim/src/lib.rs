#![deny(unsafe_code)]

use std::collections::{HashMap, HashSet};

pub type NodeId = u16;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct VirtualTime(u64);

impl VirtualTime {
    pub const fn ticks(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LinkPolicy {
    pub latency_ticks: u64,
    pub reorder: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkError {
    NodePaused(NodeId),
    NodeCrashed(NodeId),
    Partitioned(NodeId, NodeId),
    Disconnected(NodeId, NodeId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivered {
    pub from: NodeId,
    pub to: NodeId,
    pub payload: Vec<u8>,
    pub sent_at: VirtualTime,
    pub delivered_at: VirtualTime,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkEvent {
    Sent {
        sequence: u64,
        from: NodeId,
        to: NodeId,
    },
    Dropped {
        sequence: u64,
        from: NodeId,
        to: NodeId,
    },
    Duplicated {
        sequence: u64,
        copies: usize,
    },
    NodePaused(NodeId),
    NodeResumed(NodeId),
    NodeCrashed(NodeId),
    NodeRestarted(NodeId),
    Delivered {
        sequence: u64,
        from: NodeId,
        to: NodeId,
    },
}

#[derive(Clone, Debug)]
struct Packet {
    from: NodeId,
    to: NodeId,
    payload: Vec<u8>,
    sent_at: VirtualTime,
    deliver_at: VirtualTime,
    sequence: u64,
    reorder: bool,
}

#[derive(Clone, Debug, Default)]
struct LinkFaults {
    drop_next: usize,
    duplicate_next: usize,
}

#[derive(Clone, Debug)]
pub struct SimNetwork {
    now: VirtualTime,
    default_policy: LinkPolicy,
    policies: HashMap<(NodeId, NodeId), LinkPolicy>,
    faults: HashMap<(NodeId, NodeId), LinkFaults>,
    partitions: HashSet<(NodeId, NodeId)>,
    disconnected: HashSet<(NodeId, NodeId)>,
    paused: HashSet<NodeId>,
    crashed: HashSet<NodeId>,
    node_epochs: HashMap<NodeId, u64>,
    packets: Vec<Packet>,
    next_sequence: u64,
    trace: Vec<NetworkEvent>,
}

impl SimNetwork {
    pub fn new(default_latency_ticks: u64) -> Self {
        Self {
            now: VirtualTime::default(),
            default_policy: LinkPolicy {
                latency_ticks: default_latency_ticks,
                reorder: false,
            },
            policies: HashMap::new(),
            faults: HashMap::new(),
            partitions: HashSet::new(),
            disconnected: HashSet::new(),
            paused: HashSet::new(),
            crashed: HashSet::new(),
            node_epochs: HashMap::new(),
            packets: Vec::new(),
            next_sequence: 0,
            trace: Vec::new(),
        }
    }

    pub const fn now(&self) -> VirtualTime {
        self.now
    }

    pub fn set_default_policy(&mut self, policy: LinkPolicy) {
        self.default_policy = policy;
    }

    pub fn set_link_policy(&mut self, from: NodeId, to: NodeId, policy: LinkPolicy) {
        self.policies.insert((from, to), policy);
    }

    pub fn drop_next(&mut self, from: NodeId, to: NodeId, count: usize) {
        self.faults.entry((from, to)).or_default().drop_next = count;
    }

    pub fn duplicate_next(&mut self, from: NodeId, to: NodeId, count: usize) {
        self.faults.entry((from, to)).or_default().duplicate_next = count;
    }

    pub fn pause(&mut self, node: NodeId) {
        self.paused.insert(node);
        self.trace.push(NetworkEvent::NodePaused(node));
    }

    pub fn resume(&mut self, node: NodeId) {
        self.paused.remove(&node);
        self.trace.push(NetworkEvent::NodeResumed(node));
    }

    pub fn crash_node(&mut self, node: NodeId) {
        self.crashed.insert(node);
        self.paused.insert(node);
        self.packets
            .retain(|packet| packet.from != node && packet.to != node);
        self.trace.push(NetworkEvent::NodeCrashed(node));
    }

    pub fn restart_node(&mut self, node: NodeId) {
        self.crashed.remove(&node);
        self.paused.remove(&node);
        let epoch = self.node_epochs.entry(node).or_default();
        *epoch = epoch.saturating_add(1);
        self.trace.push(NetworkEvent::NodeRestarted(node));
    }

    pub fn node_epoch(&self, node: NodeId) -> u64 {
        self.node_epochs.get(&node).copied().unwrap_or(0)
    }

    pub fn partition(&mut self, left: NodeId, right: NodeId) {
        self.partitions.insert((left, right));
        self.partitions.insert((right, left));
    }

    pub fn heal_partition(&mut self, left: NodeId, right: NodeId) {
        self.partitions.remove(&(left, right));
        self.partitions.remove(&(right, left));
    }

    pub fn disconnect(&mut self, from: NodeId, to: NodeId) {
        self.disconnected.insert((from, to));
    }

    pub fn reconnect(&mut self, from: NodeId, to: NodeId) {
        self.disconnected.remove(&(from, to));
    }

    pub fn send(
        &mut self,
        from: NodeId,
        to: NodeId,
        payload: Vec<u8>,
    ) -> Result<u64, NetworkError> {
        if self.crashed.contains(&from) {
            return Err(NetworkError::NodeCrashed(from));
        }
        if self.paused.contains(&from) {
            return Err(NetworkError::NodePaused(from));
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.trace.push(NetworkEvent::Sent { sequence, from, to });
        if self.crashed.contains(&to)
            || self.disconnected.contains(&(from, to))
            || self.partitions.contains(&(from, to))
        {
            self.trace
                .push(NetworkEvent::Dropped { sequence, from, to });
            return Ok(sequence);
        }
        let faults = self.faults.entry((from, to)).or_default();
        if faults.drop_next > 0 {
            faults.drop_next -= 1;
            self.trace
                .push(NetworkEvent::Dropped { sequence, from, to });
            return Ok(sequence);
        }
        let policy = self
            .policies
            .get(&(from, to))
            .copied()
            .unwrap_or(self.default_policy);
        let copies = if faults.duplicate_next > 0 {
            faults.duplicate_next -= 1;
            2
        } else {
            1
        };
        if copies > 1 {
            self.trace
                .push(NetworkEvent::Duplicated { sequence, copies });
        }
        for _ in 0..copies {
            self.packets.push(Packet {
                from,
                to,
                payload: payload.clone(),
                sent_at: self.now,
                deliver_at: VirtualTime(self.now.0.saturating_add(policy.latency_ticks)),
                sequence,
                reorder: policy.reorder,
            });
        }
        Ok(sequence)
    }

    pub fn advance(&mut self, ticks: u64) {
        self.now = VirtualTime(self.now.0.saturating_add(ticks));
    }

    pub fn deliver_ready(&mut self) -> Vec<Delivered> {
        let mut ready = Vec::new();
        let mut pending = Vec::with_capacity(self.packets.len());
        for packet in self.packets.drain(..) {
            if packet.deliver_at <= self.now && !self.paused.contains(&packet.to) {
                if self.crashed.contains(&packet.to)
                    || self.disconnected.contains(&(packet.from, packet.to))
                    || self.partitions.contains(&(packet.from, packet.to))
                {
                    self.trace.push(NetworkEvent::Dropped {
                        sequence: packet.sequence,
                        from: packet.from,
                        to: packet.to,
                    });
                    continue;
                }
                ready.push(packet);
            } else {
                pending.push(packet);
            }
        }
        self.packets = pending;
        ready.sort_by_key(|packet| {
            (
                packet.deliver_at,
                packet.reorder,
                if packet.reorder {
                    u64::MAX - packet.sequence
                } else {
                    packet.sequence
                },
                packet.from,
                packet.to,
                packet.sequence,
            )
        });
        ready
            .into_iter()
            .map(|packet| {
                self.trace.push(NetworkEvent::Delivered {
                    sequence: packet.sequence,
                    from: packet.from,
                    to: packet.to,
                });
                Delivered {
                    from: packet.from,
                    to: packet.to,
                    payload: packet.payload,
                    sent_at: packet.sent_at,
                    delivered_at: self.now,
                    sequence: packet.sequence,
                }
            })
            .collect()
    }

    pub fn pending_packets(&self) -> usize {
        self.packets.len()
    }

    pub fn trace(&self) -> &[NetworkEvent] {
        &self.trace
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_latency_controls_delivery_without_sleeping() {
        let mut network = SimNetwork::new(10);
        network.send(1, 2, b"ping".to_vec()).unwrap();
        assert!(network.deliver_ready().is_empty());
        network.advance(9);
        assert!(network.deliver_ready().is_empty());
        network.advance(1);
        let packets = network.deliver_ready();
        assert_eq!(packets[0].payload, b"ping");
        assert_eq!(packets[0].sent_at.ticks(), 0);
        assert_eq!(packets[0].delivered_at.ticks(), 10);
    }

    #[test]
    fn latency_boundary_property_holds_for_a_range_of_tick_values() {
        for latency in 0..=32 {
            let mut network = SimNetwork::new(latency);
            network.send(1, 2, vec![latency as u8]).unwrap();
            if latency > 0 {
                network.advance(latency - 1);
                assert!(network.deliver_ready().is_empty());
                network.advance(1);
            }
            let delivered = network.deliver_ready();
            assert_eq!(delivered.len(), 1);
            assert_eq!(delivered[0].delivered_at.ticks(), latency);
            assert_eq!(delivered[0].payload, vec![latency as u8]);
        }
    }

    #[test]
    fn drops_duplicates_and_reorders_are_deterministic() {
        let mut network = SimNetwork::new(0);
        network.drop_next(1, 2, 1);
        network.duplicate_next(1, 2, 1);
        network.send(1, 2, b"drop".to_vec()).unwrap();
        network.set_link_policy(
            1,
            2,
            LinkPolicy {
                latency_ticks: 0,
                reorder: true,
            },
        );
        network.send(1, 2, b"a".to_vec()).unwrap();
        network.send(1, 2, b"b".to_vec()).unwrap();
        let packets = network.deliver_ready();
        assert_eq!(packets.len(), 3);
        assert_eq!(packets[0].payload, b"b");
        assert_eq!(packets[1].payload, b"a");
        assert_eq!(packets[2].payload, b"a");
    }

    #[test]
    fn partitions_and_pauses_are_explicit_and_healable() {
        let mut network = SimNetwork::new(0);
        network.partition(1, 2);
        assert_eq!(network.send(1, 2, vec![]), Ok(0));
        assert!(network.deliver_ready().is_empty());
        network.heal_partition(1, 2);
        network.pause(2);
        assert_eq!(network.send(1, 2, vec![]), Ok(1));
        assert!(network.deliver_ready().is_empty());
        network.resume(2);
        assert_eq!(network.deliver_ready().len(), 1);
    }

    #[test]
    fn paused_destination_holds_in_flight_messages_until_resume() {
        let mut network = SimNetwork::new(5);
        network.send(1, 2, b"delayed".to_vec()).unwrap();
        network.pause(2);
        network.advance(5);
        assert!(network.deliver_ready().is_empty());
        network.resume(2);
        let delivered = network.deliver_ready();
        assert_eq!(delivered[0].payload, b"delayed");
        assert_eq!(delivered[0].delivered_at.ticks(), 5);
    }

    #[test]
    fn partition_drops_in_flight_packets_at_delivery_boundary() {
        let mut network = SimNetwork::new(5);
        network.send(1, 2, b"in-flight".to_vec()).unwrap();
        network.partition(1, 2);
        network.advance(5);
        assert!(network.deliver_ready().is_empty());
        assert!(network
            .trace()
            .iter()
            .any(|event| matches!(event, NetworkEvent::Dropped { sequence: 0, .. })));
    }

    #[test]
    fn directional_connection_interruptions_are_explicit_and_reconnectable() {
        let mut network = SimNetwork::new(0);
        network.disconnect(1, 2);
        assert_eq!(network.send(1, 2, vec![]), Ok(0));
        assert_eq!(network.send(2, 1, vec![]), Ok(1));
        network.reconnect(1, 2);
        assert_eq!(network.send(1, 2, vec![]), Ok(2));
    }

    #[test]
    fn node_crash_discards_in_flight_packets_and_restart_advances_epoch() {
        let mut network = SimNetwork::new(5);
        network.send(1, 2, b"lost-on-crash".to_vec()).unwrap();
        network.crash_node(2);
        network.advance(5);
        assert!(network.deliver_ready().is_empty());
        assert_eq!(network.node_epoch(2), 0);
        assert_eq!(
            network.send(1, 2, vec![]),
            Ok(1),
            "messages to a crashed node are accepted then dropped"
        );
        network.restart_node(2);
        assert_eq!(network.node_epoch(2), 1);
        assert!(network.send(1, 2, vec![]).is_ok());
    }
}
