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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivered {
    pub from: NodeId,
    pub to: NodeId,
    pub from_epoch: u64,
    pub to_epoch: u64,
    pub payload: Vec<u8>,
    pub sent_at: VirtualTime,
    pub delivered_at: VirtualTime,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkEvent {
    DefaultPolicySet {
        policy: LinkPolicy,
    },
    LinkPolicySet {
        from: NodeId,
        to: NodeId,
        policy: LinkPolicy,
    },
    DropScheduled {
        from: NodeId,
        to: NodeId,
        count: usize,
    },
    DuplicateScheduled {
        from: NodeId,
        to: NodeId,
        count: usize,
    },
    Advanced {
        ticks: u64,
        now: VirtualTime,
    },
    Partitioned(NodeId, NodeId),
    PartitionHealed(NodeId, NodeId),
    Disconnected(NodeId, NodeId),
    Reconnected(NodeId, NodeId),
    Sent {
        sequence: u64,
        from: NodeId,
        to: NodeId,
        from_epoch: u64,
        to_epoch: u64,
        payload: Vec<u8>,
        policy: LinkPolicy,
        deliver_at: VirtualTime,
    },
    Dropped {
        sequence: u64,
        from: NodeId,
        to: NodeId,
        from_epoch: u64,
        to_epoch: u64,
    },
    Duplicated {
        sequence: u64,
        copies: usize,
    },
    NodePaused(NodeId),
    NodeResumed(NodeId),
    NodeCrashed(NodeId),
    NodeRestarted {
        node: NodeId,
        epoch: u64,
    },
    Delivered {
        sequence: u64,
        from: NodeId,
        to: NodeId,
        from_epoch: u64,
        to_epoch: u64,
    },
}

#[derive(Clone, Debug)]
struct Packet {
    from: NodeId,
    to: NodeId,
    from_epoch: u64,
    to_epoch: u64,
    payload: Vec<u8>,
    sent_at: VirtualTime,
    deliver_at: VirtualTime,
    sequence: u64,
    copy_index: usize,
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
        self.trace.push(NetworkEvent::DefaultPolicySet { policy });
    }

    pub fn set_link_policy(&mut self, from: NodeId, to: NodeId, policy: LinkPolicy) {
        self.policies.insert((from, to), policy);
        self.trace
            .push(NetworkEvent::LinkPolicySet { from, to, policy });
    }

    pub fn drop_next(&mut self, from: NodeId, to: NodeId, count: usize) {
        self.faults.entry((from, to)).or_default().drop_next = count;
        self.trace
            .push(NetworkEvent::DropScheduled { from, to, count });
    }

    pub fn duplicate_next(&mut self, from: NodeId, to: NodeId, count: usize) {
        self.faults.entry((from, to)).or_default().duplicate_next = count;
        self.trace
            .push(NetworkEvent::DuplicateScheduled { from, to, count });
    }

    pub fn pause(&mut self, node: NodeId) {
        if !self.crashed.contains(&node) && self.paused.insert(node) {
            self.trace.push(NetworkEvent::NodePaused(node));
        }
    }

    pub fn resume(&mut self, node: NodeId) {
        if !self.crashed.contains(&node) && self.paused.remove(&node) {
            self.trace.push(NetworkEvent::NodeResumed(node));
        }
    }

    pub fn crash_node(&mut self, node: NodeId) {
        let newly_crashed = self.crashed.insert(node);
        self.paused.insert(node);
        self.packets
            .retain(|packet| packet.from != node && packet.to != node);
        if newly_crashed {
            self.trace.push(NetworkEvent::NodeCrashed(node));
        }
    }

    pub fn restart_node(&mut self, node: NodeId) {
        self.crashed.remove(&node);
        self.paused.remove(&node);
        self.packets
            .retain(|packet| packet.from != node && packet.to != node);
        let epoch = self.node_epochs.entry(node).or_default();
        *epoch = epoch.saturating_add(1);
        self.trace.push(NetworkEvent::NodeRestarted {
            node,
            epoch: *epoch,
        });
    }

    pub fn node_epoch(&self, node: NodeId) -> u64 {
        self.node_epochs.get(&node).copied().unwrap_or(0)
    }

    pub fn partition(&mut self, left: NodeId, right: NodeId) {
        self.partitions.insert((left, right));
        self.partitions.insert((right, left));
        self.trace.push(NetworkEvent::Partitioned(left, right));
    }

    pub fn heal_partition(&mut self, left: NodeId, right: NodeId) {
        self.partitions.remove(&(left, right));
        self.partitions.remove(&(right, left));
        self.trace.push(NetworkEvent::PartitionHealed(left, right));
    }

    pub fn disconnect(&mut self, from: NodeId, to: NodeId) {
        self.disconnected.insert((from, to));
        self.trace.push(NetworkEvent::Disconnected(from, to));
    }

    pub fn reconnect(&mut self, from: NodeId, to: NodeId) {
        self.disconnected.remove(&(from, to));
        self.trace.push(NetworkEvent::Reconnected(from, to));
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
        let from_epoch = self.node_epoch(from);
        let to_epoch = self.node_epoch(to);
        let policy = self
            .policies
            .get(&(from, to))
            .copied()
            .unwrap_or(self.default_policy);
        let deliver_at = VirtualTime(self.now.0.saturating_add(policy.latency_ticks));
        self.trace.push(NetworkEvent::Sent {
            sequence,
            from,
            to,
            from_epoch,
            to_epoch,
            payload: payload.clone(),
            policy,
            deliver_at,
        });
        if self.crashed.contains(&to)
            || self.disconnected.contains(&(from, to))
            || self.partitions.contains(&(from, to))
        {
            self.trace.push(NetworkEvent::Dropped {
                sequence,
                from,
                to,
                from_epoch,
                to_epoch,
            });
            return Ok(sequence);
        }
        let faults = self.faults.entry((from, to)).or_default();
        if faults.drop_next > 0 {
            faults.drop_next -= 1;
            self.trace.push(NetworkEvent::Dropped {
                sequence,
                from,
                to,
                from_epoch,
                to_epoch,
            });
            return Ok(sequence);
        }
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
        for copy_index in 0..copies {
            self.packets.push(Packet {
                from,
                to,
                from_epoch,
                to_epoch,
                payload: payload.clone(),
                sent_at: self.now,
                deliver_at,
                sequence,
                copy_index,
                reorder: policy.reorder,
            });
        }
        Ok(sequence)
    }

    pub fn advance(&mut self, ticks: u64) {
        self.now = VirtualTime(self.now.0.saturating_add(ticks));
        self.trace.push(NetworkEvent::Advanced {
            ticks,
            now: self.now,
        });
    }

    pub fn deliver_ready(&mut self) -> Vec<Delivered> {
        let mut ready = Vec::new();
        let mut pending = Vec::with_capacity(self.packets.len());
        let node_epochs = self.node_epochs.clone();
        for packet in self.packets.drain(..) {
            if packet.deliver_at <= self.now {
                if self.crashed.contains(&packet.to)
                    || self.disconnected.contains(&(packet.from, packet.to))
                    || self.partitions.contains(&(packet.from, packet.to))
                    || node_epochs.get(&packet.from).copied().unwrap_or(0) != packet.from_epoch
                    || node_epochs.get(&packet.to).copied().unwrap_or(0) != packet.to_epoch
                {
                    self.trace.push(NetworkEvent::Dropped {
                        sequence: packet.sequence,
                        from: packet.from,
                        to: packet.to,
                        from_epoch: packet.from_epoch,
                        to_epoch: packet.to_epoch,
                    });
                    continue;
                }
                if self.paused.contains(&packet.to) {
                    pending.push(packet);
                } else {
                    ready.push(packet);
                }
            } else {
                pending.push(packet);
            }
        }
        self.packets = pending;
        ready.sort_by_key(|packet| {
            // Keep the link lane ahead of its local ordering policy. This makes
            // the ordering total and prevents one link's reorder setting from
            // moving packets from every other link.
            (
                packet.deliver_at,
                packet.from,
                packet.to,
                if packet.reorder {
                    u64::MAX - packet.sequence
                } else {
                    packet.sequence
                },
                packet.sequence,
                packet.copy_index,
            )
        });
        ready
            .into_iter()
            .map(|packet| {
                self.trace.push(NetworkEvent::Delivered {
                    sequence: packet.sequence,
                    from: packet.from,
                    to: packet.to,
                    from_epoch: packet.from_epoch,
                    to_epoch: packet.to_epoch,
                });
                Delivered {
                    from: packet.from,
                    to: packet.to,
                    from_epoch: packet.from_epoch,
                    to_epoch: packet.to_epoch,
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
    fn trace_records_constructor_default_latency_without_policy_override() {
        let mut fast = SimNetwork::new(1);
        fast.send(1, 2, b"same-send".to_vec()).unwrap();

        let mut slow = SimNetwork::new(9);
        slow.send(1, 2, b"same-send".to_vec()).unwrap();

        let fast_sent = fast
            .trace()
            .iter()
            .find(|event| matches!(event, NetworkEvent::Sent { .. }))
            .expect("send must be present in the trace");
        let slow_sent = slow
            .trace()
            .iter()
            .find(|event| matches!(event, NetworkEvent::Sent { .. }))
            .expect("send must be present in the trace");

        assert_ne!(fast.trace(), slow.trace());
        assert!(matches!(
            fast_sent,
            NetworkEvent::Sent {
                policy: LinkPolicy {
                    latency_ticks: 1,
                    reorder: false,
                },
                deliver_at,
                ..
            } if deliver_at.ticks() == 1
        ));
        assert!(matches!(
            slow_sent,
            NetworkEvent::Sent {
                policy: LinkPolicy {
                    latency_ticks: 9,
                    reorder: false,
                },
                deliver_at,
                ..
            } if deliver_at.ticks() == 9
        ));
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
    fn reordering_is_total_and_stable_across_links() {
        let mut network = SimNetwork::new(0);
        network.set_link_policy(
            1,
            2,
            LinkPolicy {
                latency_ticks: 0,
                reorder: true,
            },
        );
        network.set_link_policy(
            3,
            4,
            LinkPolicy {
                latency_ticks: 0,
                reorder: false,
            },
        );
        network.duplicate_next(1, 2, 1);
        network.send(1, 2, b"a".to_vec()).unwrap();
        network.send(3, 4, b"x".to_vec()).unwrap();
        network.send(1, 2, b"b".to_vec()).unwrap();
        network.send(3, 4, b"y".to_vec()).unwrap();

        let packets = network.deliver_ready();
        assert_eq!(
            packets
                .iter()
                .map(|packet| packet.sequence)
                .collect::<Vec<_>>(),
            vec![2, 0, 0, 1, 3]
        );
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
        network.advance(7);
        assert!(network.deliver_ready().is_empty());
        network.resume(2);
        let delivered = network.deliver_ready();
        assert_eq!(delivered[0].payload, b"delayed");
        assert_eq!(delivered[0].delivered_at.ticks(), 7);
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
    fn paused_packet_is_dropped_after_partition_before_delivery() {
        let mut network = SimNetwork::new(5);
        network.send(1, 2, b"stale-partition".to_vec()).unwrap();
        network.pause(2);
        network.partition(1, 2);
        network.advance(5);

        assert!(network.deliver_ready().is_empty());
        network.heal_partition(1, 2);
        network.resume(2);
        assert!(network.deliver_ready().is_empty());
        assert!(network.trace().iter().any(|event| matches!(
            event,
            NetworkEvent::Dropped {
                sequence: 0,
                from: 1,
                to: 2,
                ..
            }
        )));
    }

    #[test]
    fn paused_packet_is_dropped_after_disconnect_before_delivery() {
        let mut network = SimNetwork::new(5);
        network.send(1, 2, b"stale-disconnect".to_vec()).unwrap();
        network.pause(2);
        network.disconnect(1, 2);
        network.advance(5);

        assert!(network.deliver_ready().is_empty());
        network.reconnect(1, 2);
        network.resume(2);
        assert!(network.deliver_ready().is_empty());
        assert!(network.trace().iter().any(|event| matches!(
            event,
            NetworkEvent::Dropped {
                sequence: 0,
                from: 1,
                to: 2,
                ..
            }
        )));
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
    fn disconnect_drops_in_flight_packets_at_delivery_boundary() {
        let mut network = SimNetwork::new(5);
        network.send(1, 2, b"in-flight".to_vec()).unwrap();
        network.disconnect(1, 2);
        network.advance(5);
        assert!(network.deliver_ready().is_empty());
        assert!(network
            .trace()
            .iter()
            .any(|event| matches!(event, NetworkEvent::Dropped { sequence: 0, .. })));

        network.reconnect(1, 2);
        network.send(1, 2, b"after-reconnect".to_vec()).unwrap();
        network.advance(5);
        let delivered = network.deliver_ready();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].payload, b"after-reconnect");
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

    #[test]
    fn restart_clears_in_flight_packets_and_resume_cannot_revive_a_crash() {
        let mut network = SimNetwork::new(5);
        network.send(1, 2, b"stale".to_vec()).unwrap();
        network.pause(2);
        network.restart_node(2);
        assert_eq!(network.node_epoch(2), 1);
        network.advance(5);
        assert!(network.deliver_ready().is_empty());

        network.crash_node(2);
        network.resume(2);
        assert_eq!(network.send(1, 2, vec![]), Ok(1));
        assert!(!network
            .trace()
            .iter()
            .any(|event| matches!(event, NetworkEvent::NodeResumed(2))));
        network.restart_node(2);
        assert_eq!(network.node_epoch(2), 2);
    }

    #[test]
    fn old_incarnation_is_never_delivered_after_restart() {
        let mut network = SimNetwork::new(5);
        network.pause(2);
        network.send(1, 2, b"old-incarnation".to_vec()).unwrap();
        network.restart_node(2);
        assert_eq!(network.node_epoch(2), 1);
        network.advance(5);
        assert!(network.deliver_ready().is_empty());

        let delivered = network.send(1, 2, b"new-incarnation".to_vec()).unwrap();
        network.advance(5);
        let packets = network.deliver_ready();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].sequence, delivered);
        assert_eq!(packets[0].to_epoch, 1);
        assert_eq!(packets[0].payload, b"new-incarnation");
        assert!(network
            .trace()
            .iter()
            .any(|event| matches!(event, NetworkEvent::NodeRestarted { node: 2, epoch: 1 })));
    }

    #[test]
    fn trace_contains_replay_inputs_and_packet_payload() {
        let mut network = SimNetwork::new(1);
        let policy = LinkPolicy {
            latency_ticks: 3,
            reorder: true,
        };
        network.set_default_policy(policy);
        network.set_link_policy(1, 2, policy);
        network.drop_next(1, 2, 1);
        network.duplicate_next(1, 2, 2);
        network.partition(1, 2);
        network.heal_partition(1, 2);
        network.disconnect(1, 2);
        network.reconnect(1, 2);
        network.send(1, 2, b"replay-me".to_vec()).unwrap();
        network.advance(3);

        assert!(network
            .trace()
            .iter()
            .any(|event| matches!(event, NetworkEvent::DefaultPolicySet { policy: value } if *value == policy)));
        assert!(network.trace().iter().any(|event| matches!(
            event,
            NetworkEvent::LinkPolicySet { from: 1, to: 2, policy: value } if *value == policy
        )));
        assert!(network.trace().iter().any(|event| matches!(
            event,
            NetworkEvent::DropScheduled {
                from: 1,
                to: 2,
                count: 1
            }
        )));
        assert!(network.trace().iter().any(|event| matches!(
            event,
            NetworkEvent::DuplicateScheduled {
                from: 1,
                to: 2,
                count: 2
            }
        )));
        assert!(network
            .trace()
            .iter()
            .any(|event| matches!(event, NetworkEvent::Partitioned(1, 2))));
        assert!(network
            .trace()
            .iter()
            .any(|event| matches!(event, NetworkEvent::PartitionHealed(1, 2))));
        assert!(network
            .trace()
            .iter()
            .any(|event| matches!(event, NetworkEvent::Disconnected(1, 2))));
        assert!(network
            .trace()
            .iter()
            .any(|event| matches!(event, NetworkEvent::Reconnected(1, 2))));
        assert!(network.trace().iter().any(|event| matches!(
            event,
            NetworkEvent::Sent { from: 1, to: 2, payload, .. } if payload == b"replay-me"
        )));
        assert!(network.trace().iter().any(|event| matches!(
            event,
            NetworkEvent::Advanced { ticks: 3, now } if now.ticks() == 3
        )));
    }
}
