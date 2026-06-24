from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple, Set, Iterable
import heapq
import math
import random


@dataclass(frozen=True)
class SimulationConfig:
    nodes_count: int = 200
    packet_loss_probability: float = 0.0
    node_failure_probability: float = 0.0
    fanout: int = 3
    gossip_interval_ms: float = 5.0
    timeout_ms: float = 1000.0
    min_delay_ms: float = 0.6
    max_delay_ms: float = 2.0
    serialization_ms: float = 0.01
    multicast_group_size: int = 20
    seed: int = 42


@dataclass
class SimulationResult:
    algorithm: str
    nodes_count: int
    active_nodes: int
    failed_nodes: int
    packet_loss_probability: float
    node_failure_probability: float
    duration_ms: float
    coverage_active_pct: float
    coverage_total_pct: float
    messages_sent: int
    packets_lost: int
    failed_deliveries: int
    duplicates: int
    rounds: int
    p95_delivery_ms: float
    curve: List[Tuple[float, float]]

    def row(self) -> Dict[str, object]:
        d = asdict(self)
        d.pop("curve", None)
        return d


class DistSpreadSimulator:
    """Discrete-event / round-based simulator for information dissemination.

    The initiator (node 0) is always kept alive. Node failures are static for one
    run. Each packet is independently discarded with the configured loss
    probability. All algorithms use the same failed-node set and random source
    for a given run seed, which keeps comparisons reproducible.
    """

    ALGORITHMS = (
        "Single Cast",
        "Hierarchical Multicast",
        "Broadcast Flooding",
        "Gossip Push",
        "Adaptive Gossip Push-Pull",
        "Hybrid Multicast-Gossip",
    )

    def __init__(self, config: SimulationConfig):
        if config.nodes_count < 2:
            raise ValueError("nodes_count must be at least 2")
        for p in (config.packet_loss_probability, config.node_failure_probability):
            if not 0.0 <= p <= 1.0:
                raise ValueError("probabilities must be between 0 and 1")
        self.cfg = config
        self.rng = random.Random(config.seed)
        candidates = list(range(1, config.nodes_count))
        fail_count = min(len(candidates), round(config.node_failure_probability * config.nodes_count))
        self.failed: Set[int] = set(self.rng.sample(candidates, fail_count))
        self.active: Set[int] = set(range(config.nodes_count)) - self.failed

    def _delay(self) -> float:
        return self.rng.uniform(self.cfg.min_delay_ms, self.cfg.max_delay_ms)

    def _packet_outcome(self, receiver: int) -> str:
        if receiver in self.failed:
            return "failed"
        if self.rng.random() < self.cfg.packet_loss_probability:
            return "lost"
        return "ok"

    @staticmethod
    def _p95(values: Iterable[float]) -> float:
        vals = sorted(values)
        if not vals:
            return 0.0
        idx = max(0, math.ceil(0.95 * len(vals)) - 1)
        return float(vals[idx])

    def _result(
        self,
        algorithm: str,
        informed: Set[int],
        receive_times: Dict[int, float],
        duration_ms: float,
        messages_sent: int,
        packets_lost: int,
        failed_deliveries: int,
        duplicates: int,
        rounds: int,
        curve: List[Tuple[float, float]],
    ) -> SimulationResult:
        active_informed = len(informed & self.active)
        active_count = max(1, len(self.active))
        total_count = self.cfg.nodes_count
        if not curve:
            curve = [(0.0, 100.0 * active_informed / active_count)]
        return SimulationResult(
            algorithm=algorithm,
            nodes_count=self.cfg.nodes_count,
            active_nodes=active_count,
            failed_nodes=len(self.failed),
            packet_loss_probability=self.cfg.packet_loss_probability,
            node_failure_probability=self.cfg.node_failure_probability,
            duration_ms=round(float(duration_ms), 6),
            coverage_active_pct=100.0 * active_informed / active_count,
            coverage_total_pct=100.0 * len(informed) / total_count,
            messages_sent=messages_sent,
            packets_lost=packets_lost,
            failed_deliveries=failed_deliveries,
            duplicates=duplicates,
            rounds=rounds,
            p95_delivery_ms=self._p95(receive_times.values()),
            curve=curve,
        )

    def run(self, algorithm: str) -> SimulationResult:
        dispatch = {
            "Single Cast": self.run_single_cast,
            "Hierarchical Multicast": self.run_multicast,
            "Broadcast Flooding": self.run_broadcast,
            "Gossip Push": self.run_gossip_push,
            "Adaptive Gossip Push-Pull": self.run_adaptive_push_pull,
            "Hybrid Multicast-Gossip": self.run_hybrid,
        }
        try:
            return dispatch[algorithm]()
        except KeyError as exc:
            raise ValueError(f"unknown algorithm: {algorithm}") from exc

    def run_single_cast(self) -> SimulationResult:
        informed = {0}
        receive_times = {0: 0.0}
        arrivals: List[Tuple[float, int]] = []
        sent = lost = failed_deliveries = 0
        targets = list(range(1, self.cfg.nodes_count))
        for idx, target in enumerate(targets):
            send_time = idx * self.cfg.serialization_ms
            sent += 1
            outcome = self._packet_outcome(target)
            if outcome == "failed":
                failed_deliveries += 1
            elif outcome == "lost":
                lost += 1
            else:
                arrivals.append((send_time + self._delay(), target))
        arrivals.sort()
        curve = [(0.0, 100.0 / len(self.active))]
        for t, target in arrivals:
            informed.add(target)
            receive_times[target] = t
            curve.append((t, 100.0 * len(informed & self.active) / len(self.active)))
        duration = max([len(targets) * self.cfg.serialization_ms] + [t for t, _ in arrivals])
        return self._result(
            "Single Cast", informed, receive_times, duration, sent, lost,
            failed_deliveries, 0, 1, curve
        )

    def _multicast_initial_phase(self) -> Tuple[Set[int], Dict[int, float], int, int, int, int, List[Tuple[float, float]]]:
        informed: Set[int] = {0}
        receive_times: Dict[int, float] = {0: 0.0}
        sent = lost = failed_deliveries = duplicates = 0
        events: List[Tuple[float, int, int, str]] = []
        group_size = self.cfg.multicast_group_size
        groups = [list(range(start, min(start + group_size, self.cfg.nodes_count)))
                  for start in range(1, self.cfg.nodes_count, group_size)]
        leader_to_group: Dict[int, List[int]] = {group[0]: group for group in groups if group}
        for idx, leader in enumerate(leader_to_group):
            sent += 1
            outcome = self._packet_outcome(leader)
            if outcome == "failed":
                failed_deliveries += 1
            elif outcome == "lost":
                lost += 1
            else:
                heapq.heappush(events, (idx * self.cfg.serialization_ms + self._delay(), 0, leader, "leader"))
        curve = [(0.0, 100.0 / len(self.active))]
        while events:
            t, sender, receiver, kind = heapq.heappop(events)
            if t > self.cfg.timeout_ms:
                break
            if receiver in informed:
                duplicates += 1
                continue
            informed.add(receiver)
            receive_times[receiver] = t
            curve.append((t, 100.0 * len(informed & self.active) / len(self.active)))
            if kind == "leader":
                group = leader_to_group[receiver]
                for j, target in enumerate(group):
                    if target == receiver:
                        continue
                    sent += 1
                    outcome = self._packet_outcome(target)
                    if outcome == "failed":
                        failed_deliveries += 1
                    elif outcome == "lost":
                        lost += 1
                    else:
                        heapq.heappush(events, (t + j * self.cfg.serialization_ms + self._delay(), receiver, target, "member"))
        return informed, receive_times, sent, lost, failed_deliveries, duplicates, curve

    def run_multicast(self) -> SimulationResult:
        informed, receive_times, sent, lost, failed_deliveries, duplicates, curve = self._multicast_initial_phase()
        duration = max(receive_times.values()) if len(receive_times) > 1 else 0.0
        return self._result(
            "Hierarchical Multicast", informed, receive_times, duration, sent, lost,
            failed_deliveries, duplicates, 2, curve
        )

    def run_broadcast(self) -> SimulationResult:
        informed: Set[int] = {0}
        forwarded: Set[int] = set()
        receive_times = {0: 0.0}
        events: List[Tuple[float, int, int]] = []
        sent = lost = failed_deliveries = duplicates = 0
        curve = [(0.0, 100.0 / len(self.active))]

        def forward(node: int, time_ms: float, previous_sender: int | None) -> None:
            nonlocal sent, lost, failed_deliveries
            if node in forwarded or node in self.failed:
                return
            forwarded.add(node)
            order = list(range(self.cfg.nodes_count))
            self.rng.shuffle(order)
            serial_idx = 0
            for target in order:
                if target == node or target == previous_sender:
                    continue
                sent += 1
                outcome = self._packet_outcome(target)
                if outcome == "failed":
                    failed_deliveries += 1
                elif outcome == "lost":
                    lost += 1
                else:
                    arrival = time_ms + serial_idx * self.cfg.serialization_ms + self._delay()
                    heapq.heappush(events, (arrival, node, target))
                serial_idx += 1

        forward(0, 0.0, None)
        while events:
            t, sender, receiver = heapq.heappop(events)
            if t > self.cfg.timeout_ms:
                break
            if receiver in informed:
                duplicates += 1
                continue
            informed.add(receiver)
            receive_times[receiver] = t
            curve.append((t, 100.0 * len(informed & self.active) / len(self.active)))
            forward(receiver, t, sender)
            if len(informed & self.active) == len(self.active):
                # Remaining queued packets are duplicates; count them deterministically.
                duplicates += len(events)
                events.clear()
                break
        duration = max(receive_times.values()) if len(receive_times) > 1 else 0.0
        return self._result(
            "Broadcast Flooding", informed, receive_times, duration, sent, lost,
            failed_deliveries, duplicates, len(forwarded), curve
        )

    def run_gossip_push(self) -> SimulationResult:
        informed: Set[int] = {0}
        receive_times: Dict[int, float] = {0: 0.0}
        sent = lost = failed_deliveries = duplicates = 0
        curve = [(0.0, 100.0 / len(self.active))]
        no_growth = 0
        rounds = 0
        max_rounds = max(1, int(self.cfg.timeout_ms // self.cfg.gossip_interval_ms))
        all_nodes = list(range(self.cfg.nodes_count))

        for r in range(1, max_rounds + 1):
            rounds = r
            new_nodes: Set[int] = set()
            senders = list(informed & self.active)
            self.rng.shuffle(senders)
            for sender in senders:
                candidates = [x for x in all_nodes if x != sender]
                targets = self.rng.sample(candidates, min(self.cfg.fanout, len(candidates)))
                for target in targets:
                    sent += 1
                    outcome = self._packet_outcome(target)
                    if outcome == "failed":
                        failed_deliveries += 1
                    elif outcome == "lost":
                        lost += 1
                    elif target in informed or target in new_nodes:
                        duplicates += 1
                    else:
                        new_nodes.add(target)
            t = r * self.cfg.gossip_interval_ms
            for node in new_nodes:
                informed.add(node)
                receive_times[node] = t + self._delay()
            curve.append((t, 100.0 * len(informed & self.active) / len(self.active)))
            if new_nodes:
                no_growth = 0
            else:
                no_growth += 1
            if len(informed & self.active) == len(self.active):
                break
            if no_growth >= 25:
                break
        duration = rounds * self.cfg.gossip_interval_ms
        return self._result(
            "Gossip Push", informed, receive_times, duration, sent, lost,
            failed_deliveries, duplicates, rounds, curve
        )

    def run_adaptive_push_pull(self) -> SimulationResult:
        informed: Set[int] = {0}
        receive_times: Dict[int, float] = {0: 0.0}
        sent = lost = failed_deliveries = duplicates = 0
        curve = [(0.0, 100.0 / len(self.active))]
        active_nodes = list(self.active)
        max_rounds = max(1, int(self.cfg.timeout_ms // self.cfg.gossip_interval_ms))
        recent_growth: List[int] = []
        no_growth = 0
        rounds = 0

        for r in range(1, max_rounds + 1):
            rounds = r
            growth_ratio = (sum(recent_growth[-3:]) / max(1, len(self.active))) if recent_growth else 1.0
            if growth_ratio < 0.01:
                adaptive_fanout = min(5, max(2, self.cfg.fanout + 2))
            elif growth_ratio < 0.05:
                adaptive_fanout = min(4, max(2, self.cfg.fanout))
            else:
                adaptive_fanout = 2

            newly_informed: Set[int] = set()
            initiators = active_nodes[:]
            self.rng.shuffle(initiators)
            for u in initiators:
                peers = [v for v in active_nodes if v != u]
                if not peers:
                    continue
                targets = self.rng.sample(peers, min(adaptive_fanout, len(peers)))
                for v in targets:
                    # Bidirectional status exchange: two control packets.
                    status_ok = True
                    for receiver in (v, u):
                        sent += 1
                        outcome = self._packet_outcome(receiver)
                        if outcome == "failed":
                            failed_deliveries += 1
                            status_ok = False
                        elif outcome == "lost":
                            lost += 1
                            status_ok = False
                    if not status_ok:
                        continue
                    u_has = u in informed or u in newly_informed
                    v_has = v in informed or v in newly_informed
                    if u_has == v_has:
                        continue
                    receiver = v if u_has else u
                    sent += 1
                    outcome = self._packet_outcome(receiver)
                    if outcome == "failed":
                        failed_deliveries += 1
                    elif outcome == "lost":
                        lost += 1
                    elif receiver in informed or receiver in newly_informed:
                        duplicates += 1
                    else:
                        newly_informed.add(receiver)
            t = r * self.cfg.gossip_interval_ms
            for node in newly_informed:
                informed.add(node)
                receive_times[node] = t + self._delay()
            recent_growth.append(len(newly_informed))
            curve.append((t, 100.0 * len(informed & self.active) / len(self.active)))
            if newly_informed:
                no_growth = 0
            else:
                no_growth += 1
            if len(informed & self.active) == len(self.active):
                break
            if no_growth >= 20:
                break
        duration = rounds * self.cfg.gossip_interval_ms
        return self._result(
            "Adaptive Gossip Push-Pull", informed, receive_times, duration, sent, lost,
            failed_deliveries, duplicates, rounds, curve
        )

    def run_hybrid(self) -> SimulationResult:
        informed, receive_times, sent, lost, failed_deliveries, duplicates, curve = self._multicast_initial_phase()
        start_time = max(receive_times.values()) if receive_times else 0.0
        all_nodes = list(range(self.cfg.nodes_count))
        no_growth = 0
        rounds = 0
        max_rounds = max(1, int((self.cfg.timeout_ms - start_time) // self.cfg.gossip_interval_ms))

        for r in range(1, max_rounds + 1):
            rounds = r
            new_nodes: Set[int] = set()
            senders = list(informed & self.active)
            self.rng.shuffle(senders)
            # Small recovery fanout keeps the hybrid cheaper than pure Push.
            recovery_fanout = max(2, self.cfg.fanout - 1)
            for sender in senders:
                candidates = [x for x in all_nodes if x != sender]
                targets = self.rng.sample(candidates, min(recovery_fanout, len(candidates)))
                for target in targets:
                    sent += 1
                    outcome = self._packet_outcome(target)
                    if outcome == "failed":
                        failed_deliveries += 1
                    elif outcome == "lost":
                        lost += 1
                    elif target in informed or target in new_nodes:
                        duplicates += 1
                    else:
                        new_nodes.add(target)
            t = start_time + r * self.cfg.gossip_interval_ms
            for node in new_nodes:
                informed.add(node)
                receive_times[node] = t + self._delay()
            curve.append((t, 100.0 * len(informed & self.active) / len(self.active)))
            if new_nodes:
                no_growth = 0
            else:
                no_growth += 1
            if len(informed & self.active) == len(self.active):
                break
            if no_growth >= 20:
                break
        duration = start_time + rounds * self.cfg.gossip_interval_ms
        return self._result(
            "Hybrid Multicast-Gossip", informed, receive_times, duration, sent, lost,
            failed_deliveries, duplicates, rounds + 2, curve
        )
