#!/usr/bin/env python3
"""Explicit planning assumptions for a three-AZ poll service, not a benchmark.

All rates use decimal MB/s and Gb/s. Network capacity is per direction by
default. No resource is created, and no price or measured capacity is inferred.
"""

import argparse
from dataclasses import dataclass
from fractions import Fraction
import json
import sys


AZS = 3
REPLICATION = 3


def number(value):
    """Use exact rational arithmetic so a boundary never gains three nodes."""
    return value if isinstance(value, Fraction) else Fraction(str(value))


def ceil_fraction(value):
    value = number(value)
    return -(-value.numerator // value.denominator)


def triplets_for_ratio(ratio):
    return max(AZS, AZS * ceil_fraction(number(ratio) / AZS))


@dataclass(frozen=True)
class Workload:
    unique_voters: int = 100_000_000
    seconds: Fraction = Fraction(60)
    repeat_fraction: Fraction = Fraction(1, 5)
    utilization: Fraction = Fraction(7, 10)

    def validate(self):
        if self.unique_voters < 1 or number(self.seconds) <= 0:
            raise ValueError("voters and duration must be positive")
        if number(self.repeat_fraction) < 0:
            raise ValueError("repeat fraction must be nonnegative")
        if not 0 < number(self.utilization) <= 1:
            raise ValueError("target utilization must be in (0,1]")

    @property
    def attempts(self):
        return self.unique_voters * (1 + number(self.repeat_fraction))

    @property
    def rate(self):
        return self.attempts / number(self.seconds)


@dataclass(frozen=True)
class RecordLayout:
    name: str
    payload_bytes: Fraction
    frame_header_bytes: Fraction
    average_attempts_per_frame: Fraction
    kafka_overhead_bytes: Fraction

    def validate(self):
        if not self.name or number(self.payload_bytes) <= 0:
            raise ValueError("record scenario needs a name and positive payload")
        if number(self.frame_header_bytes) < 0 or number(self.kafka_overhead_bytes) < 0:
            raise ValueError("record overheads must be nonnegative")
        if number(self.average_attempts_per_frame) < 1:
            raise ValueError("average frame occupancy must be at least one")

    @property
    def amortized_header(self):
        return number(self.frame_header_bytes) / number(self.average_attempts_per_frame)

    @property
    def effective_bytes(self):
        return number(self.payload_bytes) + self.amortized_header + number(self.kafka_overhead_bytes)


@dataclass(frozen=True)
class BrokerLimits:
    leader_mb_s: Fraction = Fraction(200)
    disk_mb_s: Fraction = Fraction(600)
    network_gbps: Fraction = Fraction(10)
    network_mode: str = "per-direction"
    transport_multiplier: Fraction = Fraction(115, 100)
    disk_write_amplification: Fraction = Fraction(1)
    operational_floor: int = 6
    nvme_tb_per_node: Fraction = Fraction(192, 100)
    retention_events: int = 1
    storage_overhead_multiplier: Fraction = Fraction(3, 2)
    maximum_disk_fill: Fraction = Fraction(3, 5)

    def validate(self):
        for value in (self.leader_mb_s, self.disk_mb_s, self.network_gbps, self.nvme_tb_per_node):
            if number(value) <= 0:
                raise ValueError("broker throughput and device capacities must be positive")
        for value in (self.transport_multiplier, self.disk_write_amplification, self.storage_overhead_multiplier):
            if number(value) < 1:
                raise ValueError("overhead/amplification multipliers must be >=1")
        if self.network_mode not in ("per-direction", "aggregate"):
            raise ValueError("network mode must be per-direction or aggregate")
        if self.operational_floor < AZS or self.operational_floor % AZS:
            raise ValueError("operational floor must be a positive multiple of three, at least three")
        if self.retention_events < 1 or not 0 < number(self.maximum_disk_fill) <= 1:
            raise ValueError("retention events must be positive and disk fill must be in (0,1]")


def ingress_sizing(workload, per_node_attempts_s):
    workload.validate()
    throughput = number(per_node_attempts_s)
    if throughput <= 0:
        raise ValueError("ingress throughput must be positive")
    # Two equally sized AZs must handle the whole stream within the utilization target.
    per_az = ceil_fraction(workload.rate / (2 * number(workload.utilization) * throughput))
    nodes = AZS * per_az
    return {
        "per_node_attempts_s": throughput,
        "nodes": nodes,
        "nodes_per_az": per_az,
        "remaining_after_one_az_loss": 2 * per_az,
        "normal_utilization": workload.rate / (nodes * throughput),
        "one_az_loss_utilization": workload.rate / (2 * per_az * throughput),
        "formula": "3 * ceil(attempt_rate / (2 * utilization_target * per_node_attempt_throughput))",
    }


def broker_state(workload, layout, limits, nodes, degraded):
    """Balanced mean load per surviving broker; rebuild/replay are excluded."""
    if nodes < AZS or nodes % AZS:
        raise ValueError("broker count must be a multiple of three")
    surviving_nodes = nodes * 2 // 3 if degraded else nodes
    surviving_copies = 2 if degraded else REPLICATION
    logical_mb_s = workload.rate * layout.effective_bytes / 1_000_000
    wire_mb_s = logical_mb_s * number(limits.transport_multiplier)
    leader = logical_mb_s / surviving_nodes
    disk = surviving_copies * logical_mb_s * number(limits.disk_write_amplification) / surviving_nodes
    # Every logical byte enters one leader and RF-1 follower copies. Only the
    # leader-to-follower transfers leave brokers in this bulk-ingestion model.
    network_in = surviving_copies * wire_mb_s / surviving_nodes
    network_out = (surviving_copies - 1) * wire_mb_s / surviving_nodes
    network_load = max(network_in, network_out) if limits.network_mode == "per-direction" else network_in + network_out
    network_cap_mb_s = number(limits.network_gbps) * 1_000 / 8
    return {
        "surviving_nodes": surviving_nodes,
        "copies_per_partition": surviving_copies,
        "leader_input_mb_s_per_node": leader,
        "replica_disk_write_mb_s_per_node": disk,
        "network_in_mb_s_per_node": network_in,
        "network_out_mb_s_per_node": network_out,
        "network_capacity_mode": limits.network_mode,
        "network_capacity_mb_s_per_direction_or_aggregate": network_cap_mb_s,
        "utilization": {
            "leader": leader / number(limits.leader_mb_s),
            "disk": disk / number(limits.disk_mb_s),
            "network": network_load / network_cap_mb_s,
        },
    }


def broker_sizing(workload, layout, limits):
    workload.validate()
    layout.validate()
    limits.validate()
    # Loads are inversely proportional to node count in each balanced state.
    at_three = [broker_state(workload, layout, limits, 3, degraded) for degraded in (False, True)]
    minima = {}
    for dimension in ("leader", "disk", "network"):
        worst_ratio = max(state["utilization"][dimension] for state in at_three)
        minima[dimension] = triplets_for_ratio(3 * worst_ratio / number(workload.utilization))
    throughput_min = max(minima.values())
    logical_event_bytes = workload.attempts * layout.effective_bytes
    stored_bytes = logical_event_bytes * REPLICATION * limits.retention_events * number(limits.storage_overhead_multiplier)
    device_bytes = number(limits.nvme_tb_per_node) * 1_000_000_000_000
    storage_min = triplets_for_ratio(stored_bytes / (device_bytes * number(limits.maximum_disk_fill)))
    capacity_min = max(throughput_min, storage_min)
    candidate = max(capacity_min, limits.operational_floor)
    normal = broker_state(workload, layout, limits, candidate, False)
    degraded = broker_state(workload, layout, limits, candidate, True)
    return {
        "record_scenario": layout.name,
        "payload_bytes_per_attempt": number(layout.payload_bytes),
        "frame_header_bytes": number(layout.frame_header_bytes),
        "average_attempts_per_frame_assumed": number(layout.average_attempts_per_frame),
        "amortized_frame_header_bytes_per_attempt": layout.amortized_header,
        "kafka_record_batch_transaction_overhead_bytes_per_attempt_assumed": number(layout.kafka_overhead_bytes),
        "effective_logical_bytes_per_attempt": layout.effective_bytes,
        "logical_input_mb_s": workload.rate * layout.effective_bytes / 1_000_000,
        "throughput_minimum_by_constraint": minima,
        "throughput_minimum_brokers": throughput_min,
        "storage_minimum_brokers": storage_min,
        "capacity_minimum_brokers": capacity_min,
        "operational_floor_brokers": limits.operational_floor,
        "candidate_brokers": candidate,
        "normal": normal,
        "one_az_lost": degraded,
        "within_target_in_both_states": all(value <= number(workload.utilization) for state in (normal, degraded) for value in state["utilization"].values()),
        "retention": {
            "events": limits.retention_events,
            "logical_bytes_per_event": logical_event_bytes,
            "replication_factor": REPLICATION,
            "storage_overhead_multiplier": number(limits.storage_overhead_multiplier),
            "physical_stored_bytes_with_overhead": stored_bytes,
            "nvme_tb_per_node_decimal": number(limits.nvme_tb_per_node),
            "raw_capacity_bytes": candidate * device_bytes,
            "maximum_disk_fill": number(limits.maximum_disk_fill),
            "projected_disk_fill": stored_bytes / (candidate * device_bytes),
            "whole_events_at_fill_limit": (candidate * device_bytes * number(limits.maximum_disk_fill) / (logical_event_bytes * REPLICATION * number(limits.storage_overhead_multiplier))).__floor__(),
        },
    }


def json_safe(value):
    if isinstance(value, Fraction):
        return value.numerator if value.denominator == 1 else float(value)
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def positive_fraction(raw):
    try:
        return number(raw)
    except (ValueError, ZeroDivisionError):
        raise argparse.ArgumentTypeError("expected a finite decimal or rational number") from None


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--unique-voters", type=int, default=100_000_000)
    p.add_argument("--seconds", type=positive_fraction, default=Fraction(60))
    p.add_argument("--repeat-fraction", type=positive_fraction, default=Fraction(1, 5), help="additional attempts per unique voter; 0.2 means 20%% repeats")
    p.add_argument("--utilization", type=positive_fraction, default=Fraction(7, 10))
    p.add_argument("--ingress-rps", type=positive_fraction, action="append", help="repeat to set sensitivity values; defaults 100000,200000,250000,300000")
    p.add_argument("--candidate-ingress-rps", type=positive_fraction, default=Fraction(250_000))
    p.add_argument("--ingress-basis", choices=("assumed", "measured"), default="assumed")
    p.add_argument("--ingress-source", default="")
    p.add_argument("--broker-basis", choices=("assumed", "measured"), default="assumed")
    p.add_argument("--broker-source", default="")
    p.add_argument("--ingress-vcpu", type=int, default=32)
    p.add_argument("--ingress-memory-gib", type=int, default=64)
    p.add_argument("--broker-vcpu", type=int, default=32)
    p.add_argument("--broker-memory-gib", type=int, default=128)
    p.add_argument("--broker-leader-mb-s", type=positive_fraction, default=Fraction(200), help="effective leader append input capacity, decimal MB/s; assumed by default")
    p.add_argument("--broker-disk-mb-s", type=positive_fraction, default=Fraction(600), help="effective sustained replica-write capacity, decimal MB/s; assumed by default")
    p.add_argument("--broker-network-gbps", type=positive_fraction, default=Fraction(10), help="effective network capacity in decimal Gb/s; assumed by default")
    p.add_argument("--network-mode", choices=("per-direction", "aggregate"), default="per-direction")
    p.add_argument("--transport-multiplier", type=positive_fraction, default=Fraction(115, 100))
    p.add_argument("--disk-write-amplification", type=positive_fraction, default=Fraction(1))
    p.add_argument("--operational-broker-floor", type=int, default=6, help="separate operational floor, multiple of three; use3 only as an explicit POC assumption")
    p.add_argument("--compact-payload-bytes", type=positive_fraction, default=Fraction(28))
    p.add_argument("--legacy-payload-bytes", type=positive_fraction, default=Fraction(96))
    p.add_argument("--compact-frame-header-bytes", type=positive_fraction, default=Fraction(80))
    p.add_argument("--compact-average-frame-attempts", type=positive_fraction, default=Fraction(512))
    p.add_argument("--kafka-overhead-bytes", type=positive_fraction, default=Fraction(12), help="assumed amortized record/batch/transaction overhead per attempt; not a measurement")
    p.add_argument("--nvme-tb-per-broker", type=positive_fraction, default=Fraction(192, 100))
    p.add_argument("--retention-events", type=int, default=1)
    p.add_argument("--storage-overhead-multiplier", type=positive_fraction, default=Fraction(3, 2))
    p.add_argument("--maximum-disk-fill", type=positive_fraction, default=Fraction(3, 5))
    return p


def build_report(args):
    for role in ("ingress", "broker"):
        if getattr(args, role + "_basis") == "measured" and not getattr(args, role + "_source").strip():
            raise ValueError(f"{role} measured capacity requires an explicit --{role}-source")
    if min(args.ingress_vcpu, args.ingress_memory_gib, args.broker_vcpu, args.broker_memory_gib) <= 0:
        raise ValueError("instance shape must have positive vCPU and memory")
    workload = Workload(args.unique_voters, args.seconds, args.repeat_fraction, args.utilization)
    limits = BrokerLimits(args.broker_leader_mb_s, args.broker_disk_mb_s, args.broker_network_gbps, args.network_mode, args.transport_multiplier, args.disk_write_amplification, args.operational_broker_floor, args.nvme_tb_per_broker, args.retention_events, args.storage_overhead_multiplier, args.maximum_disk_fill)
    layouts = [RecordLayout("compact_framed", args.compact_payload_bytes, args.compact_frame_header_bytes, args.compact_average_frame_attempts, args.kafka_overhead_bytes), RecordLayout("legacy", args.legacy_payload_bytes, 0, 1, args.kafka_overhead_bytes)]
    scenarios = args.ingress_rps or [100_000, 200_000, 250_000, 300_000]
    ingress = [ingress_sizing(workload, rate) for rate in scenarios]
    candidate = ingress_sizing(workload, args.candidate_ingress_rps)
    return {
        "kind": "capacity planning sensitivity; NOT measured production capacity",
        "workload": {"unique_voters": workload.unique_voters, "seconds": number(workload.seconds), "repeat_fraction": number(workload.repeat_fraction), "attempts_per_event": workload.attempts, "attempts_per_second": workload.rate, "availability_zones": AZS, "replication_factor": REPLICATION, "utilization_target_normal_and_after_one_az_loss": number(workload.utilization)},
        "ingress": {"basis": args.ingress_basis, "source": args.ingress_source or "planning assumption; no HTTP instance capacity measured", "candidate_instance_shape": {"vcpu": args.ingress_vcpu, "memory_gib": args.ingress_memory_gib}, "shape_note": "Shape does not establish throughput; sensitivity capacities require measurement on the intended instance.", "sensitivity": ingress, "candidate": candidate},
        "broker_capacity_inputs": {"basis": args.broker_basis, "source": args.broker_source or "planning assumptions; no production broker capacity measured", "instance_shape": {"vcpu": args.broker_vcpu, "memory_gib": args.broker_memory_gib}, "leader_mb_s": number(limits.leader_mb_s), "replica_disk_mb_s": number(limits.disk_mb_s), "network_gbps": number(limits.network_gbps), "network_mode": limits.network_mode, "transport_multiplier": number(limits.transport_multiplier), "disk_write_amplification": number(limits.disk_write_amplification)},
        "broker_scenarios": [broker_sizing(workload, layout, limits) for layout in layouts],
        "limitations": [
            "CPU-only in-memory throughput is not used as an HTTP or durable-ingestion node capacity.",
            "Traffic and leaders are assumed balanced. After one AZ is lost, all new traffic uses the remaining two thirds of nodes; ISR has two surviving copies. Acks require both surviving copies; normal acks=all waits for all current ISR copies.",
            "The 70% target is a planning utilization bound, not a loss or latency guarantee. A switching pause can miss the one-minute admission deadline despite adequate steady-state capacity.",
            "Network includes client ingress and replica transfers; normal replication egress is RF-1 streams, degraded RF-2. No replay/consumer result reads or replica rebuilding during the minute is included. Small acknowledgements/control traffic must fit the explicit transport margin; protocol CPU/transaction coordinator limits require separate measurement.",
            "Frame occupancy and Kafka overhead are explicit assumptions. A maximum frame size does not prove the average occupancy; Kafka transaction rate cannot be inferred without the batching policy.",
            "Disk throughput, disk write amplification, retention-space overhead and disk-fill reserve are distinct inputs. NVMe capacity does not establish sustained write speed or loss-of-power guarantees.",
            "Operational broker floor is separate from arithmetic capacity minimum. Controllers, routing/control-plane services, results workers and administrative database require separate sizing.",
            "No independent-AZ failover or production load is tested by this calculator. No infrastructure is created and no price is estimated.",
        ],
    }


def main(argv=None):
    p = parser()
    args = p.parse_args(argv)
    try:
        result = build_report(args)
    except (ValueError, ZeroDivisionError) as error:
        p.error(str(error))
    json.dump(json_safe(result), sys.stdout, indent=2, ensure_ascii=False, allow_nan=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
