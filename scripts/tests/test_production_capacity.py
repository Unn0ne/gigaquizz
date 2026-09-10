"""Arithmetic/model invariants only; these tests do not generate service load."""

from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import production_capacity as sizing


class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.workload = sizing.Workload()
        self.compact = sizing.RecordLayout("compact", 28, 80, 512, 12)
        self.legacy = sizing.RecordLayout("legacy", 96, 0, 1, 12)
        self.limits = sizing.BrokerLimits()

    def test_ingress_sensitivity_preserves_capacity_after_az_loss(self):
        self.assertEqual(self.workload.attempts, 120_000_000)
        self.assertEqual(self.workload.rate, 2_000_000)
        for rate, nodes in ((100_000, 45), (200_000, 24), (250_000, 18), (300_000, 15)):
            row = sizing.ingress_sizing(self.workload, rate)
            self.assertEqual(row["nodes"], nodes)
            self.assertLessEqual(row["one_az_loss_utilization"], Fraction(7, 10))
            fewer_survivors = (nodes - 3) * 2 // 3
            self.assertGreater(self.workload.rate / (fewer_survivors * rate), Fraction(7, 10))
        self.assertEqual(sizing.ingress_sizing(self.workload, 250_000)["one_az_loss_utilization"], Fraction(2, 3))

    def test_exact_rounding_does_not_add_unnecessary_az_triplet(self):
        workload = replace(self.workload, unique_voters=70_000_000)
        self.assertEqual(workload.rate, 1_400_000)
        self.assertEqual(sizing.ingress_sizing(workload, 100_000)["nodes"], 30)
        self.assertEqual(sizing.triplets_for_ratio(Fraction(6)), 6)
        self.assertEqual(sizing.triplets_for_ratio(Fraction(6) + Fraction(1, 10**12)), 9)

    def test_record_header_kafka_overhead_and_transport_are_separate(self):
        row = sizing.broker_sizing(self.workload, self.compact, self.limits)
        self.assertEqual(self.compact.amortized_header, Fraction(5, 32))
        self.assertEqual(self.compact.effective_bytes, Fraction(1285, 32))
        self.assertEqual(row["logical_input_mb_s"], Fraction(1285, 16))
        self.assertEqual(self.legacy.effective_bytes, 108)
        # Changing transport cannot change leader append, disk bytes, or storage.
        doubled = sizing.broker_state(self.workload, self.compact, replace(self.limits, transport_multiplier=2), 6, False)
        normal = row["normal"]
        self.assertEqual(doubled["leader_input_mb_s_per_node"], normal["leader_input_mb_s_per_node"])
        self.assertEqual(doubled["replica_disk_write_mb_s_per_node"], normal["replica_disk_write_mb_s_per_node"])
        self.assertGreater(doubled["network_in_mb_s_per_node"], normal["network_in_mb_s_per_node"])

    def test_degraded_leaders_redistribute_and_two_copies_remain(self):
        normal = sizing.broker_state(self.workload, self.compact, self.limits, 6, False)
        degraded = sizing.broker_state(self.workload, self.compact, self.limits, 6, True)
        self.assertEqual(degraded["surviving_nodes"], 4)
        self.assertEqual(degraded["copies_per_partition"], 2)
        self.assertEqual(degraded["leader_input_mb_s_per_node"], normal["leader_input_mb_s_per_node"] * Fraction(3, 2))
        self.assertEqual(degraded["replica_disk_write_mb_s_per_node"], normal["replica_disk_write_mb_s_per_node"])
        self.assertEqual(degraded["network_in_mb_s_per_node"], normal["network_in_mb_s_per_node"])
        self.assertEqual(degraded["network_out_mb_s_per_node"], normal["network_out_mb_s_per_node"] * Fraction(3, 4))

    def test_each_throughput_constraint_can_independently_determine_nodes(self):
        cases = (
            (replace(self.limits, leader_mb_s=10, disk_mb_s=100_000, network_gbps=100_000, operational_floor=3), "leader", 18),
            (replace(self.limits, leader_mb_s=100_000, disk_mb_s=10, network_gbps=100_000, operational_floor=3), "disk", 36),
            (replace(self.limits, leader_mb_s=100_000, disk_mb_s=100_000, network_gbps=Fraction(8, 100), operational_floor=3), "network", 42),
        )
        for limits, dimension, nodes in cases:
            row = sizing.broker_sizing(self.workload, self.compact, limits)
            self.assertEqual(row["throughput_minimum_by_constraint"][dimension], nodes)
            self.assertEqual(row["candidate_brokers"], nodes)
            self.assertTrue(row["within_target_in_both_states"])
            prior = [sizing.broker_state(self.workload, self.compact, limits, nodes - 3, degraded) for degraded in (False, True)]
            self.assertTrue(any(state["utilization"][dimension] > self.workload.utilization for state in prior))

    def test_network_per_direction_is_not_aggregate(self):
        normal = sizing.broker_state(self.workload, self.compact, self.limits, 6, False)
        aggregate = sizing.broker_state(self.workload, self.compact, replace(self.limits, network_mode="aggregate"), 6, False)
        self.assertEqual(aggregate["utilization"]["network"], normal["utilization"]["network"] * Fraction(5, 3))

    def test_operational_floor_and_retention_are_not_throughput(self):
        row = sizing.broker_sizing(self.workload, self.compact, self.limits)
        self.assertEqual(row["capacity_minimum_brokers"], 3)
        self.assertEqual(row["operational_floor_brokers"], 6)
        self.assertEqual(row["candidate_brokers"], 6)
        retained = sizing.broker_sizing(self.workload, self.compact, replace(self.limits, retention_events=1000))
        self.assertEqual(retained["throughput_minimum_brokers"], row["throughput_minimum_brokers"])
        self.assertGreater(retained["storage_minimum_brokers"], 6)
        self.assertEqual(retained["retention"]["physical_stored_bytes_with_overhead"], row["retention"]["physical_stored_bytes_with_overhead"] * 1000)
        self.assertLessEqual(retained["retention"]["projected_disk_fill"], self.limits.maximum_disk_fill)

    def test_measured_label_requires_source_and_defaults_remain_assumptions(self):
        p = sizing.parser()
        result = sizing.build_report(p.parse_args([]))
        self.assertEqual(result["ingress"]["basis"], "assumed")
        self.assertEqual(result["broker_capacity_inputs"]["basis"], "assumed")
        self.assertEqual(result["ingress"]["candidate"]["nodes"], 18)
        with self.assertRaises(ValueError):
            sizing.build_report(p.parse_args(["--ingress-basis", "measured"]))


if __name__ == "__main__":
    unittest.main()
