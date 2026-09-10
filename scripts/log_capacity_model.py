"""Arithmetic for the proposed append-log architecture; not a benchmark.

Run: python3 scripts/log_capacity_model.py
Standard library only. Does not access the network, database or credentials.
All byte units are decimal; capacities are requirements, not measurements.
"""

import json
import math
from fractions import Fraction


def model():
    voters = 100_000_000
    seconds = 60
    unique_rate = Fraction(voters, seconds)
    retries = Fraction(1, 5)
    attempt_rate = unique_rate * (1 + retries)
    utilization = Fraction(4, 5)
    design_rate = attempt_rate / utilization
    survivors = Fraction(2, 3)
    replicas = 3
    sizes = (96, 128, 256)
    return {
        "status": "proposal_arithmetic_not_measured_capacity",
        "assumptions": {
            "voters": voters,
            "seconds": seconds,
            "additional_attempts_fraction": float(retries),
            "utilization_limit": float(utilization),
            "surviving_node_fraction_after_one_of_three_zones_fails": float(survivors),
            "replicas": replicas,
            "compression_credit": 0,
        },
        "rates_per_second": {
            "unique": float(unique_rate),
            "with_additional_attempts": float(attempt_rate),
            "design_including_headroom": float(design_rate),
        },
        "bytes_sensitivity": [
            {
                "assumed_record_bytes": size,
                "event_GB_with_additional_attempts": float(voters * (1 + retries) * size / 10**9),
                "event_replicated_GB": float(voters * (1 + retries) * size * replicas / 10**9),
                "steady_input_MB_per_s": float(attempt_rate * size / 10**6),
                "design_input_MB_per_s": float(design_rate * size / 10**6),
                "design_replicated_write_MB_per_s": float(design_rate * size * replicas / 10**6),
            }
            for size in sizes
        ],
        "partition_requirements": [
            {
                "partitions": partitions,
                "design_records_per_s_per_partition": float(design_rate / partitions),
                "unique_keys_per_partition": voters / partitions,
                "records_per_batch_at_20ms_and_one_writer": float(design_rate / partitions / 50),
                "batch_bytes_at_128B": float(design_rate / partitions / 50 * 128),
                "batches_per_s_at_20ms": partitions * 50,
            }
            for partitions in (128, 256)
        ],
        "inverse_broker_requirements": [
            {
                "brokers_in_three_equal_zones": brokers,
                "surviving_brokers": int(brokers * survivors),
                "required_logical_records_per_s_per_surviving_broker": float(design_rate / (brokers * survivors)),
                "normal_design_replicated_MB_per_s_per_broker_at_128B": float(design_rate * 128 * replicas / brokers / 10**6),
            }
            for brokers in (9, 12, 18, 24)
        ],
        "naive_sql_extrapolation_not_a_deployment_plan": {
            "groups_at_6000_no_reserve": math.ceil(unique_rate / 6000),
            "database_instances_with_three_copies": math.ceil(unique_rate / 6000) * 3,
        },
    }


if __name__ == "__main__":
    print(json.dumps(model(), indent=2, ensure_ascii=False))
