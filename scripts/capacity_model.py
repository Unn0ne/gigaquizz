"""Arithmetic for docs/architecture.md; assumptions, never server benchmarks.

Run from the project root: python3 scripts/capacity_model.py
No network, credentials, database access, or third-party packages are used.
"""

import gzip
import json
import math
from fractions import Fraction
from pathlib import Path

N = 100_000_000
SECONDS = 60
GETS_PER_VOTER = 5
UTILIZATION = 0.65
SHARD_SKEW = 1.2
RATE = N / SECONDS
POST_DESIGN_RATE = RATE * 1.3
# Exact arithmetic at ceil boundaries: binary floats can turn 400 into 401.
EXACT_POST_RATE = Fraction(N, SECONDS) * Fraction(13, 10)
EXACT_UTILIZATION = Fraction(str(UTILIZATION))
EXACT_SHARD_SKEW = Fraction(str(SHARD_SKEW))
KIB = 1024
GB = 10**9


def transfer(size_bytes, attempts=N, duration=SECONDS):
    size = size_bytes * attempts
    return {"GB": size / GB, "Gbit_per_s": size * 8 / duration / GB}


assets = []
root = Path(__file__).resolve().parents[1]
for name in ("poll.html", "app.css", "common.js", "poll.js"):
    raw = (root / "internal/web/static" / name).read_bytes()
    assets.append({"file": name, "raw_bytes": len(raw),
                   "gzip9_bytes": len(gzip.compress(raw, compresslevel=9, mtime=0))})

model = {
    "assumptions": {"voters": N, "seconds": SECONDS, "GETs_per_voter": GETS_PER_VOTER,
                    "utilization_fraction": UTILIZATION, "shard_skew": SHARD_SKEW,
                    "rates_are_hypothetical_not_benchmarks": True},
    "unique_votes_per_s": RATE,
    "HTTP": [{"extra_POST_fraction": r,
              "POST_per_s": RATE * (1 + r),
              "edge_HTTP_per_s": RATE * (GETS_PER_VOTER + 1 + r),
              "edge_HTTP_per_event": N * (GETS_PER_VOTER + 1 + r),
              "origin_per_s_all_GET_hit_99pct": RATE * (1 + r + GETS_PER_VOTER * 0.01),
              "origin_per_s_all_GET_hit_99_9pct": RATE * (1 + r + GETS_PER_VOTER * 0.001),
              "origin_per_s_only_3_static_hit_99pct": RATE * (1 + r + 2 + 3 * 0.01)}
             for r in (0, 0.1, 0.3)],
    "voting_bursts": [{"fraction_in_first_window": f, "window_s": t,
                       "first_window_unique_per_s": N * f / t,
                       "remaining_unique_per_s": N * (1 - f) / (SECONDS - t)}
                      for f, t in ((0.5, 10), (0.8, 10), (0.1, 1))],
    "all_GET_bodies": [{"KiB_per_voter": kb, **transfer(kb * KIB)} for kb in (16, 50, 100)],
    "POST_exchange_30pct_repeats": {
        "inbound_0_7KiB": transfer(0.7 * KIB, N * 1.3),
        "inbound_2KiB": transfer(2 * KIB, N * 1.3),
        "outbound_0_3KiB": transfer(0.3 * KIB, N * 1.3),
        "outbound_1KiB": transfer(KIB, N * 1.3)},
    "concurrent_POSTs_30pct_repeats": [{"mean_seconds": w, "requests": POST_DESIGN_RATE * w}
                                         for w in (0.02, 0.1, 0.3, 1)],
    "CPU_cores_30pct_repeats": [{"CPU_ms_per_attempt": ms,
                                "cores_at_65pct": POST_DESIGN_RATE * ms / 1000 / UTILIZATION}
                               for ms in (0.05, 0.2, 1)],
    "app_instances_three_zones_survive_one": [
        {"hypothetical_q_per_instance": q,
         "instances": 3 * math.ceil(EXACT_POST_RATE / (2 * EXACT_UTILIZATION * q))}
        for q in (10_000, 20_000, 50_000)],
    "physical_HA_shard_groups": [
        {"hypothetical_q_per_group": q,
         "groups": math.ceil(EXACT_SHARD_SKEW * EXACT_POST_RATE / (EXACT_UTILIZATION * q))}
        for q in (10_000, 25_000, 50_000)],
    "WAL_sensitivity": [{"assumed_bytes_per_new_vote": w, "primary_WAL_GB": N * w / GB,
                         "primary_WAL_MB_per_s": RATE * w / 10**6,
                         "two_replica_transfer_Gbit_per_s": 2 * RATE * w * 8 / GB}
                        for w in (256, 512, 1024, 4096)],
    "five_second_reliable_buffer": {
        "unique_votes": 5 * RATE, "logical_64B_GB": 5 * RATE * 64 / GB,
        "drain_s_with_live_arrivals": [{"service_rate_multiple": m, "seconds": 5 / (m - 1)}
                                       for m in (1.25, 1.5, 2)]},
    "event_unavailability": [{"success_fraction": a, "unserved_voters": N * (1 - a),
                              "equivalent_total_outage_s": SECONDS * (1 - a)}
                             for a in (0.999, 0.9999, 0.99999)],
    "random_128bit_collision_probability_approx": N * (N - 1) / (2 * 2**128),
    "local_asset_file_measurements": assets,
    "local_assets_raw_total_bytes": sum(a["raw_bytes"] for a in assets),
    "local_assets_gzip9_total_bytes": sum(a["gzip9_bytes"] for a in assets),
    "illustrative_CDN_USD_excluding_free_tier_discounts_and_other_items": {
        "HTTPS_630M_US": 630_000_000 / 10_000 * 0.0100,
        "HTTPS_630M_Europe": 630_000_000 / 10_000 * 0.0120,
        "GET_16KiB_at_0_085_per_decimal_GB": N * 16 * KIB / GB * 0.085,
        "GET_50KiB_at_0_085_per_decimal_GB": N * 50 * KIB / GB * 0.085},
}

print(json.dumps(model, ensure_ascii=False, indent=2))
