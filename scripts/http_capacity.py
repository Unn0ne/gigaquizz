#!/usr/bin/env python3
"""Reproducible planning arithmetic; node throughput and wire sizes are assumptions."""
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
from fractions import Fraction


def calculate(root, voters=100_000_000, seconds=60, repeats=0.1, utilization=0.7):
    if voters < 1 or not all(math.isfinite(v) for v in (seconds, repeats, utilization)) or seconds <= 0 or not 0 <= repeats <= 1 or not 0 < utilization <= 1:
        raise ValueError('invalid workload or utilization')
    assets = {}
    for name in ('poll.html', 'app.css', 'common.js', 'poll.js'):
        data = (root / 'internal/web/static' / name).read_bytes()
        assets[name] = {'bytes': len(data), 'gzip_bytes': len(gzip.compress(data, mtime=0)),
                        'sha256': hashlib.sha256(data).hexdigest()}
    page = sum(x['bytes'] for x in assets.values())
    compressed = sum(x['gzip_bytes'] for x in assets.values())
    unique_rate = voters / seconds
    def attempt_count(fraction):
        return voters + math.ceil(voters * Fraction(str(fraction)))
    attempts = attempt_count(repeats)
    rate = attempts / seconds
    def traffic(bytes_per_item, count, interval=seconds):
        total = bytes_per_item * count
        return {'bytes': total, 'decimal_GB': total / 1e9,
                'mean_Gbit_s': total * 8 / interval / 1e9}
    def nodes(throughput, tolerate_one=False):
        return math.ceil(rate / (utilization * throughput)) + int(tolerate_one)
    return {
        'kind': 'calculation, not measured production capacity',
        'assumptions': {'unique_voters': voters, 'voting_seconds': seconds,
                        'additional_vote_attempt_fraction': repeats, 'utilization': utilization,
                        'post_request_http_bytes': 1024, 'post_response_http_bytes': 1024,
                        'question_response_http_bytes': 1536,
                        'GET_request_http_bytes': 512, 'static_response_headers_bytes_each': 512,
                        'wire_note': 'HTTP byte assumptions; TCP/IP, TLS, packet loss and retransmits are additional',
                        'page_note': 'four explicitly linked resources; optional browser requests such as favicon excluded; gzip size estimated offline with Python default level; Go serves precomputed gzip.BestSpeed, so measured payload sizes differ',
                        'arrivals_note': 'page opening and voting need not occur in the same window',
                        'replication': 'Kafka RF3; one broker copy plus two replica copies; protocol overhead excluded'},
        'source_assets': assets,
        'workload': {'unique_per_second': unique_rate, 'attempts': attempts,
                     'vote_attempts_per_second': rate,
                     'cold_page_and_assets_GETs': 4 * voters,
                     'question_GETs': voters,
                     'all_HTTP_requests_without_reloads': 5 * voters + attempts,
                     'all_HTTP_RPS_if_open_and_vote_in_same_window': 5 * unique_rate + rate,
                     'new_connections_per_second_if_one_per_cold_browser': unique_rate,
                     'periodic_timer_network_requests': 0,
                     'clock_fallback_note': 'One /api/time request when cached Date/Age cannot be used, or before the first local scheduled/closed rejection without a receipt. These conditional GETs are additional; no periodic time polling.'},
        'traffic': {
            'page_uncompressed': traffic(page, voters),
            'page_gzip_estimate': traffic(compressed, voters),
            'page_gzip_preloaded_over_10_minutes': traffic(compressed, voters, 600),
            'question_responses': traffic(1536, voters),
            'vote_ingress': traffic(1024, attempts),
            'vote_egress': traffic(1024, attempts),
            'GET_ingress': traffic(512, 5 * voters),
            'static_response_headers': traffic(512, 4 * voters),
            'all_ingress_HTTP_estimate': traffic(1, 512 * 5 * voters + 1024 * attempts),
            'all_egress_gzip_HTTP_estimate': traffic(1, (compressed + 4 * 512 + 1536) * voters + 1024 * attempts),
        },
        'full_reload_scenario': {
            'condition': 'every additional vote attempt also cold-loads four resources and the question; optional, no browser cache',
            'page_and_assets_GETs': 4 * attempts, 'question_GETs': attempts,
            'all_HTTP_requests': 6 * attempts, 'all_HTTP_RPS': 6 * rate,
            'outgoing_gzip_HTTP_estimate': traffic(compressed + 4 * 512 + 1536 + 1024, attempts),
            'incoming_HTTP_estimate': traffic(5 * 512 + 1024, attempts),
        },
        'read_cache_sensitivity': [
            {'hit_fraction': hit, 'origin_question_RPS': unique_rate * (1 - hit),
             'note': 'immutable /definition is shared-cacheable; legacy /api/polls/{id} and /api/time remain no-store; CDN hit rates require measurement'}
            for hit in (0, .9, .99, .999)
        ],
        'repeat_sensitivity': [
            {'repeat_fraction': f, 'attempts': attempt_count(f),
             'vote_RPS': attempt_count(f) / seconds} for f in (0, .01, .1, .2, .3)
        ],
        'arrival_sensitivity': [
            {'first_10s_fraction': f, 'first_10s_unique_RPS': voters * f / 10,
             'remaining_50s_unique_RPS': voters * (1 - f) / 50,
             'scope': 'optional stress scenario; agreed baseline is uniform'}
            for f in (.5, .8)
        ] if seconds == 60 else [],
        'storage': {
            'legacy_files_68_bytes_per_attempt': traffic(68, attempts),
            'legacy_kafka_key_value_96_bytes_per_attempt_one_copy': traffic(96, attempts),
            'legacy_kafka_RF3_bytes': 96 * attempts * 3,
            'packed_formula_scope': 'pure v2 Kafka frames; automatic HTTP singleton stays v1 at96B. Mixed bytes =96*v1_attempts +28*v2_attempts +80*v2_frames; do not use only overall mean fill for a mixed stream',
            'packed': [
                {'mean_votes_per_frame': b, 'files_bytes_per_attempt': 28 + 40 / b,
                 'kafka_bytes_per_attempt_one_copy': 28 + 80 / b,
                 'files_total_bytes': attempts * (28 + 40 / b),
                 'kafka_RF3_total_bytes': attempts * (28 + 80 / b) * 3,
                 'physical_frames_per_second': rate / b}
                for b in (1, 32, 256, 4096)
            ],
            'retention_note': 'multiply event storage by retained polls; add checksums, manifests, indexes and safety reserve',
        },
        'admission_deadline_sensitivity': [
            {'constant_client_to_admission_ms': ms,
             'expected_late_attempts_if_client_sends_until_end': min(attempts, rate * ms / 1000),
             'scope': 'illustrative constant pre-admission delay with uniform client sends; ACK/sync delay after admission does not belong here'}
            for ms in (5, 25, 100, 250)
        ],
        'inflight_sensitivity': [
            {'mean_handler_ms': ms, 'mean_active_requests': rate * ms / 1000,
             'slots_with_utilization_headroom': math.ceil(rate * ms / 1000 / utilization)}
            for ms in (2, 10, 25, 50, 100)
        ],
        'partition_finalization_sensitivity': [
            {'partitions': p, 'mean_unique_per_partition': voters / p,
             'assumed_exact_map_bytes_at_32_per_key': math.ceil(voters / p) * 32,
             'note': 'Sequential partition aggregation now implemented in both branches; hash skew, replay/GC/queue memory and explicit per-partition bound are additional; local partitions do not create network workers'}
            for p in (1, 8, 32, 128, 256)
        ],
        'clock_fallback_sensitivity': [
            {'affected_browser_fraction': f, 'additional_requests_per_check': math.ceil(voters * f),
             'RPS_if_spread_over_minute': voters * f / seconds,
             'note': 'A synchronized deadline burst is not uniform; provision the stateless /api/time path at the edge and measure separately'}
            for f in (.001, .01, 1)
        ],
        'memory': {'raw_unique_128_bit_keys_bytes': voters * 16,
                   'exact_table_sensitivity_bytes': {str(b): voters * b for b in (24, 32, 48)},
                   'note': 'table bytes/key are assumptions, not Go map measurements; add GC, handlers, queues and replay buffers'},
        'CPU_sensitivity': [
            {'assumed_CPU_us_per_attempt': us, 'cores_at_utilization': math.ceil(rate * us / 1e6 / utilization)}
            for us in (1, 5, 10, 25, 50)
        ],
        'node_sensitivity': [
            {'assumed_per_node_capacity_RPS': c, 'nodes_without_failover': nodes(c),
             'nodes_to_tolerate_one_node_loss': nodes(c, True),
             'condition': 'POST layer only with GET/TLS served separately, or C measured with the intended GET/TLS mix; requires routing/ownership; current app has one writer owner'}
            for c in (50_000, 100_000, 200_000, 500_000, 1_000_000)
        ],
        'cost_formula': 'sum(node_count * hourly_rate * reserved_hours) + charged_GB * rate_per_GB + charged_requests * request_rate + storage_GB_month; no provider prices assumed',
        'exclusions': ['DDoS and abusive bots', 'global geographic latency', 'external network failure costs',
                       'production capacity proof', 'automatic HA of current app'],
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--voters', type=int, default=100_000_000)
    p.add_argument('--seconds', type=float, default=60)
    p.add_argument('--repeats', type=float, default=.1)
    p.add_argument('--utilization', type=float, default=.7)
    a = p.parse_args()
    try:
        result = calculate(Path(__file__).resolve().parents[1], a.voters, a.seconds, a.repeats, a.utilization)
    except ValueError as e:
        p.error(str(e))
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
