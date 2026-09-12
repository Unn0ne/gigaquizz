import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('http_capacity', ROOT / 'scripts/http_capacity.py')
model = importlib.util.module_from_spec(spec)
spec.loader.exec_module(model)


class CapacityTests(unittest.TestCase):
    def test_uniform_population_is_not_reduced_by_conversion(self):
        r = model.calculate(ROOT, repeats=0)
        self.assertEqual(r['workload']['attempts'], 100_000_000)
        self.assertAlmostEqual(r['workload']['unique_per_second'] * 60, 100_000_000)
        self.assertEqual(r['workload']['all_HTTP_requests_without_reloads'], 600_000_000)

    def test_repeats_add_writes_but_not_new_participants_or_page_loads(self):
        r = model.calculate(ROOT, repeats=.2)
        self.assertEqual(r['workload']['attempts'], 120_000_000)
        self.assertEqual(r['workload']['question_GETs'], 100_000_000)
        self.assertEqual(r['memory']['raw_unique_128_bit_keys_bytes'], 1_600_000_000)

    def test_cold_reload_and_network_budget(self):
        r = model.calculate(ROOT, repeats=.1)
        self.assertEqual(r['full_reload_scenario']['all_HTTP_requests'], 660_000_000)
        self.assertEqual(r['traffic']['all_ingress_HTTP_estimate']['bytes'], 368_640_000_000)
        self.assertEqual(r['traffic']['static_response_headers']['bytes'], 204_800_000_000)
        traffic = r['traffic']
        self.assertEqual(traffic['all_egress_gzip_HTTP_estimate']['bytes'], sum(traffic[k]['bytes'] for k in
                         ('page_gzip_estimate', 'static_response_headers', 'question_responses', 'vote_egress')))

    def test_peak_scenarios_preserve_total(self):
        for r in model.calculate(ROOT)['arrival_sensitivity']:
            self.assertAlmostEqual(r['first_10s_unique_RPS'] * 10 + r['remaining_50s_unique_RPS'] * 50, 100_000_000)

    def test_after_failure_still_has_headroom(self):
        r = model.calculate(ROOT)
        for n in r['node_sensitivity']:
            self.assertGreaterEqual((n['nodes_to_tolerate_one_node_loss'] - 1) * n['assumed_per_node_capacity_RPS'] * .7,
                                    r['workload']['vote_attempts_per_second'])

    def test_impossible_settings_are_rejected(self):
        for args in ({'voters': 0}, {'seconds': 0}, {'repeats': -1}, {'utilization': 0}):
            with self.assertRaises(ValueError):
                model.calculate(ROOT, **args)


if __name__ == '__main__':
    unittest.main()
