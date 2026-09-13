package kafkapoll

// Diagnostics exposes fixed, bounded aggregate names only. Counters cover
// this Store lifetime. Finalization retains counters, while gauges describe
// current writers only. A restarted Store does not invent historical counters.
func (s *Store) Diagnostics() map[string]uint64 {
	out := map[string]uint64{"storage_preparation_failures": s.preparationFailures.Load()}
	mapping := map[string]string{"queued_votes": "storage_queued_votes", "active_votes": "storage_active_votes", "failed_writers": "storage_failed_writers", "committed_attempts": "storage_durable_votes", "committed_frames": "storage_durable_frames", "transactions": "storage_batches", "transaction_ns": "storage_batch_ns", "busy_votes": "storage_busy_votes"}
	for _, key := range writerFailureCounters {
		mapping[key] = "storage_" + key
	}
	for _, target := range mapping {
		out[target] = 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for source, value := range s.retiredMetrics {
		if target, ok := mapping[source]; ok {
			out[target] += value
		}
	}
	for _, e := range s.polls {
		if e.writer == nil {
			continue
		}
		metrics := e.writer.Metrics()
		for source, target := range mapping {
			out[target] += metrics[source]
		}
	}
	return out
}

// Called under s.mu only when permanently releasing a writer. Gauges and
// arbitrary error labels are excluded from the retained lifetime counters.
func (s *Store) retainMetrics(metrics map[string]uint64) {
	if s.retiredMetrics == nil {
		s.retiredMetrics = make(map[string]uint64)
	}
	for _, key := range []string{"committed_attempts", "committed_frames", "transactions", "transaction_ns", "busy_votes"} {
		s.retiredMetrics[key] += metrics[key]
	}
	for _, key := range writerFailureCounters {
		if value, exists := metrics[key]; exists {
			s.retiredMetrics[key] += value
		}
	}
}

var writerFailureCounters = [...]string{"writer_failures_timeout", "writer_failures_fenced", "writer_failures_network", "writer_failures_client_closed", "writer_failures_other"}
