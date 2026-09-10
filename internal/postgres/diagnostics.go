package postgres

import "sync/atomic"

type storeDiagnostics struct {
	voteSQLCalls atomic.Uint64
	voteSQLNS    atomic.Uint64
	proofQueries atomic.Uint64
	proofQueryNS atomic.Uint64
	proofRetries atomic.Uint64
	proofWaitNS  atomic.Uint64
}

// Diagnostics returns bounded process-local counters without issuing SQL.
// SQL counters include failed calls. Vote SQL time excludes pool acquisition
// and the subsequent durability proof; proof time covers each proof query,
// excluding its retry timer. Proof counters include all Store operations.
// The acquired/total connection values are gauges; other values are cumulative.
// Concurrent operations may advance counters while this snapshot is read.
func (s *Store) Diagnostics() map[string]uint64 {
	stats := s.pool.Stat()
	return map[string]uint64{
		"db_vote_sql_calls":              s.diagnostics.voteSQLCalls.Load(),
		"db_vote_sql_ns":                 s.diagnostics.voteSQLNS.Load(),
		"db_proof_queries":               s.diagnostics.proofQueries.Load(),
		"db_proof_query_ns":              s.diagnostics.proofQueryNS.Load(),
		"db_proof_retries":               s.diagnostics.proofRetries.Load(),
		"db_proof_wait_ns":               s.diagnostics.proofWaitNS.Load(),
		"db_pool_acquire_count":          uint64(stats.AcquireCount()),
		"db_pool_acquire_ns":             uint64(stats.AcquireDuration()),
		"db_pool_empty_acquire_count":    uint64(stats.EmptyAcquireCount()),
		"db_pool_canceled_acquire_count": uint64(stats.CanceledAcquireCount()),
		"db_pool_new_conns_count":        uint64(stats.NewConnsCount()),
		"db_pool_acquired_conns":         uint64(stats.AcquiredConns()),
		"db_pool_total_conns":            uint64(stats.TotalConns()),
	}
}
