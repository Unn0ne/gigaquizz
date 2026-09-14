package kafkapoll

import (
	"context"
	"errors"
	"time"
)

var errKafkaUnavailable = errors.New("Kafka broker metadata unavailable")

// checkKafkaReady is independent of the poll inventory. A newly deployed or
// completely finalized schema must not report ready with unreachable brokers.
// Ping sends a broker-only Metadata request with an empty topic list and succeeds
// when one broker responds. It proves connectivity/authentication only, not all
// replicas, topic ACLs, or transaction readiness. Vote never calls this method.
func (s *Store) checkKafkaReady(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if s.kafkaProbe == nil || s.kafkaProbe.Ping(ctx) != nil {
		return errKafkaUnavailable
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.polls {
		if e.poll.FinalizedAt == nil && (e.writer == nil || e.writer.Metrics()["failed_writers"] > 0) {
			return errors.New("a pending poll journal is unavailable")
		}
	}
	return nil
}
