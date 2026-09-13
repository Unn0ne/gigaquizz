package votelog

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestWriterFailureTelemetryOnlyUsesFixedCategories(t *testing.T) {
	for _, tc := range []struct {
		err      error
		category string
	}{
		{context.DeadlineExceeded, "timeout"}, {kerr.RequestTimedOut, "timeout"},
		{&net.DNSError{Err: "private address omitted", IsTimeout: true}, "timeout"},
		{kerr.ProducerFenced, "fenced"}, {kerr.InvalidProducerEpoch, "fenced"},
		{kerr.NetworkException, "network"}, {&net.OpError{Op: "dial", Err: errors.New("private endpoint omitted")}, "network"},
		{kgo.ErrClientClosed, "client_closed"}, {errors.New("private raw failure omitted"), "other"},
	} {
		wrapped := &transactionFailure{stage: "produce", err: tc.err}
		if got := writerFailureCategory(wrapped); got != "writer_failures_"+tc.category {
			t.Fatalf("fixed category mismatch: %s", got)
		}
		s := &Store{writers: []*writer{{failure: wrapped}}}
		if s.Metrics()["writer_failures_"+tc.category] != 1 {
			t.Fatal("terminal failure not represented in diagnostics")
		}
	}
}
