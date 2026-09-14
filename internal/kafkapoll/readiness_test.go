package kafkapoll

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gigaquizz/internal/poll"
	"gigaquizz/internal/votelog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// An in-memory Kafka wire fixture: no listener, PG connection or real broker.
// Only ApiVersions and broker-only Metadata requests are permitted.
type readinessBroker struct {
	online   atomic.Bool
	hold     atomic.Bool
	metadata atomic.Uint64
	wg       sync.WaitGroup
}

func newReadinessBroker(t *testing.T) (*readinessBroker, *kgo.Client) {
	t.Helper()
	b := new(readinessBroker)
	b.online.Store(true)
	client, err := votelog.NewClient(votelog.Config{Brokers: []string{"127.0.0.1:19092"}},
		kgo.MaxVersions(kversion.V0_10_0()),
		kgo.Dialer(func(context.Context, string, string) (net.Conn, error) {
			if !b.online.Load() {
				return nil, errors.New("fixture broker unavailable")
			}
			client, server := net.Pipe()
			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				defer server.Close()
				b.serve(t, server)
			}()
			return client, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); b.wg.Wait() })
	return b, client
}

func (b *readinessBroker) serve(t *testing.T, conn net.Conn) {
	for {
		var header [4]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(header[:])
		if n < 10 || n > 65536 {
			t.Error("unbounded Kafka fixture request")
			return
		}
		request := make([]byte, n)
		if _, err := io.ReadFull(conn, request); err != nil {
			return
		}
		if !b.online.Load() {
			return
		}
		key, version := int16(binary.BigEndian.Uint16(request)), int16(binary.BigEndian.Uint16(request[2:]))
		var response kmsg.Response
		switch key {
		case 18:
			r := kmsg.NewPtrApiVersionsResponse()
			r.Version = version
			r.ApiKeys = []kmsg.ApiVersionsResponseApiKey{{ApiKey: 3, MinVersion: 1, MaxVersion: 1}, {ApiKey: 18, MinVersion: 0, MaxVersion: 0}}
			response = r
		case 3:
			clientIDLen := int(int16(binary.BigEndian.Uint16(request[8:])))
			if clientIDLen < 0 || 10+clientIDLen > len(request) {
				t.Error("malformed Kafka fixture header")
				return
			}
			rq := kmsg.NewPtrMetadataRequest()
			rq.Version = version
			if err := rq.ReadFrom(request[10+clientIDLen:]); err != nil || version != 1 || rq.Topics == nil || len(rq.Topics) != 0 {
				t.Error("probe requested topic inventory or unsupported metadata format")
				return
			}
			b.metadata.Add(1)
			if b.hold.Load() {
				continue
			}
			r := kmsg.NewPtrMetadataResponse()
			r.Version = version
			r.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: "127.0.0.1", Port: 19092}}
			r.ControllerID = 1
			response = r
		default:
			t.Errorf("probe performed non-metadata Kafka operation %d", key)
			return
		}
		body := append([]byte(nil), request[4:8]...)
		body = response.AppendTo(body)
		binary.BigEndian.PutUint32(header[:], uint32(len(body)))
		if _, err := conn.Write(append(header[:], body...)); err != nil {
			return
		}
	}
}

func TestKafkaReadinessChecksEmptyAndFinalizedInventoryAndRecovers(t *testing.T) {
	for _, finalized := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "finalized"}[finalized], func(t *testing.T) {
			broker, client := newReadinessBroker(t)
			s := &Store{kafkaProbe: client, polls: map[string]*entry{}}
			if finalized {
				at := time.Now()
				s.polls["old"] = &entry{poll: poll.Poll{FinalizedAt: &at}}
			}
			probe := func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				return s.checkKafkaReady(ctx)
			}
			if err := probe(); err != nil {
				t.Fatal("online broker not ready", err)
			}
			broker.online.Store(false)
			if err := probe(); !errors.Is(err, errKafkaUnavailable) {
				t.Fatal("offline broker bypassed by inventory", err)
			}
			broker.online.Store(true)
			if err := probe(); err != nil {
				t.Fatal("readiness did not recover", err)
			}
			if broker.metadata.Load() < 2 {
				t.Fatal("readiness did not actually read broker metadata")
			}
		})
	}
}

func TestKafkaReadinessPreservesPendingWriterChecks(t *testing.T) {
	_, client := newReadinessBroker(t)
	e := &entry{}
	s := &Store{kafkaProbe: client, polls: map[string]*entry{"pending": e}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.checkKafkaReady(ctx); err == nil {
		t.Fatal("missing pending writer reported ready")
	}
	e.writer = &recoveryWriter{metrics: map[string]uint64{"failed_writers": 1}}
	if err := s.checkKafkaReady(ctx); err == nil {
		t.Fatal("terminal writer reported ready")
	}
	e.writer = &recoveryWriter{metrics: map[string]uint64{}}
	if err := s.checkKafkaReady(ctx); err != nil {
		t.Fatal("healthy writer rejected", err)
	}
}

func TestKafkaReadinessHonorsDeadlineAndClosedClient(t *testing.T) {
	broker, client := newReadinessBroker(t)
	s := &Store{kafkaProbe: client}
	broker.hold.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := s.checkKafkaReady(ctx); err == nil || time.Since(started) > time.Second {
		t.Fatal("metadata probe ignored caller deadline", err)
	}
	client.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := s.checkKafkaReady(ctx2); !errors.Is(err, errKafkaUnavailable) {
		t.Fatal("closed probe reported ready", err)
	}
}

func TestInvalidBrokersRejectedBeforeDatabaseInitialization(t *testing.T) {
	for _, brokers := range [][]string{{"127.0.0.1:0"}, {"127.0.0.1:65536"}, {"127.0.0.1"}, {"broker.example:9092"}, {""}} {
		expected := votelog.ValidateBrokers(brokers, false)
		if expected == nil {
			t.Fatal("fixture must be invalid")
		}
		// Invalid DB and nil context make progression beyond pure option
		// validation fail. No metadata schema or network client may be created.
		_, err := New(nil, Options{DatabaseURL: "invalid database configuration", Brokers: brokers})
		if err == nil || err.Error() != expected.Error() {
			t.Fatal("broker syntax not rejected first", err)
		}
	}
}

func TestStartupProbeCancellationPrecedesDatabaseInitialization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := New(ctx, Options{DatabaseURL: "invalid database configuration", Brokers: []string{"127.0.0.1:19092"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("startup proceeded to PostgreSQL after canceled Kafka preflight", err)
	}
}
