package filelog

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

type crashRecord struct {
	Position Position
	Vote     Vote
}
type crashRead struct {
	Scan    ScanResult
	Records []crashRecord
}

// TestProcessCrashPreservesParentObservedACK uses the actual OS durability
// implementation. The parent observes a receipt, SIGKILLs its live writer,
// then starts an independent reader process. This tests process failure; it
// deliberately makes no claim to emulate a power cut or device loss.
func TestProcessCrashPreservesParentObservedACK(t *testing.T) {
	if mode := os.Getenv("GIGAQUIZZ_FILELOG_CHILD"); mode != "" {
		b, err := base64.StdEncoding.DecodeString(os.Getenv("GIGAQUIZZ_FILELOG_CONFIG"))
		if err != nil {
			t.Fatal(err)
		}
		var c Config
		if err = json.Unmarshal(b, &c); err != nil {
			t.Fatal(err)
		}
		if mode == "write" {
			s, err := New(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			a := testInput(1, 1)
			b := a
			b.Token[15] = 2
			b.Choice = 2
			r, err := s.SubmitFrame(context.Background(), []Input{a, b, {Token: a.Token, Choice: 2}})
			if err != nil {
				t.Fatal(err)
			}
			if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
				t.Fatal(err)
			}
			time.Sleep(30 * time.Second)
			t.Fatal("parent did not SIGKILL child")
		} else if mode == "read" {
			var out crashRead
			r, err := Scan(context.Background(), c, func(p Position, v Vote) error { out.Records = append(out.Records, crashRecord{p, v}); return nil })
			if err != nil {
				t.Fatal(err)
			}
			out.Scan = r
			if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
				t.Fatal(err)
			}
		} else {
			t.Fatalf("unknown child mode %q", mode)
		}
		return
	}
	c := testConfig(t)
	config, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	child := func(mode string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessCrashPreservesParentObservedACK$", "-test.timeout=25s")
		cmd.Env = append(os.Environ(), "GIGAQUIZZ_FILELOG_CHILD="+mode, "GIGAQUIZZ_FILELOG_CONFIG="+base64.StdEncoding.EncodeToString(config))
		return cmd
	}
	writer := child("write")
	stdout, err := writer.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	writer.Stderr = os.Stderr
	if err = writer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Process.Kill() })
	// The parent keeps the actual successful response, not an inference from
	// bytes later found in the journal.
	var acknowledged FrameReceipt
	if err = json.NewDecoder(bufio.NewReader(stdout)).Decode(&acknowledged); err != nil {
		_ = writer.Process.Kill()
		_ = writer.Wait()
		t.Fatal(err)
	}
	if acknowledged.Count != 3 || acknowledged.Offset != 0 {
		t.Fatalf("unexpected ACK: %+v", acknowledged)
	}
	if err = writer.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = writer.Wait(); err == nil {
		t.Fatal("writer exited normally instead of SIGKILL")
	}
	output, err := child("read").Output()
	if err != nil {
		t.Fatalf("independent reader: %v: %s", err, output)
	}
	var recovered crashRead
	if err = json.Unmarshal(firstJSONLine(output), &recovered); err != nil {
		t.Fatalf("reader output: %v: %s", err, output)
	}
	if recovered.Scan.Closed || recovered.Scan.IncompleteTail || recovered.Scan.Votes != uint64(acknowledged.Count) || len(recovered.Records) != 3 {
		t.Fatalf("lost parent-observed ACK: %+v", recovered)
	}
	for i, record := range recovered.Records {
		if record.Position.Offset != acknowledged.Offset || record.Position.Index != uint32(i) || !record.Vote.AdmittedAt.Equal(acknowledged.AdmittedAt) {
			t.Fatalf("receipt mismatch: %+v / %+v", acknowledged, record)
		}
	}
	a, b := recovered.Records[0].Vote, recovered.Records[1].Vote
	if a.Token == b.Token || a.Token[0] != 1 || b.Token[15] != 2 || a.Choice != 1 || b.Choice != 2 || recovered.Records[2].Vote.Token != a.Token || recovered.Records[2].Vote.Choice != 2 {
		t.Fatal("full IDs or choices changed after crash")
	}
	t.Logf("parent ACK retained through SIGKILL and independent reader: %s, frame=%d entries=%d", SyncMode(), acknowledged.Offset, acknowledged.Count)
}

func firstJSONLine(b []byte) []byte {
	for i, c := range b {
		if c == '\n' {
			return b[:i]
		}
	}
	return []byte(fmt.Sprintf("%s", b))
}
