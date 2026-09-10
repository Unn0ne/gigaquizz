package filelog

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"
)

type appendReady struct {
	Ready   bool `json:"ready"`
	Written int  `json:"written"`
}

// A second append really reaches the OS before READY. The parent then kills
// the writer, without letting that frame complete or receive a sync/ACK.
// Process failure leaves the OS alive; this does not simulate power failure.
func TestProcessCrashDuringAppendPreservesACKPrefix(t *testing.T) {
	if mode := os.Getenv("GIGAQUIZZ_FILELOG_APPEND_CHILD"); mode != "" {
		config, err := base64.StdEncoding.DecodeString(os.Getenv("GIGAQUIZZ_FILELOG_CONFIG"))
		if err != nil {
			t.Fatal(err)
		}
		var c Config
		if err := json.Unmarshal(config, &c); err != nil {
			t.Fatal(err)
		}
		if mode == "read" {
			var recovered crashRead
			r, err := Scan(context.Background(), c, func(p Position, v Vote) error {
				recovered.Records = append(recovered.Records, crashRecord{p, v})
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			recovered.Scan = r
			if _, err := Replay(context.Background(), c, nil); err == nil {
				t.Fatal("strict Replay accepted an unsealed partial journal")
			}
			if err := json.NewEncoder(os.Stdout).Encode(recovered); err != nil {
				t.Fatal(err)
			}
			return
		}
		partial := 17
		if mode == "partial_payload" {
			partial = frameHeaderBytes + 7
		} else if mode != "partial_header" {
			t.Fatalf("unknown child mode %q", mode)
		}
		s, err := New(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		first := []Input{testInput(1, 1), testInput(2, 2)}
		r, err := s.SubmitFrame(context.Background(), first)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
			t.Fatal(err)
		}
		// The previous job has completed before this hook is installed. It
		// writes to the real file and blocks before the remaining write/sync.
		s.write = func(b []byte) (int, error) {
			if len(b) <= partial {
				return 0, errors.New("second frame unexpectedly small")
			}
			if err := writeAll(s.f.Write, b[:partial]); err != nil {
				return 0, err
			}
			if err := json.NewEncoder(os.Stdout).Encode(appendReady{Ready: true, Written: partial}); err != nil {
				return partial, err
			}
			time.Sleep(30 * time.Second)
			return partial, errors.New("parent did not SIGKILL during append")
		}
		if _, err := s.SubmitFrame(context.Background(), []Input{testInput(3, 1)}); err != nil {
			t.Fatal(err)
		}
		t.Fatal("unfinished second frame was acknowledged")
		return
	}

	for _, stage := range []string{"partial_header", "partial_payload"} {
		t.Run(stage, func(t *testing.T) {
			c := testConfig(t)
			config, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			child := func(mode string) *exec.Cmd {
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessCrashDuringAppendPreservesACKPrefix$", "-test.timeout=25s")
				cmd.Env = append(os.Environ(), "GIGAQUIZZ_FILELOG_APPEND_CHILD="+mode, "GIGAQUIZZ_FILELOG_CONFIG="+base64.StdEncoding.EncodeToString(config))
				return cmd
			}
			writer := child(stage)
			stdout, err := writer.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			writer.Stderr = os.Stderr
			if err := writer.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = writer.Process.Kill() })
			decoder := json.NewDecoder(bufio.NewReader(stdout))
			var acknowledged FrameReceipt
			if err := decoder.Decode(&acknowledged); err != nil {
				_ = writer.Process.Kill()
				_ = writer.Wait()
				t.Fatal(err)
			}
			if acknowledged.Offset != 0 || acknowledged.Count != 2 {
				t.Fatalf("unexpected first ACK: %+v", acknowledged)
			}
			var ready appendReady
			if err := decoder.Decode(&ready); err != nil {
				_ = writer.Process.Kill()
				_ = writer.Wait()
				t.Fatal(err)
			}
			expectedPartial := 17
			if stage == "partial_payload" {
				expectedPartial = frameHeaderBytes + 7
			}
			if !ready.Ready || ready.Written != expectedPartial {
				t.Fatalf("second append did not reach OS: %+v", ready)
			}
			if err := writer.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := writer.Wait(); err == nil {
				t.Fatal("writer exited normally instead of SIGKILL")
			}
			output, err := child("read").Output()
			if err != nil {
				t.Fatalf("independent reader: %v: %s", err, output)
			}
			var recovered crashRead
			if err := json.Unmarshal(firstJSONLine(output), &recovered); err != nil {
				t.Fatalf("reader output: %v: %s", err, output)
			}
			if !recovered.Scan.IncompleteTail || recovered.Scan.Closed || recovered.Scan.Votes != 2 || recovered.Scan.Frames != 1 || len(recovered.Records) != 2 || recovered.Scan.ValidBytes != fileHeaderBytes+frameHeaderBytes+2*entryBytes {
				t.Fatalf("ACK prefix or incomplete tail changed: %+v", recovered)
			}
			for i, record := range recovered.Records {
				want := testInput(byte(i+1), uint32(i+1))
				if record.Position != (Position{Offset: acknowledged.Offset, Index: uint32(i)}) || record.Vote.Token != want.Token || record.Vote.Choice != want.Choice || !record.Vote.AdmittedAt.Equal(acknowledged.AdmittedAt) {
					t.Fatalf("parent-observed ACK mismatch: %+v / %+v", acknowledged, record)
				}
			}
			t.Logf("parent ACK preserved after SIGKILL during %s (%d real bytes), independent reader reports incomplete tail", stage, ready.Written)
		})
	}
}
