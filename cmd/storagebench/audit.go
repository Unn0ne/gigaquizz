package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gigaquizz/internal/postgres"
	"github.com/jackc/pgx/v5"
)

type keyLedger struct {
	Version    int        `json:"version"`
	Schema     string     `json:"schema"`
	PollID     string     `json:"poll_id"`
	Mode       string     `json:"mode"`
	PollEndsAt time.Time  `json:"poll_ends_at"`
	Keys       []keyState `json:"keys"`
}

const maximumLedgerBytes = 128 * 1024 * 1024

func validSchema(name string) bool {
	if len(name) != 24 || !strings.HasPrefix(name, "gqbench_") {
		return false
	}
	_, err := hex.DecodeString(name[8:])
	return err == nil && strings.ToLower(name) == name
}
func validateLedger(l keyLedger) error {
	if l.Version != 1 || !validSchema(l.Schema) || len(l.Keys) < 1 || len(l.Keys) > maximumKeys || (l.Mode != "vote" && l.Mode != "blind") {
		return errors.New("invalid benchmark ledger")
	}
	if len(l.PollID) != 36 || l.PollID[8] != '-' || l.PollID[13] != '-' || l.PollID[18] != '-' || l.PollID[23] != '-' {
		return errors.New("invalid ledger poll")
	}
	if _, err := hex.DecodeString(strings.ReplaceAll(l.PollID, "-", "")); err != nil {
		return errors.New("invalid ledger poll")
	}
	seen := make(map[string]bool, len(l.Keys))
	for _, k := range l.Keys {
		if len(k.Token) != 32 || strings.ToLower(k.Token) != k.Token || seen[k.Token] || k.SentChoices > 3 || k.ConfirmedChoices > 3 || k.ReceiptChoices > 3 || k.Accepted < 0 || k.Accepted > 2 {
			return errors.New("invalid ledger key state")
		}
		if _, err := hex.DecodeString(k.Token); err != nil {
			return errors.New("invalid ledger key")
		}
		seen[k.Token] = true
	}
	return nil
}
func saveLedger(path string, l keyLedger) (string, error) {
	if err := validateLedger(l); err != nil {
		return "", err
	}
	if path == "" {
		path = filepath.Join(".local", "storagebench", l.Schema+".json")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(path)
		}
	}()
	if err = json.NewEncoder(f).Encode(l); err != nil {
		return "", err
	}
	if err = f.Sync(); err != nil {
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", err
	}
	ok = true
	return path, nil
}
func readLedger(path string) (keyLedger, error) {
	var l keyLedger
	f, err := os.Open(path)
	if err != nil {
		return l, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return l, err
	}
	if !stat.Mode().IsRegular() || stat.Size() > maximumLedgerBytes {
		return l, errors.New("ledger must be a regular file of at most 128 MiB")
	}
	d := json.NewDecoder(io.LimitReader(f, maximumLedgerBytes))
	d.DisallowUnknownFields()
	if err = d.Decode(&l); err != nil {
		return l, err
	}
	if err = d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return l, errors.New("ledger contains trailing data")
	}
	return l, validateLedger(l)
}

type auditReport struct {
	Stable               bool          `json:"poll_finalized_barrier_visible"`
	Correct              bool          `json:"per_key_correct"`
	Rows                 int64         `json:"unique_database_rows"`
	FinalResultRows      int64         `json:"final_result_total"`
	FinalChoiceCounts    map[int]int64 `json:"final_choice_counts"`
	ScannedRows          int           `json:"rows_scanned_bounded"`
	UnexpectedRows       int64         `json:"unexpected_keys"`
	UnsentRows           int           `json:"rows_for_never_dispatched_keys"`
	ContentMismatches    int           `json:"key_content_mismatches"`
	ConfirmedMissing     int           `json:"client_confirmed_keys_absent"`
	SentAbsent           int           `json:"dispatched_keys_absent"`
	UnconfirmedPersisted int           `json:"persisted_keys_without_client_confirmation"`
	UnknownPersisted     int           `json:"unknown_unconfirmed_keys_found"`
	UnknownAbsent        int           `json:"unknown_unconfirmed_keys_absent_after_barrier"`
	MultipleAccepted     int           `json:"keys_with_multiple_accepted_responses"`
	AggregateMatches     bool          `json:"final_aggregate_matches_rows_and_choices"`
	Method               string        `json:"method"`
}

func reconcile(ctx context.Context, conn *pgx.Conn, l keyLedger) (*auditReport, error) {
	a := &auditReport{FinalChoiceCounts: map[int]int64{}, Method: "Read-only repeatable-read snapshot after the real close/finalization barrier; compare every bounded submitted key and receipt choice against rows, then compare final aggregate totals and option counts. Unknown absence is not loss of a confirmed vote."}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if err = tx.QueryRow(ctx, `SELECT finalized_at IS NOT NULL FROM polls WHERE id=$1::uuid`, l.PollID).Scan(&a.Stable); err != nil {
		return nil, err
	}
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM votes WHERE poll_id=$1::uuid`, l.PollID).Scan(&a.Rows); err != nil {
		return nil, err
	}
	index := make(map[string]int, len(l.Keys))
	for i, k := range l.Keys {
		index[k.Token] = i
	}
	found := make([]bool, len(l.Keys))
	counts := map[int]int64{}
	rows, err := tx.Query(ctx, `SELECT token::text,choices FROM votes WHERE poll_id=$1::uuid LIMIT $2`, l.PollID, len(l.Keys)+1)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var token string
		var choices []int32
		if err = rows.Scan(&token, &choices); err != nil {
			rows.Close()
			return nil, err
		}
		a.ScannedRows++
		for _, choice := range choices {
			counts[int(choice)]++
		}
		i, ok := index[strings.ReplaceAll(token, "-", "")]
		if !ok {
			a.UnexpectedRows++
			continue
		}
		if found[i] {
			a.ContentMismatches++
		}
		found[i] = true
		k := l.Keys[i]
		if k.SentChoices == 0 {
			a.UnsentRows++
		}
		var mask uint8
		if len(choices) == 1 && choices[0] >= 1 && choices[0] <= 2 {
			mask = 1 << uint(choices[0]-1)
		}
		if mask == 0 || k.SentChoices&mask == 0 || (k.ConfirmedChoices != 0 && k.ConfirmedChoices != mask) || (k.ReceiptChoices != 0 && k.ReceiptChoices != mask) {
			a.ContentMismatches++
		}
		if k.ConfirmedChoices == 0 {
			a.UnconfirmedPersisted++
			if k.Unknown {
				a.UnknownPersisted++
			}
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if a.Rows > int64(a.ScannedRows) {
		a.UnexpectedRows += a.Rows - int64(a.ScannedRows)
	}
	for i, k := range l.Keys {
		if k.Accepted > 1 {
			a.MultipleAccepted++
		}
		if !found[i] {
			if k.SentChoices != 0 {
				a.SentAbsent++
			}
			if k.ConfirmedChoices != 0 {
				a.ConfirmedMissing++
			}
			if k.Unknown && k.ConfirmedChoices == 0 {
				a.UnknownAbsent++
			}
		}
	}
	if a.Stable {
		var options []byte
		if err = tx.QueryRow(ctx, `SELECT total_votes,options FROM poll_results WHERE poll_id=$1::uuid`, l.PollID).Scan(&a.FinalResultRows, &options); err != nil {
			return nil, err
		}
		var result []struct {
			ID    int   `json:"id"`
			Votes int64 `json:"votes"`
		}
		if err = json.Unmarshal(options, &result); err != nil {
			return nil, err
		}
		a.AggregateMatches = a.Rows == a.FinalResultRows && int64(a.ScannedRows) == a.Rows
		for _, option := range result {
			a.FinalChoiceCounts[option.ID] = option.Votes
			if counts[option.ID] != option.Votes {
				a.AggregateMatches = false
			}
			delete(counts, option.ID)
		}
		for _, n := range counts {
			if n != 0 {
				a.AggregateMatches = false
			}
		}
	}
	a.Correct = a.Stable && a.UnexpectedRows == 0 && a.UnsentRows == 0 && a.ContentMismatches == 0 && a.ConfirmedMissing == 0 && a.MultipleAccepted == 0 && a.AggregateMatches
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return a, nil
}

func auditOnly(parent context.Context, c config, dsn string) (r report) {
	start := time.Now()
	defer func() { r.WallSeconds = time.Since(start).Seconds() }()
	r.Mode = "audit-only"
	r.Errors = []string{}
	r.Cleanup = "not_requested_read_only"
	l, err := readLedger(c.auditOnly)
	if err != nil {
		r.Errors = append(r.Errors, "invalid_or_unreadable_private_ledger")
		return
	}
	r.Schema = l.Schema
	r.PollID = l.PollID
	r.KeyLedgerFile = c.auditOnly
	r.Configuration = map[string]any{"required_standbys": c.requiredStandbys, "standby_names": c.standbyNames, "read_only_audit": true}
	pinned, err := pinSchema(dsn, l.Schema)
	if err != nil {
		r.Errors = append(r.Errors, "database_configuration_invalid")
		return
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	conn, err := openConnection(ctx, pinned)
	if err != nil {
		r.Errors = append(r.Errors, "audit_connection_failed_details_suppressed")
		return
	}
	defer conn.Close(context.Background())
	var proof *postgres.Store
	if c.requiredStandbys > 0 {
		options := postgres.DurabilityOptions{RequiredStandbys: c.requiredStandbys}
		for _, name := range strings.Split(c.standbyNames, ",") {
			options.StandbyNames = append(options.StandbyNames, strings.TrimSpace(name))
		}
		proof, err = postgres.NewWithOptions(ctx, pinned, 1, options)
		if err != nil {
			r.Errors = append(r.Errors, "audit_durability_configuration_or_primary_unavailable")
			return
		}
		defer proof.Close()
	}
	r.Database, err = readDatabaseInfo(ctx, conn)
	if err != nil {
		r.Errors = append(r.Errors, "database_settings_unavailable")
	}
	r.Audit, err = reconcile(ctx, conn, l)
	if err != nil {
		r.Errors = append(r.Errors, "per_key_reconciliation_unavailable")
	} else if !r.Audit.Stable || !r.Audit.Correct {
		r.Errors = append(r.Errors, "reconciliation_not_stable_or_correct")
	}
	if proof != nil && r.Audit != nil && r.Audit.Stable {
		if err = proveFinalAudit(ctx, proof, l.PollID, r.Audit); err != nil {
			r.Errors = append(r.Errors, "audit_replication_durability_not_established")
		} else {
			r.DurabilityChecked = true
		}
	}
	r.Limitations = []string{"Read-only audit of retained artificial keys; does not elect a leader, fence a primary, finalize a poll or certify a failure-zone model.", "Without a positive explicit standby policy this audits only the observed copy. An explicit policy requires a fixed direct primary endpoint and verifies repository Results plus its WAL proof after the per-key snapshot."}
	return
}

func proveFinalAudit(ctx context.Context, store *postgres.Store, pollID string, audit *auditReport) error {
	result, err := store.Results(ctx, pollID)
	if err != nil {
		return err
	}
	if result.State != "final" || result.TotalVotes != audit.Rows || len(result.Options) != len(audit.FinalChoiceCounts) {
		return errors.New("audit result changed")
	}
	for _, option := range result.Options {
		if audit.FinalChoiceCounts[option.ID] != option.Votes {
			return errors.New("audit choices changed")
		}
	}
	return store.WaitDurable(ctx)
}
