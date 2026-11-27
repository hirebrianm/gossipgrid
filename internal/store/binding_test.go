package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	st, err := New(path)
	if err != nil {
		t.Fatalf("opening test store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestFreshEmptyDatabaseBinding(t *testing.T) {
	st := openTestStore(t)

	b, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis)
	if err != nil {
		t.Fatalf("binding fresh empty database: %v", err)
	}
	if b.ClusterID != "cluster-1" || b.EpochID != "epoch-1" {
		t.Errorf("binding: expected cluster-1/epoch-1, got %s/%s", b.ClusterID, b.EpochID)
	}
	if b.Generation != 1 {
		t.Errorf("binding generation: expected 1, got %d", b.Generation)
	}
	if b.CreatedAt.IsZero() {
		t.Error("binding created_at is zero")
	}
}

func TestIdempotentRebinding(t *testing.T) {
	st := openTestStore(t)

	b1, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis)
	if err != nil {
		t.Fatalf("first bind: %v", err)
	}

	b2, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 2, lifecycle.ProvenanceGenesis)
	if err != nil {
		t.Fatalf("rebind same values: %v", err)
	}
	if b2.ClusterID != b1.ClusterID || b2.EpochID != b1.EpochID {
		t.Error("rebind: binding values changed")
	}
	// The created_at should be preserved from the first bind.
	if !b2.CreatedAt.Equal(b1.CreatedAt) {
		t.Errorf("rebind: created_at changed: %v vs %v", b2.CreatedAt, b1.CreatedAt)
	}
}

func TestBindingMismatchSameClusterDifferentEpoch(t *testing.T) {
	st := openTestStore(t)

	if _, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis); err != nil {
		t.Fatalf("first bind: %v", err)
	}

	_, err := st.Bind(context.Background(), "cluster-1", "epoch-2", 2, lifecycle.ProvenanceGenesis)
	if err == nil {
		t.Fatal("expected binding mismatch error, got nil")
	}
	if !errors.Is(err, ErrBindingMismatch) {
		t.Errorf("expected ErrBindingMismatch, got %v", err)
	}
}

func TestBindingMismatchDifferentCluster(t *testing.T) {
	st := openTestStore(t)

	if _, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis); err != nil {
		t.Fatalf("first bind: %v", err)
	}

	_, err := st.Bind(context.Background(), "cluster-2", "epoch-1", 1, lifecycle.ProvenanceGenesis)
	if err == nil {
		t.Fatal("expected binding mismatch error, got nil")
	}
	if !errors.Is(err, ErrBindingMismatch) {
		t.Errorf("expected ErrBindingMismatch, got %v", err)
	}
}

func TestNonemptyLegacyDatabaseWithoutBinding(t *testing.T) {
	st := openTestStore(t)

	// Insert a message (making the database nonempty).
	if _, err := st.CreateLocalMessage("node-1", "hello"); err != nil {
		t.Fatalf("inserting message: %v", err)
	}

	// Attempt to bind a nonempty database without an existing binding.
	_, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis)
	if err == nil {
		t.Fatal("expected ErrBindingNonempty, got nil")
	}
	if !errors.Is(err, ErrBindingNonempty) {
		t.Errorf("expected ErrBindingNonempty, got %v", err)
	}
}

func TestGetBindingNoBinding(t *testing.T) {
	st := openTestStore(t)

	b, err := st.GetBinding()
	if err != nil {
		t.Fatalf("GetBinding on fresh database: %v", err)
	}
	if b != nil {
		t.Errorf("expected nil binding, got %+v", b)
	}
}

func TestGetBindingAfterBind(t *testing.T) {
	st := openTestStore(t)

	if _, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	b, err := st.GetBinding()
	if err != nil {
		t.Fatalf("GetBinding: %v", err)
	}
	if b == nil {
		t.Fatal("expected non-nil binding")
	}
	if b.ClusterID != "cluster-1" || b.EpochID != "epoch-1" {
		t.Errorf("binding: expected cluster-1/epoch-1, got %s/%s", b.ClusterID, b.EpochID)
	}
}

func TestConcurrentBindingAttempts(t *testing.T) {
	st := openTestStore(t)

	var wg sync.WaitGroup
	const goroutines = 10
	results := make(chan *lifecycle.DatabaseBinding, goroutines)
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: %w", i, err)
			} else {
				results <- b
			}
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)

	var gotErr error
	for err := range errs {
		gotErr = errors.Join(gotErr, err)
	}
	if gotErr != nil {
		t.Fatalf("concurrent bind errors: %v", gotErr)
	}

	var bindings []*lifecycle.DatabaseBinding
	for b := range results {
		bindings = append(bindings, b)
	}
	if len(bindings) != goroutines {
		t.Fatalf("expected %d binding results, got %d", goroutines, len(bindings))
	}
	// All bindings should have the same values.
	for _, b := range bindings {
		if b.ClusterID != "cluster-1" || b.EpochID != "epoch-1" {
			t.Errorf("binding values: expected cluster-1/epoch-1, got %s/%s", b.ClusterID, b.EpochID)
		}
	}
}

func TestRestartWithCorrectBinding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// First open: bind the database.
	st1, err := New(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	b1, err := st1.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	_ = st1.Close()

	// Second open (simulating restart): the binding should be preserved.
	st2, err := New(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer func() { _ = st2.Close() }()

	b2, err := st2.GetBinding()
	if err != nil {
		t.Fatalf("GetBinding after restart: %v", err)
	}
	if b2 == nil {
		t.Fatal("expected non-nil binding after restart")
	}
	if b2.ClusterID != b1.ClusterID || b2.EpochID != b1.EpochID {
		t.Errorf("binding after restart: expected %s/%s, got %s/%s",
			b1.ClusterID, b1.EpochID, b2.ClusterID, b2.EpochID)
	}
	// Idempotent rebind should succeed.
	_, err = st2.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis)
	if err != nil {
		t.Errorf("rebind after restart: %v", err)
	}
}

func TestRestartWithConflictingEpoch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// First open: bind to epoch A.
	st1, err := New(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, err := st1.Bind(context.Background(), "cluster-1", "epoch-A", 1, lifecycle.ProvenanceGenesis); err != nil {
		t.Fatalf("bind epoch A: %v", err)
	}
	_ = st1.Close()

	// Second open: attempt to bind to epoch B (conflicting).
	st2, err := New(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer func() { _ = st2.Close() }()

	_, err = st2.Bind(context.Background(), "cluster-1", "epoch-B", 2, lifecycle.ProvenanceGenesis)
	if err == nil {
		t.Fatal("expected binding mismatch on conflicting epoch, got nil")
	}
	if !errors.Is(err, ErrBindingMismatch) {
		t.Errorf("expected ErrBindingMismatch, got %v", err)
	}
	// The original binding should be preserved.
	b, err := st2.GetBinding()
	if err != nil {
		t.Fatalf("GetBinding: %v", err)
	}
	if b.EpochID != "epoch-A" {
		t.Errorf("original binding changed: expected epoch-A, got %s", b.EpochID)
	}
}

func TestMalformedBindingRow(t *testing.T) {
	st := openTestStore(t)

	// Insert a malformed binding row directly.
	if err := st.db.Exec("INSERT INTO local_metadata (id, cluster_id, epoch_id, generation, created_at) VALUES (1, '', '', 0, ?)",
		time.Now().UTC()).Error; err != nil {
		t.Fatalf("inserting malformed binding: %v", err)
	}

	_, err := st.GetBinding()
	if err == nil {
		t.Fatal("expected ErrBindingMalformed, got nil")
	}
	if !errors.Is(err, ErrBindingMalformed) {
		t.Errorf("expected ErrBindingMalformed, got %v", err)
	}
}

func TestCountMessages(t *testing.T) {
	st := openTestStore(t)

	count, err := st.CountMessages()
	if err != nil {
		t.Fatalf("CountMessages on empty db: %v", err)
	}
	if count != 0 {
		t.Errorf("empty db: expected 0 messages, got %d", count)
	}

	if _, err := st.CreateLocalMessage("node-1", "hello"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := st.CreateLocalMessage("node-1", "world"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	count, err = st.CountMessages()
	if err != nil {
		t.Fatalf("CountMessages: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 messages, got %d", count)
	}
}

func TestWALAndImmutableInsertsPreserved(t *testing.T) {
	st := openTestStore(t)

	// Verify that the existing message insert path still works
	// alongside the binding table.
	if _, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis); err != nil {
		t.Fatalf("bind: %v", err)
	}

	msg, err := st.CreateLocalMessage("node-1", "test")
	if err != nil {
		t.Fatalf("CreateLocalMessage: %v", err)
	}
	if msg.ID == "" {
		t.Error("message ID is empty")
	}

	// Idempotent replicated insert.
	if err := st.InsertReplicatedMessage(&Message{
		ID:        msg.ID,
		Body:      "test",
		Origin:    "node-1",
		CreatedAt: msg.CreatedAt,
	}); err != nil {
		t.Fatalf("InsertReplicatedMessage (duplicate): %v", err)
	}

	// The binding should still be intact.
	b, err := st.GetBinding()
	if err != nil {
		t.Fatalf("GetBinding: %v", err)
	}
	if b.ClusterID != "cluster-1" || b.EpochID != "epoch-1" {
		t.Error("binding changed after message inserts")
	}
}

// TestBinding_ConflictingConcurrentBinds_CompetingEpochs verifies that two
// concurrent Bind calls with different epoch values (same cluster) result in
// one winner and the other receiving ErrBindingMismatch. The loser must NOT
// receive a successful result containing the competing epoch.
func TestBinding_ConflictingConcurrentBinds_CompetingEpochs(t *testing.T) {
	st := openTestStore(t)

	type bindResult struct {
		b   *lifecycle.DatabaseBinding
		err error
	}
	results := make(chan bindResult, 2)
	start := make(chan struct{})

	go func() {
		<-start
		b, err := st.Bind(context.Background(), "cluster-1", "epoch-A", 1, lifecycle.ProvenanceGenesis)
		results <- bindResult{b, err}
	}()
	go func() {
		<-start
		b, err := st.Bind(context.Background(), "cluster-1", "epoch-B", 2, lifecycle.ProvenanceGenesis)
		results <- bindResult{b, err}
	}()
	close(start)

	r1 := <-results
	r2 := <-results

	var successes, errs int
	var firstErr error
	for _, r := range []bindResult{r1, r2} {
		if r.err != nil {
			errs++
			firstErr = r.err
		} else {
			successes++
		}
	}

	if successes != 1 || errs != 1 {
		t.Fatalf("expected 1 success and 1 error; got %d success, %d error (err=%v)", successes, errs, firstErr)
	}
	if !errors.Is(firstErr, ErrBindingMismatch) {
		t.Fatalf("expected ErrBindingMismatch for loser; got %v", firstErr)
	}

	db, err := st.GetBinding()
	if err != nil {
		t.Fatalf("GetBinding: %v", err)
	}
	if db == nil {
		t.Fatal("expected a binding in the database; got nil")
	}
	if db.EpochID != "epoch-A" && db.EpochID != "epoch-B" {
		t.Fatalf("unexpected epoch in DB: %s", db.EpochID)
	}
}

// TestBinding_ConflictingConcurrentBinds_CompetingProvenance verifies that two
// concurrent Bind calls with the same cluster/epoch but different provenance
// values result in both succeeding (idempotent for same cluster/epoch), and
// the database preserves the winner's provenance (no relabel).
func TestBinding_ConflictingConcurrentBinds_CompetingProvenance(t *testing.T) {
	st := openTestStore(t)

	type bindResult struct {
		b   *lifecycle.DatabaseBinding
		err error
	}
	results := make(chan bindResult, 2)
	start := make(chan struct{})

	go func() {
		<-start
		b, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceGenesis)
		results <- bindResult{b, err}
	}()
	go func() {
		<-start
		b, err := st.Bind(context.Background(), "cluster-1", "epoch-1", 1, lifecycle.ProvenanceJoin)
		results <- bindResult{b, err}
	}()
	close(start)

	r1 := <-results
	r2 := <-results

	// Both should succeed (same cluster/epoch is idempotent).
	if r1.err != nil || r2.err != nil {
		t.Fatalf("expected both to succeed (idempotent); got r1.err=%v r2.err=%v", r1.err, r2.err)
	}

	// The database must have exactly one binding with the winner's provenance.
	db, err := st.GetBinding()
	if err != nil {
		t.Fatalf("GetBinding: %v", err)
	}
	if db == nil {
		t.Fatal("expected a binding in the database; got nil")
	}
	if db.Provenance != lifecycle.ProvenanceGenesis && db.Provenance != lifecycle.ProvenanceJoin {
		t.Fatalf("unexpected provenance in DB: %s", db.Provenance)
	}

	// Both returned bindings must have the same provenance as the DB.
	if r1.b != nil && r1.b.Provenance != db.Provenance {
		t.Fatalf("r1 provenance %s != DB provenance %s", r1.b.Provenance, db.Provenance)
	}
	if r2.b != nil && r2.b.Provenance != db.Provenance {
		t.Fatalf("r2 provenance %s != DB provenance %s", r2.b.Provenance, db.Provenance)
	}
}
