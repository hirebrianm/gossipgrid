// Package store: real-SQLite tests for binding provenance (Milestone 5B,
// Stage 3A). These tests verify that the provenance column is persisted
// atomically with the binding, survives store reopening, is immutable
// through ordinary bind operations, and fails closed when malformed.
package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"gossipgrid/internal/lifecycle"
)

func TestBindingProvenance_PersistedInStore(t *testing.T) {
	t.Run("join provenance survives restart", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.db")

		st, err := New(path)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		b, err := st.Bind(context.Background(), "c1", "e1", 1, lifecycle.ProvenanceJoin)
		if err != nil {
			t.Fatalf("bind join: %v", err)
		}
		if b.Provenance != lifecycle.ProvenanceJoin {
			t.Fatalf("expected join provenance; got %s", b.Provenance)
		}
		_ = st.Close()

		// Reopen (simulating process restart).
		st2, err := New(path)
		if err != nil {
			t.Fatalf("reopen store: %v", err)
		}
		defer func() {
			if cerr := st2.Close(); cerr != nil {
				t.Errorf("close store after restart: %v", cerr)
			}
		}()

		b2, err := st2.GetBinding()
		if err != nil {
			t.Fatalf("GetBinding after restart: %v", err)
		}
		if b2 == nil {
			t.Fatal("binding lost after restart")
		}
		if b2.Provenance != lifecycle.ProvenanceJoin {
			t.Fatalf("provenance lost after restart: expected join, got %s", b2.Provenance)
		}
		if b2.ClusterID != "c1" || b2.EpochID != "e1" {
			t.Fatalf("binding values changed after restart: got %s/%s", b2.ClusterID, b2.EpochID)
		}
	})

	t.Run("genesis provenance survives restart", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.db")

		st, err := New(path)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		b, err := st.Bind(context.Background(), "c1", "e1", 1, lifecycle.ProvenanceGenesis)
		if err != nil {
			t.Fatalf("bind genesis: %v", err)
		}
		if b.Provenance != lifecycle.ProvenanceGenesis {
			t.Fatalf("expected genesis provenance; got %s", b.Provenance)
		}
		_ = st.Close()

		st2, err := New(path)
		if err != nil {
			t.Fatalf("reopen store: %v", err)
		}
		defer func() {
			if cerr := st2.Close(); cerr != nil {
				t.Errorf("close store after restart: %v", cerr)
			}
		}()

		b2, err := st2.GetBinding()
		if err != nil {
			t.Fatalf("GetBinding after restart: %v", err)
		}
		if b2 == nil {
			t.Fatal("binding lost after restart")
		}
		if b2.Provenance != lifecycle.ProvenanceGenesis {
			t.Fatalf("provenance lost after restart: expected genesis, got %s", b2.Provenance)
		}
	})
}

func TestBindingProvenance_JoinNotRelabeled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	st, err := New(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()

	// Bind with join provenance.
	_, err = st.Bind(context.Background(), "c1", "e1", 1, lifecycle.ProvenanceJoin)
	if err != nil {
		t.Fatalf("first bind: %v", err)
	}

	// Attempt to rebind with genesis provenance (same cluster/epoch —
	// idempotent path). The existing binding must be returned unchanged.
	b, err := st.Bind(context.Background(), "c1", "e1", 1, lifecycle.ProvenanceGenesis)
	if err != nil {
		t.Fatalf("rebind (idempotent): %v", err)
	}
	if b.Provenance != lifecycle.ProvenanceJoin {
		t.Fatalf("CRITICAL: provenance was relabeled from join to %s; it must be immutable", b.Provenance)
	}
}

func TestBindingProvenance_ConflictingEpochCannotReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	st, err := New(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()

	_, err = st.Bind(context.Background(), "c1", "e1", 1, lifecycle.ProvenanceJoin)
	if err != nil {
		t.Fatalf("first bind: %v", err)
	}

	// A different epoch must fail with ErrBindingMismatch.
	_, err = st.Bind(context.Background(), "c1", "e2", 2, lifecycle.ProvenanceJoin)
	if err == nil {
		t.Fatal("expected binding mismatch for different epoch; got nil")
	}
	if !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("expected ErrBindingMismatch; got %v", err)
	}

	// The original binding is preserved.
	b, err := st.GetBinding()
	if err != nil {
		t.Fatalf("GetBinding: %v", err)
	}
	if b.EpochID != "e1" {
		t.Fatalf("original binding was overwritten: epoch=%s", b.EpochID)
	}
	if b.Provenance != lifecycle.ProvenanceJoin {
		t.Fatalf("original provenance was changed: got %s", b.Provenance)
	}
}

func TestBindingProvenance_MalformedProvenanceFailsClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	st, err := New(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()

	// Insert a binding row with an invalid provenance value (99).
	if err := st.db.Exec(
		"INSERT INTO local_metadata (id, cluster_id, epoch_id, generation, created_at, provenance) VALUES (1, 'c1', 'e1', 1, datetime('now'), 99)",
	).Error; err != nil {
		t.Fatalf("inserting malformed binding: %v", err)
	}

	_, err = st.GetBinding()
	if err == nil {
		t.Fatal("expected ErrBindingMalformed for invalid provenance value; got nil")
	}
	if !errors.Is(err, ErrBindingMalformed) {
		t.Fatalf("expected ErrBindingMalformed; got %v", err)
	}
}

func TestBindingProvenance_SameEpochBindingIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	st, err := New(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()

	// Bind with join provenance.
	b1, err := st.Bind(context.Background(), "c1", "e1", 1, lifecycle.ProvenanceJoin)
	if err != nil {
		t.Fatalf("first bind: %v", err)
	}

	// Rebind with the same values but different generation: idempotent.
	b2, err := st.Bind(context.Background(), "c1", "e1", 2, lifecycle.ProvenanceJoin)
	if err != nil {
		t.Fatalf("rebind same epoch: %v", err)
	}
	if b2.CreatedAt != b1.CreatedAt {
		t.Fatal("binding was rewritten (CreatedAt changed)")
	}
	if b2.Provenance != lifecycle.ProvenanceJoin {
		t.Fatalf("provenance changed: got %s", b2.Provenance)
	}
}

func TestBindingProvenance_ConcurrentBindNoConflictingSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	st, err := New(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()

	// Concurrent binds to the same epoch: all should succeed (idempotent).
	var wg sync.WaitGroup
	const n = 10
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := st.Bind(context.Background(), "c1", "e1", 1, lifecycle.ProvenanceJoin)
			results <- err
		}()
	}
	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Fatalf("concurrent bind error: %v", err)
		}
	}

	// Verify only one binding exists and it has join provenance.
	b, err := st.GetBinding()
	if err != nil {
		t.Fatalf("GetBinding: %v", err)
	}
	if b.Provenance != lifecycle.ProvenanceJoin {
		t.Fatalf("expected join provenance; got %s", b.Provenance)
	}
}
