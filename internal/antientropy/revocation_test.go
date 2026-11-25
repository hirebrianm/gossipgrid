package antientropy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/peertls"
	"gossipgrid/internal/store"
)

// barrierLocalStore is a fake LocalStore that blocks in FindMissingIDs until
// the test releases it via a channel. This allows the test to revoke
// authorization while the store operation is in progress, then verify the
// cursor is NOT advanced.
type barrierLocalStore struct {
	mu       sync.Mutex
	ids      map[string]bool
	blockCh  chan struct{}
	inserted []string
}

func newBarrierLocalStore() *barrierLocalStore {
	return &barrierLocalStore{
		ids:     make(map[string]bool),
		blockCh: make(chan struct{}),
	}
}

func (s *barrierLocalStore) FindMissingIDs(ctx context.Context, ids []string) ([]string, error) {
	// Block until the test releases us, or context is canceled.
	select {
	case <-s.blockCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var missing []string
	for _, id := range ids {
		if !s.ids[id] {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

func (s *barrierLocalStore) InsertReplicatedMessageReturningInserted(msg *store.Message) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids[msg.ID] {
		return false, store.ErrDuplicateIDConflict
	}
	s.ids[msg.ID] = true
	s.inserted = append(s.inserted, msg.ID)
	return true, nil
}

// InsertReplicatedMessagesBounded implements the LocalStore interface for the
// barrier test. It blocks until the test releases blockCh (same as
// FindMissingIDs), then installs the batch: existing IDs are AlreadyPresent,
// new IDs are Inserted.
func (s *barrierLocalStore) InsertReplicatedMessagesBounded(ctx context.Context, msgs []*store.Message, _ func() time.Time) (store.BootstrapInsertResult, error) {
	// Block until the test releases us, or context is canceled.
	select {
	case <-s.blockCh:
	case <-ctx.Done():
		var res store.BootstrapInsertResult
		return res, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var res store.BootstrapInsertResult
	for _, m := range msgs {
		if s.ids[m.ID] {
			res.AlreadyPresent = append(res.AlreadyPresent, m.ID)
			continue
		}
		s.ids[m.ID] = true
		s.inserted = append(s.inserted, m.ID)
		res.Inserted = append(res.Inserted, m.ID)
	}
	return res, nil
}

// blockingMessageFetcher wraps a MessageFetcher and blocks calls after the
// first N. This allows the test to revoke authorization mid-fetch-sequence.
type blockingMessageFetcher struct {
	blockAfter int
	blockCh    chan struct{}
	fetcher    *fakeMessageFetcher
	calls      int
	mu         sync.Mutex
}

func (f *blockingMessageFetcher) FetchMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string) ([]peertls.SyncMessageWire, error) {
	f.mu.Lock()
	f.calls++
	callNum := f.calls
	f.mu.Unlock()
	if callNum > f.blockAfter {
		select {
		case <-f.blockCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return f.fetcher.FetchMessages(ctx, dialAddr, expectedIdentity, ids)
}

// FetchBootstrapMessages is the bootstrap-fetch variant of FetchMessages
// (Milestone 5B, Stage 2). The test fake blocks the same way; the request
// purpose header is untrusted and does not change the serving-side decision in
// the fake.
func (f *blockingMessageFetcher) FetchBootstrapMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string, _ *lifecycle.RequestContext) ([]peertls.SyncMessageWire, error) {
	return f.FetchMessages(ctx, dialAddr, expectedIdentity, ids)
}

// newRevocationTestEngine creates an engine with the given page setup and a
// controllable admission guard, ready for a direct syncPeer call.
func newRevocationTestEngine(t *testing.T, cfg Config, invClient InventoryClient, msgFetcher MessageFetcher, localStore LocalStore) (*Engine, *bool, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	var authorized = true
	admissionCheck := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if !authorized {
			return errors.New("lifecycle lockout: not authorized")
		}
		return nil
	}
	engine := New(cfg, &fakePeerSource{valid: true}, invClient, msgFetcher, localStore, nil)
	engine.SetAdmissionCheck(admissionCheck)
	return engine, &authorized, &mu
}

// TestRevocationDuringFindMissingNonFinalPage verifies that when authorization
// is revoked while FindMissingIDs is blocked on a non-final page, the cursor
// is NOT advanced.
func TestRevocationDuringFindMissingNonFinalPage(t *testing.T) {
	ids := genUUIDs(0, 6)
	invClient := &fakeInventoryClient{
		inv: map[string][]string{"gossipgrid-1": ids},
	}
	localStore := newBarrierLocalStore()
	msgFetcher := &fakeMessageFetcher{msgs: make(map[string]peertls.SyncMessageWire)}

	cfg := Config{
		Interval:        time.Second,
		PageSize:        3,
		FetchBatchSize:  2,
		MaxPagesPerPeer: 2,
		Workers:         1,
		RequestTimeout:  5 * time.Second,
	}

	engine, authorized, mu := newRevocationTestEngine(t, cfg, invClient, msgFetcher, localStore)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-1", PodIdentity: "gossipgrid-1"}
	ctx := context.Background()

	// Run syncPeer in a goroutine. It will block in FindMissingIDs.
	done := make(chan struct{})
	go func() {
		engine.syncPeer(ctx, target)
		close(done)
	}()

	// Wait for the store to be blocked (the engine calls FindMissingIDs
	// after the inventory page is fetched). We give it a short delay.
	time.Sleep(50 * time.Millisecond)

	// Revoke authorization.
	mu.Lock()
	*authorized = false
	mu.Unlock()

	// Release the blocked FindMissingIDs.
	close(localStore.blockCh)

	// Wait for syncPeer to complete.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("syncPeer did not complete within 5s")
	}

	// Verify: cursor must NOT have advanced.
	stats := engine.Stats()
	peerProg, ok := stats.PerPeer["gossipgrid-1"]
	if !ok {
		t.Fatalf("expected per-peer progress for gossipgrid-1, got keys: %v", stats.PerPeer)
	}
	if peerProg.LastCursor != "" {
		t.Errorf("cursor should be empty (not advanced) after revocation, got %q", peerProg.LastCursor)
	}
	if stats.CompletedPasses != 0 {
		t.Errorf("CompletedPasses should be 0 after revocation, got %d", stats.CompletedPasses)
	}
	if stats.FailedPasses == 0 {
		t.Error("FailedPasses should be > 0 after revocation")
	}
}

// TestRevocationDuringFindMissingFinalPage verifies that when authorization
// is revoked while FindMissingIDs is blocked on the final page (HasMore=false),
// the cursor is NOT reset and CompletedPasses is NOT incremented.
func TestRevocationDuringFindMissingFinalPage(t *testing.T) {
	ids := genUUIDs(0, 3)
	// Only 3 IDs with page size 3: one page, HasMore=false (final page).
	invClient := &fakeInventoryClient{
		inv: map[string][]string{"gossipgrid-1": ids},
	}
	localStore := newBarrierLocalStore()
	msgFetcher := &fakeMessageFetcher{msgs: make(map[string]peertls.SyncMessageWire)}

	cfg := Config{
		Interval:        time.Second,
		PageSize:        3,
		FetchBatchSize:  2,
		MaxPagesPerPeer: 2,
		Workers:         1,
		RequestTimeout:  5 * time.Second,
	}

	engine, authorized, mu := newRevocationTestEngine(t, cfg, invClient, msgFetcher, localStore)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-1", PodIdentity: "gossipgrid-1"}
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		engine.syncPeer(ctx, target)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	// Revoke authorization.
	mu.Lock()
	*authorized = false
	mu.Unlock()

	// Release the blocked FindMissingIDs.
	close(localStore.blockCh)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("syncPeer did not complete within 5s")
	}

	// Verify: CompletedPasses must NOT be incremented, LastSuccess must NOT be set.
	stats := engine.Stats()
	if stats.CompletedPasses != 0 {
		t.Errorf("CompletedPasses should be 0 after revocation on final page, got %d", stats.CompletedPasses)
	}
	if !stats.LastSuccess.IsZero() {
		t.Errorf("LastSuccess should be zero after revocation on final page, got %v", stats.LastSuccess)
	}
	if stats.FailedPasses == 0 {
		t.Error("FailedPasses should be > 0 after revocation")
	}
}

// TestRevocationDuringMessageFetch verifies that when authorization is revoked
// during a message-fetch or insert sequence (before page completion), the
// cursor is NOT advanced.
func TestRevocationDuringMessageFetch(t *testing.T) {
	ids := genUUIDs(0, 3)
	// 3 IDs, page size 3: one page with HasMore=false.
	// All 3 IDs are missing locally, so fetchAndInsert is called.
	// FetchBatchSize=1 forces 3 fetch calls. Block after the first.
	invClient := &fakeInventoryClient{
		inv: map[string][]string{"gossipgrid-1": ids},
	}
	localStore := &fakeLocalStore{ids: make(map[string]bool)}
	fetcher := &fakeMessageFetcher{msgs: make(map[string]peertls.SyncMessageWire)}
	blockingFetcher := &blockingMessageFetcher{
		blockAfter: 1,
		blockCh:    make(chan struct{}),
		fetcher:    fetcher,
	}

	cfg := Config{
		Interval:        time.Second,
		PageSize:        3,
		FetchBatchSize:  1,
		MaxPagesPerPeer: 2,
		Workers:         1,
		RequestTimeout:  5 * time.Second,
	}

	engine, authorized, mu := newRevocationTestEngine(t, cfg, invClient, blockingFetcher, localStore)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-1", PodIdentity: "gossipgrid-1"}
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		engine.syncPeer(ctx, target)
		close(done)
	}()

	// Wait for the first fetch to complete and the second to block.
	time.Sleep(50 * time.Millisecond)

	// Revoke authorization.
	mu.Lock()
	*authorized = false
	mu.Unlock()

	// Release the blocked fetch.
	close(blockingFetcher.blockCh)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("syncPeer did not complete within 5s")
	}

	// Verify: cursor must NOT have advanced.
	stats := engine.Stats()
	peerProg, ok := stats.PerPeer["gossipgrid-1"]
	if !ok {
		t.Fatalf("expected per-peer progress for gossipgrid-1, got keys: %v", stats.PerPeer)
	}
	if peerProg.LastCursor != "" {
		t.Errorf("cursor should be empty (not advanced) after revocation during fetch, got %q", peerProg.LastCursor)
	}
	if stats.CompletedPasses != 0 {
		t.Errorf("CompletedPasses should be 0 after revocation, got %d", stats.CompletedPasses)
	}
}

// TestNoCursorAdvanceAfterRevocationOnFinalPage verifies that when
// authorization is revoked between page processing and the cursor commit
// on a final page, the sweep is NOT counted as completed.
func TestNoCursorAdvanceAfterRevocationOnFinalPage(t *testing.T) {
	ids := genUUIDs(0, 2)
	// 2 IDs, page size 2: one page, HasMore=false.
	invClient := &fakeInventoryClient{
		inv: map[string][]string{"gossipgrid-1": ids},
	}
	localStore := &fakeLocalStore{ids: make(map[string]bool)}
	fetcher := &fakeMessageFetcher{msgs: make(map[string]peertls.SyncMessageWire)}

	cfg := Config{
		Interval:        time.Second,
		PageSize:        2,
		FetchBatchSize:  2,
		MaxPagesPerPeer: 2,
		Workers:         1,
		RequestTimeout:  5 * time.Second,
	}

	engine, authorized, mu := newRevocationTestEngine(t, cfg, invClient, fetcher, localStore)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-1", PodIdentity: "gossipgrid-1"}
	ctx := context.Background()

	// Revoke authorization BEFORE syncPeer runs. The admission check at
	// pass entry will reject, so no page is processed at all.
	mu.Lock()
	*authorized = false
	mu.Unlock()

	engine.syncPeer(ctx, target)

	stats := engine.Stats()
	if stats.CompletedPasses != 0 {
		t.Errorf("CompletedPasses should be 0 when authorization is revoked before pass entry, got %d", stats.CompletedPasses)
	}
	if stats.InventoryPages != 0 {
		t.Errorf("InventoryPages should be 0 when authorization is revoked before pass entry, got %d", stats.InventoryPages)
	}
}
