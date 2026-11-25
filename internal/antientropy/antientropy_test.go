package antientropy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/peertls"
	"gossipgrid/internal/store"
)

// --- Test helpers ---

// genUUIDs generates n sequential UUIDv7 strings (deterministic, ascending).
// They are not real time-based UUIDs but valid canonical UUIDv7 format.
// The offset parameter shifts the base to produce distinct ranges.
func genUUIDs(offset, n int) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		v := offset + i
		b := make([]byte, 16)
		b[0] = 0x01
		b[1] = 0x9a
		b[6] = 0x70 // version 7
		b[8] = 0x80 // variant
		// Encode v into the last 8 bytes.
		for j := 0; j < 8; j++ {
			b[15-j] = byte(v >> (j * 8))
		}
		u := uuid.UUID(b)
		ids[i] = u.String()
	}
	return ids
}

// fakePeerSource implements PeerSource with a fixed peer list, expected
// identities, and a mutable membership-validity flag (defaulting to true /
// authoritative).
type fakePeerSource struct {
	mu       sync.Mutex
	peers    []PeerTarget
	expected []string
	valid    bool
}

// Snapshot implements PeerSource: returns the membership snapshot from a
// single consistent view.
func (f *fakePeerSource) Snapshot() MembershipSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return MembershipSnapshot{
		Peers:              f.peers,
		ExpectedIdentities: f.expected,
		Authoritative:      f.valid,
	}
}

func (f *fakePeerSource) setPeers(peers []PeerTarget) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.peers = peers
}

func (f *fakePeerSource) setExpected(ids []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expected = ids
}

func (f *fakePeerSource) setValid(valid bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.valid = valid
}

// fakeInventoryClient implements InventoryClient.
type fakeInventoryClient struct {
	mu    sync.Mutex
	inv   map[string][]string // peer key -> all IDs
	err   error
	calls int
}

func (f *fakeInventoryClient) FetchInventoryPage(ctx context.Context, dialAddr, expectedIdentity, after string, limit int) (*peertls.InventoryPageResponse, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	all := f.inv[expectedIdentity]
	// Keyset pagination: return IDs after cursor, up to limit.
	startIdx := 0
	if after != "" {
		for i, id := range all {
			if id == after {
				startIdx = i + 1
				break
			}
		}
	}
	remaining := all[startIdx:]
	if len(remaining) == 0 {
		return &peertls.InventoryPageResponse{IDs: []string{}, HasMore: false}, nil
	}
	pageLen := limit
	if pageLen > len(remaining) {
		pageLen = len(remaining)
	}
	page := remaining[:pageLen]
	resp := &peertls.InventoryPageResponse{
		IDs:     page,
		HasMore: len(remaining) > limit,
	}
	if resp.HasMore {
		resp.NextCursor = page[len(page)-1]
	}
	// When HasMore is false (final page), NextCursor is empty per the
	// protocol contract. The engine resets its local cursor to detect
	// a completed full sweep.
	return resp, nil
}

// FetchBootstrapInventoryPage is the bootstrap-fetch variant of
// FetchInventoryPage (Milestone 5B, Stage 2). The test fake serves the same
// data; the request-purpose header is untrusted and does not change the
// serving-side decision in the fake.
func (f *fakeInventoryClient) FetchBootstrapInventoryPage(ctx context.Context, dialAddr, expectedIdentity, after string, limit int, _ *lifecycle.RequestContext) (*peertls.InventoryPageResponse, error) {
	return f.FetchInventoryPage(ctx, dialAddr, expectedIdentity, after, limit)
}

// fakeMessageFetcher implements MessageFetcher.
type fakeMessageFetcher struct {
	mu    sync.Mutex
	msgs  map[string]peertls.SyncMessageWire // ID -> message
	err   error
	calls int
}

func (f *fakeMessageFetcher) FetchMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string) ([]peertls.SyncMessageWire, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	var result []peertls.SyncMessageWire
	for _, id := range ids {
		m, ok := f.msgs[id]
		if !ok {
			return nil, fmt.Errorf("message %q not found on peer", id)
		}
		result = append(result, m)
	}
	return result, nil
}

// FetchBootstrapMessages is the bootstrap-fetch variant of FetchMessages
// (Milestone 5B, Stage 2). The test fake serves the same data; the request
// purpose header is untrusted and does not change the serving-side decision in
// the fake.
func (f *fakeMessageFetcher) FetchBootstrapMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string, _ *lifecycle.RequestContext) ([]peertls.SyncMessageWire, error) {
	return f.FetchMessages(ctx, dialAddr, expectedIdentity, ids)
}

// fakeLocalStore implements LocalStore.
type fakeLocalStore struct {
	mu       sync.Mutex
	ids      map[string]bool // local message IDs
	inserted []string        // IDs that were actually inserted (new)
	err      error
}

func (f *fakeLocalStore) FindMissingIDs(ctx context.Context, ids []string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	var missing []string
	for _, id := range ids {
		if !f.ids[id] {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

func (f *fakeLocalStore) InsertReplicatedMessageReturningInserted(msg *store.Message) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return false, f.err
	}
	if f.ids[msg.ID] {
		return false, store.ErrDuplicateIDConflict
	}
	f.ids[msg.ID] = true
	f.inserted = append(f.inserted, msg.ID)
	return true, nil
}

// InsertReplicatedMessagesBounded implements the LocalStore interface for the
// deterministic bootstrap tests. It mirrors the store's transactional batch
// install: a new row is created (Inserted), an identical duplicate is a no-op
// (AlreadyPresent), a conflicting duplicate is a Conflict (the batch fails),
// and an expired record (createdAt < threshold) is skipped (Expired). The
// expirationThreshold callback is invoked to obtain the threshold (nil =
// disabled).
func (f *fakeLocalStore) InsertReplicatedMessagesBounded(_ context.Context, msgs []*store.Message, expirationThreshold func() time.Time) (store.BootstrapInsertResult, error) {
	var res store.BootstrapInsertResult
	var threshold time.Time
	if expirationThreshold != nil {
		threshold = expirationThreshold()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return res, f.err
	}
	for _, msg := range msgs {
		// Retention: skip expired records (requester-side threshold).
		if !threshold.IsZero() && msg.CreatedAt.Before(threshold) {
			res.Expired = append(res.Expired, msg.ID)
			continue
		}
		if f.ids[msg.ID] {
			// A conflicting duplicate (the fake has no content comparison;
			// any pre-existing ID is treated as a conflict to exercise the
			// conflict path deterministically).
			res.Conflict = append(res.Conflict, msg.ID)
			return res, nil
		}
		f.ids[msg.ID] = true
		f.inserted = append(f.inserted, msg.ID)
		res.Inserted = append(res.Inserted, msg.ID)
	}
	return res, nil
}

func (f *fakeLocalStore) addLocal(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids[id] = true
}

// --- Config validation tests ---

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"valid", Config{Interval: 30 * time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 5 * time.Second}, false},
		{"zero interval", Config{Interval: 0, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 5 * time.Second}, true},
		{"interval too small", Config{Interval: 100 * time.Millisecond, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 5 * time.Second}, true},
		{"interval too large", Config{Interval: 11 * time.Minute, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 5 * time.Second}, true},
		{"page size zero", Config{Interval: 30 * time.Second, PageSize: 0, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 5 * time.Second}, true},
		{"page size too large", Config{Interval: 30 * time.Second, PageSize: 2048, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 5 * time.Second}, true},
		{"batch too large", Config{Interval: 30 * time.Second, PageSize: 128, FetchBatchSize: 2048, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 5 * time.Second}, true},
		{"batch exceeds 8", Config{Interval: 30 * time.Second, PageSize: 128, FetchBatchSize: 9, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 5 * time.Second}, true},
		{"batch exceeds page", Config{Interval: 30 * time.Second, PageSize: 4, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 5 * time.Second}, true},
		{"max pages zero", Config{Interval: 30 * time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 0, Workers: 2, RequestTimeout: 5 * time.Second}, true},
		{"max pages too large", Config{Interval: 30 * time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 128, Workers: 2, RequestTimeout: 5 * time.Second}, true},
		{"workers zero", Config{Interval: 30 * time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 0, RequestTimeout: 5 * time.Second}, true},
		{"workers too many", Config{Interval: 30 * time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 32, RequestTimeout: 5 * time.Second}, true},
		{"zero timeout", Config{Interval: 30 * time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 0}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// --- Engine lifecycle tests ---

func TestEngineStartStop(t *testing.T) {
	cfg := Config{Interval: time.Second, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 2, Workers: 1, RequestTimeout: time.Second}
	src := &fakePeerSource{}
	inv := &fakeInventoryClient{inv: map[string][]string{}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}

	e := New(cfg, src, inv, fetcher, local, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Repeated Start fails.
	if err := e.Start(context.Background()); err == nil {
		t.Fatal("second Start should fail")
	}
	// Stop is safe.
	e.Stop(context.Background())
	// Repeated Stop is safe.
	e.Stop(context.Background())
	// Start after Stop fails.
	if err := e.Start(context.Background()); err == nil {
		t.Fatal("Start after Stop should fail")
	}
}

func TestEngineStopBeforeStart(t *testing.T) {
	cfg := Config{Interval: time.Second, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 2, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, &fakePeerSource{}, &fakeInventoryClient{inv: map[string][]string{}}, &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}, &fakeLocalStore{ids: map[string]bool{}}, nil)
	e.Stop(context.Background()) // safe, no-op
}

func TestEngineConcurrentStop(t *testing.T) {
	cfg := Config{Interval: time.Second, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 2, Workers: 2, RequestTimeout: time.Second}
	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{"gossipgrid-1.suffix": genUUIDs(0, 5)}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}

	e := New(cfg, src, inv, fetcher, local, nil)
	//nolint:errcheck // test: Start failure is not expected and would be caught by the Stop assertion below.
	e.Start(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Stop(context.Background())
		}()
	}
	wg.Wait()
}

// --- Synchronization logic tests ---

// TestSyncRepairMissingRecords: local store is missing records that exist on
// the peer. After a sync cycle, the missing records are fetched and inserted.
func TestSyncRepairMissingRecords(t *testing.T) {
	ids := genUUIDs(0, 5)
	peerKey := "gossipgrid-1.suffix"

	// Peer has all 5 IDs; local has only the first 2.
	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})

	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	// Set up messages for all IDs on the peer.
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "body-" + id[:8], Origin: "gossipgrid-1", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}

	// Local has first 2 IDs.
	local := &fakeLocalStore{ids: map[string]bool{}}
	local.addLocal(ids[0])
	local.addLocal(ids[1])

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	// Call syncPeer directly (no need to start the full engine).
	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	// Verify: local should now have all 5 IDs.
	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.ids) != 5 {
		t.Errorf("expected 5 local IDs after sync, got %d", len(local.ids))
	}
	for i := 2; i < 5; i++ {
		if !local.ids[ids[i]] {
			t.Errorf("expected local to have ID %s after sync", ids[i])
		}
	}
}

// TestSyncIdenticalInventories: both sides have the same IDs; no inserts occur.
func TestSyncIdenticalInventories(t *testing.T) {
	ids := genUUIDs(0, 3)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}
	for _, id := range ids {
		local.addLocal(id)
	}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	// No new inserts should have occurred.
	if len(local.inserted) != 0 {
		t.Errorf("expected 0 inserts, got %d", len(local.inserted))
	}
}

// TestSyncEmptyInventories: both sides empty; no error, no inserts.
func TestSyncEmptyInventories(t *testing.T) {
	peerKey := "gossipgrid-1.suffix"
	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: nil}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	if len(local.inserted) != 0 {
		t.Errorf("expected 0 inserts, got %d", len(local.inserted))
	}
}

// TestSyncPagination: peer has more IDs than one page; cursor advances across pages.
func TestSyncPagination(t *testing.T) {
	ids := genUUIDs(0, 25) // 25 IDs, page size 10 → 3 pages
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	// All 25 IDs should be in local.
	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.ids) != 25 {
		t.Errorf("expected 25 local IDs after full sweep, got %d", len(local.ids))
	}
}

// TestSyncCursorNotAdvancedOnFetchError: when the fetch fails, the cursor
// is NOT advanced (safe retry next cycle).
func TestSyncCursorNotAdvancedOnFetchError(t *testing.T) {
	ids := genUUIDs(0, 5)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}, err: errors.New("network error")}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	// The cursor should NOT have advanced past the first page (fetch failed).
	e.mu.Lock()
	cursor := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor == nil {
		t.Fatal("expected cursor to exist")
	}
	// After a failed fetch on the first page (has_more=false since only 5 IDs),
	// the cursor should still be "" because the page processing failed.
	if cursor.cursor != "" {
		t.Errorf("expected cursor to be empty after fetch failure, got %q", cursor.cursor)
	}
}

// TestSyncConflictPreservesLocal: a conflicting ID is never overwritten, and
// the conflict fails the page (cursor not advanced, pass not completed).
// This simulates a race: the ID is reported as missing by FindMissingIDs
// (because it was inserted concurrently after the inventory was fetched),
// but the insert hits a conflict because the local row has different content.
func TestSyncConflictPreservesLocal(t *testing.T) {
	ids := genUUIDs(0, 3)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "remote-body", Origin: "gossipgrid-1", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}

	// Use a conflict-injecting store: FindMissingIDs reports all as missing,
	// but InsertReplicatedMessageReturningInserted returns a conflict for ids[0].
	local := &conflictStore{
		ids:        map[string]bool{},
		inserted:   nil,
		conflictID: ids[0],
	}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	st := e.Stats()
	// The conflict must be recorded.
	if st.Conflicts == 0 {
		t.Error("expected at least 1 conflict, got 0")
	}
	// The conflict must fail the pass: FailedPasses incremented.
	if st.FailedPasses == 0 {
		t.Error("expected failed pass to be recorded when a conflict occurs")
	}
	// The conflict must NOT be reported as a successful completed pass.
	if st.CompletedPasses != 0 {
		t.Errorf("expected 0 completed passes (conflict failed the page), got %d", st.CompletedPasses)
	}
	// The cursor must NOT have advanced past the failed page (it stays "").
	e.mu.Lock()
	cursor := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor == nil {
		t.Fatal("expected cursor to exist")
	}
	if cursor.cursor != "" {
		t.Errorf("expected cursor to remain empty after conflict, got %q", cursor.cursor)
	}
}

// conflictStore simulates a race where FindMissingIDs reports an ID as missing
// but the insert hits a conflict (the ID was inserted concurrently with
// different content).
type conflictStore struct {
	mu         sync.Mutex
	ids        map[string]bool
	inserted   []string
	conflictID string
}

func (c *conflictStore) FindMissingIDs(_ context.Context, ids []string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Report all IDs as missing (simulates the race).
	return ids, nil
}

func (c *conflictStore) InsertReplicatedMessageReturningInserted(msg *store.Message) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if msg.ID == c.conflictID {
		// Simulate: the row already exists with different content.
		c.ids[msg.ID] = true
		return false, store.ErrDuplicateIDConflict
	}
	c.ids[msg.ID] = true
	c.inserted = append(c.inserted, msg.ID)
	return true, nil
}

// InsertReplicatedMessagesBounded implements the LocalStore interface for the
// conflict test. It delegates per-record to the single-insert semantics: a
// conflict ID produces a Conflict (the batch is rolled back); a duplicate ID
// already in the store is AlreadyPresent; otherwise Inserted.
func (c *conflictStore) InsertReplicatedMessagesBounded(_ context.Context, msgs []*store.Message, _ func() time.Time) (store.BootstrapInsertResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var res store.BootstrapInsertResult
	for _, m := range msgs {
		if m.ID == c.conflictID {
			c.ids[m.ID] = true
			res.Conflict = append(res.Conflict, m.ID)
			return res, nil
		}
		if c.ids[m.ID] {
			res.AlreadyPresent = append(res.AlreadyPresent, m.ID)
			continue
		}
		c.ids[m.ID] = true
		c.inserted = append(c.inserted, m.ID)
		res.Inserted = append(res.Inserted, m.ID)
	}
	return res, nil
}

// TestSyncMultipleBatches: missing records exceed one fetch batch.
func TestSyncMultipleBatches(t *testing.T) {
	ids := genUUIDs(0, 12) // 12 missing IDs, batch size 5 → 3 batches
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 16, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.ids) != 12 {
		t.Errorf("expected 12 local IDs, got %d", len(local.ids))
	}
	// Fetcher should have been called 3 times (ceil(12/5) = 3 batches).
	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()
	if fetcher.calls != 3 {
		t.Errorf("expected 3 fetch calls, got %d", fetcher.calls)
	}
}

// TestSyncUnavailablePeer: a peer that returns an error does not crash the engine.
func TestSyncUnavailablePeer(t *testing.T) {
	peerKey := "gossipgrid-1.suffix"
	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{}, err: errors.New("connection refused")}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	// No data loss, no panic. The failed pass is recorded.
	st := e.Stats()
	if st.FailedPasses == 0 {
		t.Error("expected failed pass to be recorded")
	}
}

// TestSyncFullSweepResetsCursor: when a full sweep completes (has_more=false),
// the cursor resets to empty for the next cycle.
func TestSyncFullSweepResetsCursor(t *testing.T) {
	ids := genUUIDs(0, 3)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	// After a full sweep, the cursor should be reset to "".
	e.mu.Lock()
	cursor := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor == nil {
		t.Fatal("expected cursor to exist")
	}
	if cursor.cursor != "" {
		t.Errorf("expected cursor to be reset to empty after full sweep, got %q", cursor.cursor)
	}
	if !cursor.sweepDone {
		t.Error("expected sweepDone to be true after full sweep")
	}
}

// TestSyncMaxPagesBudget: the engine stops after MaxPagesPerPeer pages even if
// the peer has more.
func TestSyncMaxPagesBudget(t *testing.T) {
	ids := genUUIDs(0, 50) // 50 IDs, page size 10 → 5 pages, but max is 2
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 2, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	// Only 2 pages × 10 = 20 IDs should be processed.
	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.ids) != 20 {
		t.Errorf("expected 20 local IDs (2 pages of 10), got %d", len(local.ids))
	}
	// The cursor should point to the 20th ID (not the end).
	e.mu.Lock()
	cursor := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor.cursor != ids[19] {
		t.Errorf("expected cursor to be at ID[19], got %q", cursor.cursor)
	}
}

// TestSyncLateLowerIDFoundOnNextSweep: after a full sweep resets the cursor,
// a new ID that sorts before the old cursor position is found on the next sweep.
func TestSyncLateLowerIDFoundOnNextSweep(t *testing.T) {
	ids := genUUIDs(0, 5)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)
	ctx := context.Background()

	// First sweep: sync all 5 IDs.
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})
	// Verify all 5 are local.
	local.mu.Lock()
	if len(local.ids) != 5 {
		local.mu.Unlock()
		t.Fatalf("expected 5 local IDs after first sweep, got %d", len(local.ids))
	}
	local.mu.Unlock()

	// Now simulate a "late" ID being inserted on the peer that sorts before
	// the old cursor position. Since our cursor was reset to "" after the full
	// sweep, the next sweep starts from the beginning and will find it.
	// (In this test the cursor is already reset, so the next sweep sees all IDs.)
	// The key property: cursor was reset, so no ID is permanently skipped.

	// Remove one local ID to simulate it being deleted (or a new peer ID appearing).
	local.mu.Lock()
	delete(local.ids, ids[1])
	local.mu.Unlock()

	// Second sweep: should find and re-insert ids[1].
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	local.mu.Lock()
	defer local.mu.Unlock()
	if !local.ids[ids[1]] {
		t.Error("expected ID[1] to be re-inserted on second sweep")
	}
}

// TestSyncIdempotentReconciliation: running the same sync twice produces no
// additional inserts the second time.
func TestSyncIdempotentReconciliation(t *testing.T) {
	ids := genUUIDs(0, 3)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)
	ctx := context.Background()
	target := PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}

	// First sync.
	e.syncPeer(ctx, target)
	local.mu.Lock()
	firstInserts := len(local.inserted)
	local.mu.Unlock()
	if firstInserts != 3 {
		t.Fatalf("expected 3 inserts on first sync, got %d", firstInserts)
	}

	// Second sync (cursor was reset after full sweep).
	e.syncPeer(ctx, target)
	local.mu.Lock()
	defer local.mu.Unlock()
	// No new inserts (all already present).
	if len(local.inserted) != 3 {
		t.Errorf("expected no additional inserts on second sync, got %d total", len(local.inserted))
	}
}

// --- PeerSource tests ---

func TestDiscoveryPeerSourceExcludesSelf(t *testing.T) {
	src := NewDiscoveryPeerSource("test.svc", func() PeerSourceSnapshot {
		return PeerSourceSnapshot{
			Peers: []PeerSourceInfo{
				{Hostname: "gossipgrid-0", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0:8443"},
				{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1:8443"},
			},
			LocalNode:          &PeerSourceInfo{Hostname: "gossipgrid-0", PodIdentity: "gossipgrid-0"},
			ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
			Mode:               "static",
		}
	})
	snap := src.Snapshot()
	if !snap.Authoritative {
		t.Error("expected authoritative=true for static mode")
	}
	if len(snap.Peers) != 1 {
		t.Fatalf("expected 1 peer (self excluded), got %d", len(snap.Peers))
	}
	if snap.Peers[0].PodIdentity != "gossipgrid-1" {
		t.Errorf("expected gossipgrid-1, got %s", snap.Peers[0].PodIdentity)
	}
	if snap.Peers[0].ExpectedIdentity != "gossipgrid-1.test.svc" {
		t.Errorf("expected identity gossipgrid-1.test.svc, got %s", snap.Peers[0].ExpectedIdentity)
	}
	// Expected identities should be passed through.
	if len(snap.ExpectedIdentities) != 2 {
		t.Errorf("expected 2 expected identities, got %d", len(snap.ExpectedIdentities))
	}
}

func TestDiscoveryPeerSourceStaleDNS(t *testing.T) {
	src := NewDiscoveryPeerSource("test.svc", func() PeerSourceSnapshot {
		return PeerSourceSnapshot{
			Peers: []PeerSourceInfo{
				{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1:8443"},
			},
			ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
			Stale:              true,
			Mode:               "dns",
		}
	})
	snap := src.Snapshot()
	if snap.Authoritative {
		t.Error("expected authoritative=false for stale DNS")
	}
	if len(snap.Peers) != 0 {
		t.Errorf("expected 0 peers when DNS stale, got %d", len(snap.Peers))
	}
	// Expected identities are still carried through (independent of DNS state).
	if len(snap.ExpectedIdentities) != 2 {
		t.Errorf("expected 2 expected identities even when stale, got %d", len(snap.ExpectedIdentities))
	}
}

func TestDiscoveryPeerSourceStaticNotStale(t *testing.T) {
	src := NewDiscoveryPeerSource("test.svc", func() PeerSourceSnapshot {
		return PeerSourceSnapshot{
			Peers: []PeerSourceInfo{
				{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1:8443"},
			},
			ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
			Stale:              true, // stale flag set, but mode is static
			Mode:               "static",
		}
	})
	snap := src.Snapshot()
	if !snap.Authoritative {
		t.Error("expected authoritative=true for static mode (stale irrelevant)")
	}
	if len(snap.Peers) != 1 {
		t.Errorf("expected 1 peer for static mode (stale irrelevant), got %d", len(snap.Peers))
	}
}

// --- Concurrency test ---

func TestSyncConcurrentWorkers(t *testing.T) {
	peerKey1 := "gossipgrid-1.suffix"
	peerKey2 := "gossipgrid-2.suffix"
	ids1 := genUUIDs(0, 5)
	ids2 := genUUIDs(100, 5)

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{
		{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey1, PodIdentity: "gossipgrid-1"},
		{DialAddr: "127.0.0.1:8444", ExpectedIdentity: peerKey2, PodIdentity: "gossipgrid-2"},
	})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey1: ids1, peerKey2: ids2}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range append(ids1, ids2...) {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	// Run syncPeer concurrently for both peers.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey1, PodIdentity: "gossipgrid-1"})
	}()
	go func() {
		defer wg.Done()
		e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8444", ExpectedIdentity: peerKey2, PodIdentity: "gossipgrid-2"})
	}()
	wg.Wait()

	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.ids) != 10 {
		t.Errorf("expected 10 local IDs after concurrent sync, got %d", len(local.ids))
	}
}

// --- Stats test ---

func TestStatsAfterSync(t *testing.T) {
	ids := genUUIDs(0, 4)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	st := e.Stats()
	if st.CompletedPasses == 0 {
		t.Error("expected completed passes > 0")
	}
	if st.InventoryPages == 0 {
		t.Error("expected inventory pages > 0")
	}
	if st.IDsInspected != 4 {
		t.Errorf("expected 4 IDs inspected, got %d", st.IDsInspected)
	}
	if st.MissingIDsFound != 4 {
		t.Errorf("expected 4 missing IDs, got %d", st.MissingIDsFound)
	}
	if st.NewInserts != 4 {
		t.Errorf("expected 4 new inserts, got %d", st.NewInserts)
	}
	if st.LastSuccess.IsZero() {
		t.Error("expected last success to be set")
	}
}

// --- Cursor state bounding tests ---

// TestCursorReconciliationRemovesLeftPeers verifies that cursor entries for
// peers that have left membership are removed (authoritative membership),
// while entries for peers still in membership are preserved.
func TestCursorReconciliationRemovesLeftPeers(t *testing.T) {
	peerKey1 := "gossipgrid-1.suffix"
	peerKey2 := "gossipgrid-2.suffix"

	src := &fakePeerSource{}
	inv := &fakeInventoryClient{inv: map[string][]string{}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	// Set up cursor entries for both peers (simulating in-progress sweeps).
	e.mu.Lock()
	e.cursors["gossipgrid-1"] = &peerCursor{cursor: "id-a", lastSuccess: time.Now()}
	e.cursors["gossipgrid-2"] = &peerCursor{cursor: "id-b", lastSuccess: time.Now()}
	e.mu.Unlock()

	// Both peers are in the expected membership: reconcile should preserve both.
	src.setValid(true)
	src.setPeers([]PeerTarget{
		{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey1, PodIdentity: "gossipgrid-1"},
		{DialAddr: "127.0.0.1:8444", ExpectedIdentity: peerKey2, PodIdentity: "gossipgrid-2"},
	})
	src.setExpected([]string{"gossipgrid-1", "gossipgrid-2"})
	snap := src.Snapshot()
	e.reconcileCursors(&snap)

	e.mu.Lock()
	_, has1 := e.cursors["gossipgrid-1"]
	_, has2 := e.cursors["gossipgrid-2"]
	e.mu.Unlock()
	if !has1 || !has2 {
		t.Fatal("expected both cursor entries to be preserved when both peers are in membership")
	}

	// Peer 2 is removed from the expected membership (StatefulSet scaled down):
	// reconcile should remove peer 2's cursor and preserve peer 1's.
	src.setPeers([]PeerTarget{
		{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey1, PodIdentity: "gossipgrid-1"},
	})
	src.setExpected([]string{"gossipgrid-1"})
	snap = src.Snapshot()
	e.reconcileCursors(&snap)

	e.mu.Lock()
	_, has1After := e.cursors["gossipgrid-1"]
	_, has2After := e.cursors["gossipgrid-2"]
	e.mu.Unlock()
	if !has1After {
		t.Error("expected peer 1 cursor to be preserved (still in expected membership)")
	}
	if has2After {
		t.Error("expected peer 2 cursor to be removed (confirmed removal, expected membership)")
	}
}

// TestCursorReconciliationPreservesActivePeer verifies that a peer with an
// active sync worker is NOT removed from the cursor map, even if it is
// not in the current peer list.
func TestCursorReconciliationPreservesActivePeer(t *testing.T) {
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers(nil) // no peers in current membership

	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: genUUIDs(0, 3)}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range genUUIDs(0, 3) {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	// Mark the peer as active (simulating an in-flight sync).
	e.mu.Lock()
	e.activePeers["gossipgrid-1"] = true
	e.cursors["gossipgrid-1"] = &peerCursor{cursor: "some-cursor"}
	e.mu.Unlock()

	// Reconcile with an empty expected membership (authoritative, no replicas).
	// The peer is not in the expected membership but is active (in-flight sync),
	// so its cursor must be preserved.
	e.reconcileCursors(&MembershipSnapshot{Authoritative: true})

	e.mu.Lock()
	_, stillPresent := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if !stillPresent {
		t.Error("expected active peer cursor to be preserved despite leaving membership")
	}
}

// --- Peer progress tracking tests ---

// TestPeerProgressTracksLastSuccessAndError verifies that per-peer progress
// (LastCursor, LastSuccess, LastError) is tracked meaningfully.
func TestPeerProgressTracksLastSuccessAndError(t *testing.T) {
	peerKey := "gossipgrid-1.suffix"
	ids := genUUIDs(0, 3)

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)
	ctx := context.Background()
	target := PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}

	// Successful sync: LastSuccess should be set, LastError empty.
	e.syncPeer(ctx, target)
	st := e.Stats()
	pp, ok := st.PerPeer["gossipgrid-1"]
	if !ok {
		t.Fatal("expected per-peer progress for gossipgrid-1")
	}
	if pp.LastSuccess.IsZero() {
		t.Error("expected LastSuccess to be set after successful sync")
	}
	if pp.LastError != "" {
		t.Errorf("expected empty LastError after successful sync, got %q", pp.LastError)
	}

	// Failed sync: LastError should be set.
	inv.err = errors.New("connection refused")
	e.syncPeer(ctx, target)
	st = e.Stats()
	pp, ok = st.PerPeer["gossipgrid-1"]
	if !ok {
		t.Fatal("expected per-peer progress for gossipgrid-1")
	}
	if pp.LastError == "" {
		t.Error("expected LastError to be set after failed sync")
	}
	if pp.LastError != "connection refused" {
		t.Errorf("expected LastError 'connection refused', got %q", pp.LastError)
	}
}

// TestPeerProgressConcurrentStats verifies that Stats returns immutable
// copies and is safe for concurrent access.
func TestPeerProgressConcurrentStats(t *testing.T) {
	peerKey := "gossipgrid-1.suffix"
	ids := genUUIDs(0, 5)

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)
	ctx := context.Background()
	target := PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}

	// Run concurrent syncPeer and Stats calls.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			e.syncPeer(ctx, target)
		}()
		go func() {
			defer wg.Done()
			st := e.Stats()
			_ = st.PerPeer
		}()
	}
	wg.Wait()
}

// --- Batch size / response bound tests ---

// TestFetchBatchSizeExceedsBoundRejected verifies that a fetch batch size
// exceeding 8 (the current bound) is rejected by validation.
func TestFetchBatchSizeExceedsBoundRejected(t *testing.T) {
	cfg := Config{Interval: 30 * time.Second, PageSize: 128, FetchBatchSize: 9, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: 5 * time.Second}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for fetch batch size 9 (exceeds 8 bound)")
	}
}

// --- Completed-pass counting tests (Issue 1) ---

// TestPartialSweepNotCompletedPass reproduces the reported defect: with a
// remote inventory of 3 records, PageSize=1, MaxPagesPerPeer=1, and a first
// response of has_more=true, the engine must NOT count a completed pass (the
// page budget is not the end of the remote inventory). It also verifies the
// cursor is preserved so the next cycle resumes from it.
func TestPartialSweepNotCompletedPass(t *testing.T) {
	ids := genUUIDs(0, 3)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	// PageSize=1, MaxPagesPerPeer=1: only the first page is processed, and it
	// has has_more=true (3 records, page size 1).
	cfg := Config{Interval: 100 * time.Hour, PageSize: 1, FetchBatchSize: 1, MaxPagesPerPeer: 1, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	st := e.Stats()
	if st.CompletedPasses != 0 {
		t.Errorf("partial sweep (has_more=true, page budget exhausted): CompletedPasses = %d, want 0", st.CompletedPasses)
	}
	if st.InventoryPages != 1 {
		t.Errorf("partial sweep: InventoryPages = %d, want 1 (the one page processed)", st.InventoryPages)
	}
	// The cursor must be preserved (at ids[0]) so the next cycle resumes.
	e.mu.Lock()
	cursor := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor == nil {
		t.Fatal("expected cursor to exist")
	}
	if cursor.cursor != ids[0] {
		t.Errorf("cursor = %q, want %q (preserved after partial sweep)", cursor.cursor, ids[0])
	}
	// LastSuccess must not be set for a partial sweep.
	if !st.LastSuccess.IsZero() {
		t.Errorf("partial sweep: LastSuccess set (%v), want zero (a partial sweep is not a completed synchronization)", st.LastSuccess)
	}
}

// TestMultiCycleCompletion reproduces the full lifecycle: a large inventory is
// swept across multiple cycles (each cycle processes up to MaxPagesPerPeer
// pages), and CompletedPasses increments ONLY when the final cycle reaches
// has_more=false.
func TestMultiCycleCompletion(t *testing.T) {
	ids := genUUIDs(0, 30) // 30 IDs, page size 10 → 3 pages
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	// MaxPagesPerPeer=1: one page per cycle. 3 pages → 3 cycles to complete.
	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 10, MaxPagesPerPeer: 1, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)
	ctx := context.Background()
	target := PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}

	// Cycle 1: page 1 (has_more=true). Not a completed pass.
	e.syncPeer(ctx, target)
	if st := e.Stats(); st.CompletedPasses != 0 {
		t.Errorf("cycle 1 (partial): CompletedPasses = %d, want 0", st.CompletedPasses)
	}
	// Cycle 2: page 2 (has_more=true). Not a completed pass.
	e.syncPeer(ctx, target)
	if st := e.Stats(); st.CompletedPasses != 0 {
		t.Errorf("cycle 2 (partial): CompletedPasses = %d, want 0", st.CompletedPasses)
	}
	// Cycle 3: page 3 (has_more=false). Completed pass.
	e.syncPeer(ctx, target)
	st := e.Stats()
	if st.CompletedPasses != 1 {
		t.Errorf("cycle 3 (complete): CompletedPasses = %d, want 1", st.CompletedPasses)
	}
	if st.InventoryPages != 3 {
		t.Errorf("after 3 cycles: InventoryPages = %d, want 3", st.InventoryPages)
	}
	// After completion, the cursor is reset.
	e.mu.Lock()
	cursor := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor.cursor != "" {
		t.Errorf("cursor after completed sweep = %q, want empty (reset)", cursor.cursor)
	}
}

// TestFailedPageNotCompletedPass verifies that when a page fails (inventory
// fetch error), the pass is not counted as completed, even if the inventory
// has only one page (has_more=false would have been the case for a full
// sweep, but the failure prevents completion).
func TestFailedPageNotCompletedPass(t *testing.T) {
	ids := genUUIDs(0, 3)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	// Fail the inventory fetch.
	inv.err = errors.New("connection refused")
	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	st := e.Stats()
	if st.CompletedPasses != 0 {
		t.Errorf("failed page: CompletedPasses = %d, want 0", st.CompletedPasses)
	}
	if st.FailedPasses == 0 {
		t.Error("failed page: FailedPasses should be > 0")
	}
}

// TestFullSweepCompletedPass verifies that a single-cycle full sweep
// (has_more=false) is counted as exactly one completed pass.
func TestFullSweepCompletedPass(t *testing.T) {
	ids := genUUIDs(0, 3)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)
	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	st := e.Stats()
	if st.CompletedPasses != 1 {
		t.Errorf("full sweep: CompletedPasses = %d, want 1", st.CompletedPasses)
	}
}

// --- Stale DNS cursor preservation tests (Issue 2) ---

// TestStaleDNSPreservesCursor is the regression test for the reported defect:
// a peer with an inventory larger than the per-cycle page budget advances its
// cursor in one cycle; a temporary stale DNS snapshot occurs; no sync is
// attempted during the stale period; the cursor is retained; fresh discovery
// returns; reconciliation resumes from the saved cursor and completes.
func TestStaleDNSPreservesCursor(t *testing.T) {
	peerKey := "gossipgrid-1.suffix"
	ids := genUUIDs(0, 3) // 3 IDs, page size 1 → 3 pages

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	// MaxPagesPerPeer=1: one page per cycle.
	cfg := Config{Interval: 100 * time.Hour, PageSize: 1, FetchBatchSize: 1, MaxPagesPerPeer: 1, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)
	ctx := context.Background()

	// Cycle 1 (fresh membership): advance the cursor to ids[0].
	src.setValid(true)
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	src.setExpected([]string{"gossipgrid-1"})
	snap := src.Snapshot()
	e.reconcileCursors(&snap)
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})
	e.mu.Lock()
	cursorAfter1 := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursorAfter1 == nil || cursorAfter1.cursor != ids[0] {
		t.Fatalf("after cycle 1: cursor = %v, want %q", cursorAfter1, ids[0])
	}

	// Stale DNS period: the stale snapshot yields no peers, and membership is
	// NOT authoritative. Reconciliation must NOT drop the cursor.
	src.setValid(false)
	src.setPeers(nil) // stale DNS: no peers returned
	snap = src.Snapshot()
	e.reconcileCursors(&snap)
	e.mu.Lock()
	cursorDuringStale := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursorDuringStale == nil {
		t.Fatal("cursor was removed during stale DNS (the reported defect)")
	}
	if cursorDuringStale.cursor != ids[0] {
		t.Errorf("cursor during stale period = %q, want %q (preserved)", cursorDuringStale.cursor, ids[0])
	}

	// No sync is attempted during the stale period (Snapshot() returns no peers).
	snap = src.Snapshot()
	if got := len(snap.Peers); got != 0 {
		t.Errorf("expected 0 peers during stale period, got %d", got)
	}

	// Fresh discovery returns: membership is authoritative again, peer present.
	src.setValid(true)
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	src.setExpected([]string{"gossipgrid-1"})
	snap = src.Snapshot()
	e.reconcileCursors(&snap)
	e.mu.Lock()
	cursorAfterFresh := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursorAfterFresh == nil || cursorAfterFresh.cursor != ids[0] {
		t.Fatalf("after fresh discovery: cursor = %v, want %q (resumed from saved cursor)", cursorAfterFresh, ids[0])
	}

	// Cycles 2 and 3 resume from the saved cursor and complete the sweep.
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})
	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.ids) != 3 {
		t.Errorf("after resuming from saved cursor: local has %d IDs, want 3 (full sweep completed)", len(local.ids))
	}
}

// TestConfirmedPeerRemovalCleansUp verifies that when membership is
// authoritative (fresh) and a peer is genuinely removed, its cursor state is
// cleaned up (bounded state, no indefinite accumulation).
func TestConfirmedPeerRemovalCleansUp(t *testing.T) {
	peerKey1 := "gossipgrid-1.suffix"
	peerKey2 := "gossipgrid-2.suffix"

	src := &fakePeerSource{}
	src.setValid(true)
	src.setPeers([]PeerTarget{
		{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey1, PodIdentity: "gossipgrid-1"},
		{DialAddr: "127.0.0.1:8444", ExpectedIdentity: peerKey2, PodIdentity: "gossipgrid-2"},
	})
	src.setExpected([]string{"gossipgrid-1", "gossipgrid-2"})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey1: genUUIDs(0, 2), peerKey2: genUUIDs(100, 2)}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range append(genUUIDs(0, 2), genUUIDs(100, 2)...) {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 1, FetchBatchSize: 1, MaxPagesPerPeer: 1, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)
	ctx := context.Background()

	// Both peers present; sync both (partial sweeps, cursors set).
	snap := src.Snapshot()
	e.reconcileCursors(&snap)
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey1, PodIdentity: "gossipgrid-1"})
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8444", ExpectedIdentity: peerKey2, PodIdentity: "gossipgrid-2"})

	// Peer 2 is confirmed removed (expected membership no longer includes it).
	// Reconcile.
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey1, PodIdentity: "gossipgrid-1"}})
	src.setExpected([]string{"gossipgrid-1"})
	snap = src.Snapshot()
	e.reconcileCursors(&snap)

	e.mu.Lock()
	_, has1 := e.cursors["gossipgrid-1"]
	_, has2 := e.cursors["gossipgrid-2"]
	e.mu.Unlock()
	if !has1 {
		t.Error("peer 1 cursor should be preserved (still in expected membership)")
	}
	if has2 {
		t.Error("peer 2 cursor should be removed (confirmed removal, expected membership)")
	}
}

// --- Regression tests for the 2026-11-20 M4C final corrections ---

// TestLastSuccessPreservedAcrossCyclesAfterCompletedSweep is the regression
// test for the reported defect: a completed successful sweep resets the
// cursor to empty; the next membership reconciliation (with the same
// authoritative peer still present) must NOT delete the cursor entry, so
// LastSuccess, LastError, and completion diagnostics remain available.
func TestLastSuccessPreservedAcrossCyclesAfterCompletedSweep(t *testing.T) {
	ids := genUUIDs(0, 3)
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setValid(true)
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	src.setExpected([]string{"gossipgrid-1"})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)
	ctx := context.Background()
	target := PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}

	// Cycle 1: full sweep completes (has_more=false). Cursor resets to "".
	e.syncPeer(ctx, target)
	st := e.Stats()
	if st.CompletedPasses != 1 {
		t.Fatalf("cycle 1: CompletedPasses = %d, want 1", st.CompletedPasses)
	}
	if st.LastSuccess.IsZero() {
		t.Fatal("cycle 1: LastSuccess should be set after completed sweep")
	}
	firstSuccess := st.LastSuccess

	// Reconcile with the same peer still present (authoritative).
	// The cursor was reset to "" by the completed sweep; the old code would
	// delete the entry here, losing LastSuccess.
	snap := src.Snapshot()
	e.reconcileCursors(&snap)

	e.mu.Lock()
	cursor := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor == nil {
		t.Fatal("cursor was deleted after completed sweep + reconciliation (the reported defect)")
	}
	if cursor.lastSuccess.IsZero() {
		t.Error("LastSuccess was lost after reconciliation (the reported defect)")
	}
	if !cursor.lastSuccess.Equal(firstSuccess) {
		t.Errorf("LastSuccess changed: got %v, want %v", cursor.lastSuccess, firstSuccess)
	}

	// Cycle 2: another full sweep (idempotent, no new inserts).
	e.syncPeer(ctx, target)
	st = e.Stats()
	if st.CompletedPasses != 2 {
		t.Errorf("cycle 2: CompletedPasses = %d, want 2", st.CompletedPasses)
	}
	if st.LastSuccess.IsZero() {
		t.Error("cycle 2: LastSuccess should still be set")
	}

	// Reconcile again.
	snap = src.Snapshot()
	e.reconcileCursors(&snap)
	e.mu.Lock()
	cursor = e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor == nil {
		t.Fatal("cursor was deleted after second reconciliation")
	}
	if cursor.lastSuccess.IsZero() {
		t.Error("LastSuccess was lost after second reconciliation")
	}
}

// TestCursorPreservedWhenMembershipNotAuthoritative verifies that when
// membership is not authoritative (stale DNS), no cursor state is removed
// even for peers with empty cursors.
func TestCursorPreservedWhenMembershipNotAuthoritative(t *testing.T) {
	src := &fakePeerSource{}
	src.setValid(false) // non-authoritative
	src.setPeers(nil)

	inv := &fakeInventoryClient{inv: map[string][]string{}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	// Set up a cursor entry with an empty cursor (completed sweep).
	e.mu.Lock()
	e.cursors["gossipgrid-1"] = &peerCursor{cursor: "", sweepDone: true, lastSuccess: time.Now()}
	e.mu.Unlock()

	// Reconcile with non-authoritative membership (stale DNS, no peers).
	snap := src.Snapshot()
	e.reconcileCursors(&snap)

	e.mu.Lock()
	_, present := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if !present {
		t.Error("cursor should be preserved when membership is not authoritative")
	}
}

// TestStaleToFreshTransitionDuringCycle verifies the reported defect: a
// stale-to-fresh transition between two separate PeerSource reads can
// produce Peers()=empty (stale) and PeersValid()=true (fresh), causing
// the engine to incorrectly delete saved cursors. The unified Snapshot()
// method prevents this by reading the discovery snapshot exactly once.
func TestStaleToFreshTransitionDuringCycle(t *testing.T) {
	peerKey := "gossipgrid-1.suffix"
	ids := genUUIDs(0, 3)

	// A deterministic snapshot function that transitions from stale to fresh
	// between calls (simulating a DNS recovery during a cycle).
	callCount := 0
	src := NewDiscoveryPeerSource("test.svc", func() PeerSourceSnapshot {
		callCount++
		if callCount == 1 {
			// First call: stale DNS (no peers, non-authoritative)
			return PeerSourceSnapshot{
				Peers:              nil,
				ExpectedIdentities: []string{"gossipgrid-1"},
				Stale:              true,
				Mode:               "dns",
			}
		}
		// Second call: fresh DNS (peer present, authoritative)
		return PeerSourceSnapshot{
			Peers: []PeerSourceInfo{
				{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1:8443"},
			},
			ExpectedIdentities: []string{"gossipgrid-1"},
			Stale:              false,
			Mode:               "dns",
		}
	})

	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "o", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	// Simulate a prior sync that advanced the cursor (partial sweep).
	e.mu.Lock()
	e.cursors["gossipgrid-1"] = &peerCursor{cursor: ids[0], lastSuccess: time.Now().Add(-time.Hour)}
	e.mu.Unlock()

	// Call Snapshot() once (the way runCycle does). The first call returns
	// the stale snapshot (no peers, non-authoritative). Reconciliation must
	// NOT delete the cursor.
	snap := src.Snapshot()
	e.reconcileCursors(&snap)

	e.mu.Lock()
	cursor := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor == nil {
		t.Fatal("cursor was deleted during stale-to-fresh transition (the reported defect)")
	}
	if cursor.cursor != ids[0] {
		t.Errorf("cursor changed: got %q, want %q", cursor.cursor, ids[0])
	}

	// A second call (next cycle) returns the fresh snapshot (peer present).
	snap = src.Snapshot()
	if !snap.Authoritative {
		t.Fatal("expected authoritative=true for fresh DNS")
	}
	if len(snap.Peers) != 1 {
		t.Fatalf("expected 1 peer for fresh DNS, got %d", len(snap.Peers))
	}
	e.reconcileCursors(&snap)

	e.mu.Lock()
	cursor = e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor == nil {
		t.Fatal("cursor was deleted after fresh snapshot reconciliation")
	}
}

// TestFreshToStaleTransition verifies that a fresh-to-stale transition
// preserves cursor state: the first read is fresh (authoritative, peer
// present), the second is stale (non-authoritative, no peers). Cursor
// state must be preserved across the transition.
func TestFreshToStaleTransition(t *testing.T) {
	callCount := 0
	src := NewDiscoveryPeerSource("test.svc", func() PeerSourceSnapshot {
		callCount++
		if callCount == 1 {
			return PeerSourceSnapshot{
				Peers: []PeerSourceInfo{
					{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1:8443"},
				},
				ExpectedIdentities: []string{"gossipgrid-1"},
				Stale:              false,
				Mode:               "dns",
			}
		}
		return PeerSourceSnapshot{
			Peers:              nil,
			ExpectedIdentities: []string{"gossipgrid-1"},
			Stale:              true,
			Mode:               "dns",
		}
	})

	inv := &fakeInventoryClient{inv: map[string][]string{}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)

	// Set up a cursor entry (in-progress sweep).
	e.mu.Lock()
	e.cursors["gossipgrid-1"] = &peerCursor{cursor: "some-cursor"}
	e.mu.Unlock()

	// First read: fresh (authoritative, peer present). Reconciliation preserves.
	snap := src.Snapshot()
	if !snap.Authoritative {
		t.Fatal("expected authoritative=true for fresh DNS")
	}
	e.reconcileCursors(&snap)

	e.mu.Lock()
	_, present := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if !present {
		t.Fatal("cursor should be preserved after fresh reconciliation")
	}

	// Second read: stale (non-authoritative, no peers). Reconciliation preserves.
	snap = src.Snapshot()
	if snap.Authoritative {
		t.Fatal("expected authoritative=false for stale DNS")
	}
	e.reconcileCursors(&snap)

	e.mu.Lock()
	cursor := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor == nil {
		t.Fatal("cursor should be preserved after stale reconciliation (fresh-to-stale transition)")
	}
	if cursor.cursor != "some-cursor" {
		t.Errorf("cursor changed: got %q, want %q", cursor.cursor, "some-cursor")
	}
}

// --- Regression tests for the 2026-11-21 M4C cursor-preservation correction ---

// TestDNSTemporarilyOmitsPeerPreservesCursor is the regression test for the
// reported defect: a successful DNS lookup temporarily omits a StatefulSet pod
// (the pod is not removed from the cluster). With PageSize=1 and
// MaxPagesPerPeer=1, one cycle retrieves the first message and saves its
// cursor. A fresh DNS result that omits the peer must NOT delete the saved
// cursor. When the peer becomes discoverable again, synchronization resumes
// from the saved cursor.
//
// Reproduction: five cycles with intermittent fresh-DNS omissions must still
// recover all three messages.
func TestDNSTemporarilyOmitsPeerPreservesCursor(t *testing.T) {
	peerKey := "gossipgrid-1.suffix"
	ids := genUUIDs(0, 3) // 3 IDs, page size 1 → 3 pages

	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "gossipgrid-1", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	// PageSize=1, MaxPagesPerPeer=1: one ID per cycle.
	cfg := Config{Interval: 100 * time.Hour, PageSize: 1, FetchBatchSize: 1, MaxPagesPerPeer: 1, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, nil, inv, fetcher, local, nil)
	ctx := context.Background()
	target := PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}

	// The expected membership includes gossipgrid-1 (the StatefulSet has 2
	// replicas: gossipgrid-0 is local, gossipgrid-1 is the remote peer).
	expected := []string{"gossipgrid-0", "gossipgrid-1"}

	// Cycle 1: fresh DNS, peer present. Sync advances cursor to ids[0].
	snap := MembershipSnapshot{
		Peers:              []PeerTarget{target},
		ExpectedIdentities: expected,
		Authoritative:      true,
	}
	e.reconcileCursors(&snap)
	e.syncPeer(ctx, target)

	e.mu.Lock()
	cursor1 := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor1 == nil || cursor1.cursor != ids[0] {
		t.Fatalf("cycle 1: cursor = %v, want %q", cursor1, ids[0])
	}
	if len(local.ids) != 1 {
		t.Fatalf("cycle 1: local has %d IDs, want 1", len(local.ids))
	}

	// Cycle 2: FRESH DNS that temporarily omits gossipgrid-1.
	// The expected membership still includes gossipgrid-1 (it was not
	// removed from the StatefulSet). No sync is attempted (no target),
	// but reconciliation must NOT delete the cursor.
	snap = MembershipSnapshot{
		Peers:              nil, // DNS omitted the pod
		ExpectedIdentities: expected,
		Authoritative:      true, // the DNS lookup itself succeeded
	}
	e.reconcileCursors(&snap)

	e.mu.Lock()
	cursor2 := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor2 == nil {
		t.Fatal("cycle 2: cursor was deleted by fresh DNS omission (the reported defect)")
	}
	if cursor2.cursor != ids[0] {
		t.Errorf("cycle 2: cursor = %q, want %q (preserved)", cursor2.cursor, ids[0])
	}

	// Cycle 3: peer reappears in DNS. Reconciliation preserves the cursor.
	// Sync resumes from ids[0] and advances to ids[1].
	snap = MembershipSnapshot{
		Peers:              []PeerTarget{target},
		ExpectedIdentities: expected,
		Authoritative:      true,
	}
	e.reconcileCursors(&snap)
	e.syncPeer(ctx, target)

	e.mu.Lock()
	cursor3 := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor3 == nil || cursor3.cursor != ids[1] {
		t.Fatalf("cycle 3: cursor = %v, want %q (resumed from saved cursor)", cursor3, ids[1])
	}
	if len(local.ids) != 2 {
		t.Fatalf("cycle 3: local has %d IDs, want 2", len(local.ids))
	}

	// Cycle 4: fresh DNS omits the peer again.
	snap = MembershipSnapshot{
		Peers:              nil,
		ExpectedIdentities: expected,
		Authoritative:      true,
	}
	e.reconcileCursors(&snap)

	e.mu.Lock()
	cursor4 := e.cursors["gossipgrid-1"]
	e.mu.Unlock()
	if cursor4 == nil {
		t.Fatal("cycle 4: cursor was deleted by fresh DNS omission (the reported defect)")
	}
	if cursor4.cursor != ids[1] {
		t.Errorf("cycle 4: cursor = %q, want %q (preserved)", cursor4.cursor, ids[1])
	}

	// Cycle 5: peer reappears. Sync resumes and completes the sweep.
	snap = MembershipSnapshot{
		Peers:              []PeerTarget{target},
		ExpectedIdentities: expected,
		Authoritative:      true,
	}
	e.reconcileCursors(&snap)
	e.syncPeer(ctx, target)

	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.ids) != 3 {
		t.Errorf("after 5 cycles: local has %d IDs, want 3 (full sweep completed)", len(local.ids))
	}
}

// TestDNSTemporarilyOmitsPeerPreservesLastSuccess verifies that LastSuccess
// and LastError diagnostics for an expected replica are preserved across
// temporary DNS outages.
func TestDNSTemporarilyOmitsPeerPreservesLastSuccess(t *testing.T) {
	peerKey := "gossipgrid-1.suffix"
	ids := genUUIDs(0, 3)

	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "gossipgrid-1", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, nil, inv, fetcher, local, nil)
	ctx := context.Background()
	target := PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}
	expected := []string{"gossipgrid-0", "gossipgrid-1"}

	// Full sweep completes.
	snap := MembershipSnapshot{Peers: []PeerTarget{target}, ExpectedIdentities: expected, Authoritative: true}
	e.reconcileCursors(&snap)
	e.syncPeer(ctx, target)

	st := e.Stats()
	pp, ok := st.PerPeer["gossipgrid-1"]
	if !ok {
		t.Fatal("expected per-peer progress for gossipgrid-1")
	}
	if pp.LastSuccess.IsZero() {
		t.Fatal("expected LastSuccess after completed sweep")
	}
	firstSuccess := pp.LastSuccess

	// Fresh DNS omits the peer. Reconciliation must preserve LastSuccess.
	snap = MembershipSnapshot{Peers: nil, ExpectedIdentities: expected, Authoritative: true}
	e.reconcileCursors(&snap)

	st = e.Stats()
	pp, ok = st.PerPeer["gossipgrid-1"]
	if !ok {
		t.Fatal("per-peer progress for gossipgrid-1 was deleted (the reported defect)")
	}
	if pp.LastSuccess.IsZero() {
		t.Error("LastSuccess was lost after DNS omission reconciliation (the reported defect)")
	}
	if !pp.LastSuccess.Equal(firstSuccess) {
		t.Errorf("LastSuccess changed: got %v, want %v", pp.LastSuccess, firstSuccess)
	}
}

// TestExpectedMembershipRemovalCleansUpCursor verifies that when the
// authoritative expected-membership change genuinely removes a replica
// (StatefulSet scaled down), the obsolete cursor is cleaned up.
func TestExpectedMembershipRemovalCleansUpCursor(t *testing.T) {
	inv := &fakeInventoryClient{inv: map[string][]string{}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, nil, inv, fetcher, local, nil)

	// Set up a cursor entry for gossipgrid-2 (in-progress sweep).
	e.mu.Lock()
	e.cursors["gossipgrid-2"] = &peerCursor{cursor: "some-cursor", lastSuccess: time.Now()}
	e.mu.Unlock()

	// The StatefulSet was scaled down: gossipgrid-2 is no longer in the
	// expected membership. The DNS lookup also reflects this (peer absent).
	snap := MembershipSnapshot{
		Peers:              nil,
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Authoritative:      true,
	}
	e.reconcileCursors(&snap)

	e.mu.Lock()
	_, present := e.cursors["gossipgrid-2"]
	e.mu.Unlock()
	if present {
		t.Error("expected gossipgrid-2 cursor to be removed (confirmed removal from expected membership)")
	}
}

// TestStateBoundedThroughMembershipChanges verifies that the cursor map does
// not grow unboundedly as peers join and leave the expected membership.
func TestStateBoundedThroughMembershipChanges(t *testing.T) {
	inv := &fakeInventoryClient{inv: map[string][]string{}}
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := New(cfg, nil, inv, fetcher, local, nil)

	// Simulate a peer joining (cursor created) then leaving (cursor removed)
	// multiple times. The map should stay bounded.
	for i := 0; i < 10; i++ {
		// Peer joins: expected membership includes it, sync creates a cursor.
		peerID := fmt.Sprintf("gossipgrid-%d", i+3) // ordinal 3-12
		snap := MembershipSnapshot{
			Peers: []PeerTarget{{
				DialAddr:         fmt.Sprintf("127.0.0.1:%d", 8443+i),
				ExpectedIdentity: peerID + ".suffix",
				PodIdentity:      peerID,
			}},
			ExpectedIdentities: []string{"gossipgrid-0", peerID},
			Authoritative:      true,
		}
		e.reconcileCursors(&snap)
		// Simulate a cursor being created (as syncPeer would).
		e.mu.Lock()
		e.cursors[peerID] = &peerCursor{cursor: "cursor-" + peerID}
		e.mu.Unlock()

		// Peer leaves: expected membership no longer includes it.
		snap = MembershipSnapshot{
			Peers:              nil,
			ExpectedIdentities: []string{"gossipgrid-0"},
			Authoritative:      true,
		}
		e.reconcileCursors(&snap)
	}

	// After 10 join/leave cycles, the cursor map should be empty (bounded).
	e.mu.Lock()
	count := len(e.cursors)
	e.mu.Unlock()
	if count != 0 {
		t.Errorf("cursor map has %d entries after 10 join/leave cycles, want 0 (bounded)", count)
	}
}

// TestConcurrentStatsAndSyncWithExpectedMembership verifies that Stats and
// syncPeer remain race-safe when the peer source returns expected membership.
func TestConcurrentStatsAndSyncWithExpectedMembership(t *testing.T) {
	peerKey := "gossipgrid-1.suffix"
	ids := genUUIDs(0, 5)

	src := &fakePeerSource{}
	src.setValid(true)
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})
	src.setExpected([]string{"gossipgrid-0", "gossipgrid-1"})
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: ids}}
	msgs := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		msgs[id] = peertls.SyncMessageWire{ID: id, Body: "b", Origin: "gossipgrid-1", CreatedAt: "2026-11-20T12:00:00Z"}
	}
	fetcher := &fakeMessageFetcher{msgs: msgs}
	local := &fakeLocalStore{ids: map[string]bool{}}

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 2, RequestTimeout: time.Second}
	e := New(cfg, src, inv, fetcher, local, nil)
	ctx := context.Background()
	target := PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}

	// Run concurrent syncPeer, Stats, and reconcileCursors calls.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			e.syncPeer(ctx, target)
		}()
		go func() {
			defer wg.Done()
			st := e.Stats()
			_ = st.PerPeer
		}()
		go func() {
			defer wg.Done()
			snap := src.Snapshot()
			e.reconcileCursors(&snap)
		}()
	}
	wg.Wait()
}

// TestAntiEntropyRejectsExpiredMessages verifies that, with retention enabled,
// the anti-entropy engine does NOT insert fetched messages whose creation
// timestamp is older than the current expiration threshold. This is the
// regression test for the resurrection-through-anti-entropy defect: without
// this check, anti-entropy would reinsert (resurrect) messages that the local
// replica has already expired (e.g. after a cleanup sweep).
//
// The test verifies:
//  1. An expired message is NOT inserted (the local store does not gain it).
//  2. A fresh message IS inserted (retention does not block valid messages).
//  3. The page is NOT failed (no error returned; cursor advances normally).
func TestAntiEntropyRejectsExpiredMessages(t *testing.T) {
	base := "2026-06-01T12:00:00Z"
	freshTime := "2026-06-01T11:30:00Z"   // 30 min before base (within 2h TTL)
	expiredTime := "2026-06-01T06:00:00Z" // 6h before base (exceeds 2h TTL)

	freshID := "019a0000-0000-7000-0000-000000000001"
	expiredID := "019a0000-0001-7000-0000-000000000002"
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})

	// Peer advertises both IDs.
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: {freshID, expiredID}}}

	// Peer has both messages.
	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{
		freshID:   {ID: freshID, Body: "fresh", Origin: "gossipgrid-1", CreatedAt: freshTime},
		expiredID: {ID: expiredID, Body: "expired", Origin: "gossipgrid-1", CreatedAt: expiredTime},
	}}

	// Local has neither message (both are "missing").
	local := &fakeLocalStore{ids: map[string]bool{}}

	ttl := 2 * time.Hour
	fixedNow, _ := time.Parse(time.RFC3339, base)

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := WithClock(cfg, src, inv, fetcher, local, nil, func() time.Time { return fixedNow })
	e.SetRetentionTTL(ttl)

	// Call syncPeer directly.
	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	// The fresh message must have been inserted.
	local.mu.Lock()
	defer local.mu.Unlock()
	if !local.ids[freshID] {
		t.Errorf("fresh message not inserted; retention should not block valid messages")
	}
	// The expired message must NOT have been inserted.
	if local.ids[expiredID] {
		t.Errorf("expired message was inserted (resurrection through anti-entropy)")
	}
	// Only 1 insert should have occurred.
	if len(local.inserted) != 1 {
		t.Errorf("inserted = %v, want only the fresh ID", local.inserted)
	}
}

// TestAntiEntropyExpirationDuringFetch verifies that when a message expires
// between inventory enumeration and the batch fetch (the peer's 404
// response lists the expired ID as missing), the anti-entropy engine does
// NOT stall: the valid messages in the same batch are still inserted, the
// page is NOT failed, and the cursor advances. This is the regression test
// for the defect where one expired ID caused the entire batch to fail
// (404 on any missing ID), stalling reconciliation indefinitely.
//
// The 404-retry logic lives in the client's FetchMessages (it parses the
// 404 body, identifies the missing IDs, retries with only the present
// IDs, and returns a 200 with the valid records). The engine's
// fetchAndInsert calls FetchMessages and receives only the present
// messages (not the 404 error), so the page is processed successfully and
// the cursor advances. This test simulates that outcome: the fetcher
// returns only the valid message (as the real client would after its
// 404-retry), and the engine must insert it without failing the page.
func TestAntiEntropyExpirationDuringFetch(t *testing.T) {
	freshTime := "2026-06-01T11:30:00Z" // 30 min before base (within 2h TTL)

	freshID := "019a0000-0000-7000-0000-000000000001"
	expiredID := "019a0000-0001-7000-0000-000000000002"
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})

	// Peer advertises both IDs (the inventory was enumerated before the
	// expired message crossed the expiration boundary on the peer).
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: {freshID, expiredID}}}

	// The fetcher simulates the client's 404-retry outcome: the peer's
	// inventory advertised both IDs, but by the time the fetch arrived the
	// expired message had been excluded (404 listing it as missing). The
	// client's FetchMessages parsed the 404, retried with only the fresh
	// ID, and got a 200 with only the fresh message. The engine receives
	// only the fresh message (the subset that is still present).
	fetcher := &subsetFetcher{
		// When asked for both IDs, return only the fresh one (simulating
		// the client's 404-retry result).
		result: map[string]peertls.SyncMessageWire{
			freshID: {ID: freshID, Body: "fresh", Origin: "gossipgrid-1", CreatedAt: freshTime},
		},
	}

	// Local has neither message.
	local := &fakeLocalStore{ids: map[string]bool{}}

	ttl := 2 * time.Hour
	fixedNow, _ := time.Parse(time.RFC3339, "2026-06-01T12:00:00Z")

	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := WithClock(cfg, src, inv, fetcher, local, nil, func() time.Time { return fixedNow })
	e.SetRetentionTTL(ttl)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	// The fresh message must have been inserted (the valid message in the
	// batch was retrievable despite the expired ID's 404).
	local.mu.Lock()
	defer local.mu.Unlock()
	if !local.ids[freshID] {
		t.Errorf("fresh message not inserted; a 404 for one ID should not block retrieval of valid messages in the same batch")
	}
	// The expired message must NOT have been inserted (the fetcher did not
	// return it; the client's 404-retry excluded it).
	if local.ids[expiredID] {
		t.Errorf("expired message was inserted (resurrection)")
	}
	// The page must have been processed successfully (no failed pass).
	if e.Stats().FailedPasses != 0 {
		t.Errorf("FailedPasses = %d, want 0 (expiration during fetch must not fail the page)", e.Stats().FailedPasses)
	}
	// The completed pass must be recorded (full sweep reached has_more=false
	// with no errors).
	if e.Stats().CompletedPasses != 1 {
		t.Errorf("CompletedPasses = %d, want 1 (page processed without failure)", e.Stats().CompletedPasses)
	}
}

// subsetFetcher simulates the client's 404-retry outcome: when asked for a
// batch of IDs, it returns only the messages that are still present on the
// peer (the subset). This models the production behavior where the client
// receives a 404, parses the missing IDs, retries with only the present
// IDs, and gets a 200 with the valid records.
type subsetFetcher struct {
	mu     sync.Mutex
	result map[string]peertls.SyncMessageWire
	calls  int
}

func (f *subsetFetcher) FetchMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string) ([]peertls.SyncMessageWire, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	var result []peertls.SyncMessageWire
	for _, id := range ids {
		if m, ok := f.result[id]; ok {
			result = append(result, m)
		}
	}
	// No error: the client's 404-retry succeeded (or there was no 404).
	// The engine receives the subset of present messages.
	return result, nil
}

// FetchBootstrapMessages is the bootstrap-fetch variant of FetchMessages
// (Milestone 5B, Stage 2). The test fake serves the same data; the request
// purpose header is untrusted and does not change the serving-side decision in
// the fake.
func (f *subsetFetcher) FetchBootstrapMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string, _ *lifecycle.RequestContext) ([]peertls.SyncMessageWire, error) {
	return f.FetchMessages(ctx, dialAddr, expectedIdentity, ids)
}

// TestAntiEntropyExpiredAfterCleanupSweep verifies that, after a cleanup sweep
// removes an expired message from the local store, anti-entropy does NOT
// reinsert (resurrect) it when a peer still advertises it. This is the
// "resurrection after cleanup" regression test for the anti-entropy path.
func TestAntiEntropyExpiredAfterCleanupSweep(t *testing.T) {
	freshTime := "2026-06-01T11:30:00Z"
	expiredTime := "2026-06-01T06:00:00Z"
	fixedNow, _ := time.Parse(time.RFC3339, "2026-06-01T12:00:00Z")

	freshID := "019a0000-0000-7000-0000-000000000001"
	expiredID := "019a0000-0001-7000-0000-000000000002"
	peerKey := "gossipgrid-1.suffix"

	src := &fakePeerSource{}
	src.setPeers([]PeerTarget{{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"}})

	// Peer advertises both IDs (the peer's retention may differ, or the
	// peer may not have run its cleanup sweep yet).
	inv := &fakeInventoryClient{inv: map[string][]string{peerKey: {freshID, expiredID}}}

	fetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{
		freshID:   {ID: freshID, Body: "fresh", Origin: "gossipgrid-1", CreatedAt: freshTime},
		expiredID: {ID: expiredID, Body: "expired", Origin: "gossipgrid-1", CreatedAt: expiredTime},
	}}

	// Local has neither message (both are "missing" from FindMissingIDs).
	// Simulates the state after a cleanup sweep removed the expired message.
	local := &fakeLocalStore{ids: map[string]bool{}}

	ttl := 2 * time.Hour
	cfg := Config{Interval: 100 * time.Hour, PageSize: 10, FetchBatchSize: 5, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: time.Second}
	e := WithClock(cfg, src, inv, fetcher, local, nil, func() time.Time { return fixedNow })
	e.SetRetentionTTL(ttl)

	ctx := context.Background()
	e.syncPeer(ctx, PeerTarget{DialAddr: "127.0.0.1:8443", ExpectedIdentity: peerKey, PodIdentity: "gossipgrid-1"})

	local.mu.Lock()
	defer local.mu.Unlock()
	// The expired message must NOT be reinserted (resurrection prevented).
	if local.ids[expiredID] {
		t.Errorf("expired message resurrected after cleanup sweep via anti-entropy")
	}
	// The fresh message should have been inserted.
	if !local.ids[freshID] {
		t.Errorf("fresh message not inserted")
	}
}
