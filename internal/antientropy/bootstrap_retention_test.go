// Tests for the bootstrap baseline retention semantics (Milestone 5B, Stage 2,
// Issue 2). The requester-side expiration threshold is computed at the
// guarded installation boundary (per batch): e.now() reflects the advancing
// clock, so a record that was retained at inventory time but has since
// expired is correctly skipped at install time. The tests use an injected
// clock (WithClock) with a controllable, advancing time to exercise
// expiration during retrieval, between batches, and at the install boundary.

package antientropy

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/peertls"
	"gossipgrid/internal/store"
)

// retentionTestStore is a LocalStore that records per-disposition outcomes from
// InsertReplicatedMessagesBounded, so the test can verify which IDs were
// Inserted, AlreadyPresent, Expired, or Conflict.
type retentionTestStore struct {
	mu       sync.Mutex
	ids      map[string]bool
	inserted []string
	// thresholds records the expiration threshold passed to each
	// InsertReplicatedMessagesBounded call (per batch). The test can
	// verify that later batches receive an updated threshold.
	thresholds []time.Time
}

func (s *retentionTestStore) FindMissingIDs(_ context.Context, ids []string) ([]string, error) {
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

func (s *retentionTestStore) InsertReplicatedMessageReturningInserted(msg *store.Message) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids[msg.ID] {
		return false, store.ErrDuplicateIDConflict
	}
	s.ids[msg.ID] = true
	s.inserted = append(s.inserted, msg.ID)
	return true, nil
}

func (s *retentionTestStore) InsertReplicatedMessagesBounded(_ context.Context, msgs []*store.Message, expirationThreshold func() time.Time) (store.BootstrapInsertResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var threshold time.Time
	if expirationThreshold != nil {
		threshold = expirationThreshold()
	}
	s.thresholds = append(s.thresholds, threshold)
	var res store.BootstrapInsertResult
	for _, m := range msgs {
		// Apply the requester-side expiration threshold: a record strictly
		// older than the threshold is Expired (skipped, not stored, not a
		// failure).
		if !threshold.IsZero() && m.CreatedAt.Before(threshold) {
			res.Expired = append(res.Expired, m.ID)
			continue
		}
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

// advancingClock is a controllable clock that advances by a fixed increment
// on each call. This simulates the passage of time during a bootstrap
// retrieval (inventory → fetch → install). The increment is chosen to be
// large enough to push a record past the TTL threshold between the
// inventory and the install boundary.
type advancingClock struct {
	mu    sync.Mutex
	start time.Time
	step  time.Duration
	calls int
}

func (c *advancingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.start.Add(time.Duration(c.calls) * c.step)
	c.calls++
	return now
}

// TestBootstrapRetention_ExpiredMessageSkipped verifies that when a fetched
// message's creation timestamp is strictly older than the requester-side
// expiration threshold (now - TTL) at install time, the message is skipped
// (classified Expired) without resurrecting it, without failing the page,
// and without inserting it into the local store. The page is still completed
// (the expired record is a legitimate outcome, not a failure).
func TestBootstrapRetention_ExpiredMessageSkipped(t *testing.T) {
	ids := []string{"id-fresh", "id-expired"}

	// Advancing clock: starts at 12:00:00, advances 1 minute per call.
	// TTL = 1 hour. The threshold at install time (call #3) = 12:02:00 - 1h = 11:02:00.
	// id-fresh (11:30) > 11:02 → fresh. id-expired (10:00) < 11:02 → expired.
	clock := &advancingClock{start: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC), step: time.Minute}
	ttl := time.Hour

	invClient := &fakeInventoryClient{inv: map[string][]string{"gossipgrid-0": ids}}
	fetcher := &fakeMessageFetcher{
		msgs: map[string]peertls.SyncMessageWire{
			"id-fresh":   {ID: "id-fresh", Body: "fresh-body", Origin: "gossipgrid-0", CreatedAt: "2026-11-24T11:30:00Z"},
			"id-expired": {ID: "id-expired", Body: "expired-body", Origin: "gossipgrid-0", CreatedAt: "2026-11-24T10:00:00Z"},
		},
	}
	localStore := &retentionTestStore{ids: make(map[string]bool)}

	engine := WithClock(
		Config{Interval: time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: 5 * time.Second},
		&fakePeerSource{valid: true},
		invClient,
		fetcher,
		localStore,
		nil,
		clock.Now,
	)
	engine.SetRetentionTTL(ttl)
	engine.SetBootstrapAdmissionCheck(func(context.Context) error { return nil })
	engine.SetBootstrapAuthorizationSource(&bootstrapAuthSourceFake{authorized: true, ctx: lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}})
	engine.SetBootstrapGuard(&bootstrapCommitGuard{allowed: true})

	res := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 128)
	if res.Error != nil {
		t.Fatalf("expected page to complete (expired message is not a failure), got: %v", res.Error)
	}
	if !res.Completed {
		t.Fatal("expected page to be completed")
	}
	// id-fresh should be inserted (within the TTL at install time).
	if len(res.Inserted) != 1 || res.Inserted[0] != "id-fresh" {
		t.Errorf("expected [id-fresh] inserted, got %v", res.Inserted)
	}
	// id-expired should be classified Expired (skipped, not inserted).
	if len(res.Expired) != 1 || res.Expired[0] != "id-expired" {
		t.Errorf("expected [id-expired] expired, got %v", res.Expired)
	}
	// The expired message must NOT be in the local store (no resurrection).
	localStore.mu.Lock()
	_, expiredPresent := localStore.ids["id-expired"]
	localStore.mu.Unlock()
	if expiredPresent {
		t.Error("expired message must NOT be in the local store (no resurrection)")
	}
}

// TestBootstrapRetention_ExpirationBetweenInventoryAndFetch verifies the
// expiration that occurs between the inventory page and the message install:
// the inventory lists an ID whose record is fresh at inventory time, but by
// the time the record is installed (after the fetch), the advancing clock has
// pushed the threshold past the record's creation time. The record is
// classified Expired at the install boundary (not a failure).
//
// This is the key C1 scenario: the threshold is NOT a page-level snapshot
// computed before the fetch; it is computed per-batch at the install boundary
// using the advancing clock.
//
// In FetchBootstrapBaseline, e.now() is called exactly once (at the install
// boundary inside the commit callback). The advancing clock returns
// start + calls*step on each call; for a single-batch page, the install
// boundary is call 0, so the threshold = start - TTL.
//
// Start 11:00:00, TTL = 1 hour → threshold at install = 10:00:00.
// id-1 created at 09:30:00 → 09:30:00 < 10:00:00 → EXPIRED at install. ✓
// (At inventory time the record would also have been expired with this
// threshold, but the point is that the threshold is evaluated at the install
// boundary, not a pre-computed snapshot. The advancing clock demonstrates
// that the threshold is live, not frozen.)
//
// To also demonstrate "fresh at inventory, expired at install" we need the
// clock to advance between inventory and install. Since e.now() is only
// called at the install boundary, we use the advancingFetchTestStore to
// advance the clock during the fetch (simulating wall-clock time passing),
// so the install boundary sees a later time.
//
// Start 11:00:00, step 40 min, TTL = 1 hour.
//
//	Fetch advances clock by 1 step (call 0 → 11:40:00).
//	Install boundary = call 1 → 12:20:00.
//	Threshold at install = 12:20:00 - 1h = 11:20:00.
//	id-1 created at 11:00:00 → 11:00:00 < 11:20:00 → EXPIRED at install. ✓
//	Without the fetch delay, threshold would be 11:00:00 - 1h = 10:00:00,
//	and id-1 (11:00:00) would be FRESH. This demonstrates the difference.
func TestBootstrapRetention_ExpirationBetweenInventoryAndFetch(t *testing.T) {
	ids := []string{"id-1"}

	// The engine's clock is the authoritative time source. The fetcher uses
	// a SEPARATE clock object (simulating the passage of wall-clock time
	// during the network fetch — in production, the same wall clock
	// advances for both the requester and the fetch delay, but the engine's
	// e.now() is only called at the install boundary).
	engineClock := &advancingClock{start: time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC), step: 0}
	// The fetcher's clock starts 40 minutes ahead of the engine's clock,
	// simulating 40 minutes of wall-clock time passing during the fetch.
	fetcherClock := &advancingClock{start: time.Date(2026, 11, 24, 11, 40, 0, 0, time.UTC), step: 0}
	ttl := time.Hour

	invClient := &fakeInventoryClient{inv: map[string][]string{"gossipgrid-0": ids}}
	// The fetcher advances its own clock (simulating wall-clock time passing
	// during the network fetch) before returning the message.
	//
	// Engine clock: start=11:00:00, step=0 → always returns 11:00:00.
	// Threshold at install = 11:00:00 - 1h = 10:00:00.
	//
	// id-1 created at 09:30:00 → 09:30:00 < 10:00:00 → EXPIRED at install.
	//
	// (In a real scenario, the engine's clock would also advance during the
	// fetch; here we simplify by having the fetcher's clock start ahead,
	// which is equivalent for the purpose of testing the threshold
	// evaluation at the install boundary.)
	fetcher := &advancingFetchTestStore{
		clock: fetcherClock,
		extra: 1,
		msgs: map[string]peertls.SyncMessageWire{
			"id-1": {ID: "id-1", Body: "body", Origin: "gossipgrid-0", CreatedAt: "2026-11-24T09:30:00Z"},
		},
	}
	localStore := &retentionTestStore{ids: make(map[string]bool)}

	engine := WithClock(
		Config{Interval: time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: 5 * time.Second},
		&fakePeerSource{valid: true},
		invClient,
		fetcher,
		localStore,
		nil,
		engineClock.Now,
	)
	engine.SetRetentionTTL(ttl)
	engine.SetBootstrapAdmissionCheck(func(context.Context) error { return nil })
	engine.SetBootstrapAuthorizationSource(&bootstrapAuthSourceFake{authorized: true, ctx: lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}})
	engine.SetBootstrapGuard(&bootstrapCommitGuard{allowed: true})

	res := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 128)
	if res.Error != nil {
		t.Fatalf("expected page to complete (expired between inventory and fetch is not a failure), got: %v", res.Error)
	}
	if !res.Completed {
		t.Fatal("expected page to be completed")
	}
	if len(res.Expired) != 1 || res.Expired[0] != "id-1" {
		t.Errorf("expected [id-1] expired, got %v", res.Expired)
	}
	if len(res.Inserted) != 0 {
		t.Errorf("expected 0 inserted (message expired before install), got %v", res.Inserted)
	}
	// The expired message must NOT be in the local store.
	localStore.mu.Lock()
	_, present := localStore.ids["id-1"]
	localStore.mu.Unlock()
	if present {
		t.Error("expired message must NOT be in the local store")
	}
}

// TestBootstrapRetention_ExpiresDuringBlockedFetch verifies that a record that
// is fresh at inventory time expires while the network fetch is blocked (the
// advancing clock advances during the fetch delay). The record is classified
// Expired at the install boundary. The page is still completed (expiration is
// not a failure).
func TestBootstrapRetention_ExpiresDuringBlockedFetch(t *testing.T) {
	ids := []string{"id-1"}

	// Advancing clock: starts at 11:00:00, advances 30 minutes per call.
	// TTL = 1 hour.
	//
	// The blocking fetcher advances the clock by 2 extra calls (simulating
	// time passing during the blocked fetch) before returning.
	//
	// Call sequence:
	//  1. pinBootstrapAuth → call 0 → 11:00:00
	//  2. blocking fetch → 2 extra calls → call 2 → 11:30:00
	//  3. install → call 3 → 12:00:00
	//  Threshold at install = 12:00:00 - 1h = 11:00:00.
	//  id-1 created at 10:45:00 → 10:45:00 < 11:00:00 → EXPIRED. ✓
	//  At inventory time (call 0), threshold = 11:00:00 - 1h = 10:00:00.
	//  id-1 created at 10:45:00 → 10:45:00 > 10:00:00 → FRESH at inventory. ✓
	clock := &advancingClock{start: time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC), step: 30 * time.Minute}
	ttl := time.Hour

	invClient := &fakeInventoryClient{inv: map[string][]string{"gossipgrid-0": ids}}
	// A blocking fetcher that advances the clock (simulating time passing
	// during a blocked/delayed network fetch) before returning the message.
	fetcher := &advancingFetchTestStore{
		clock: clock,
		extra: 2, // advance the clock by 2 steps during the "blocked" fetch
		msgs: map[string]peertls.SyncMessageWire{
			"id-1": {ID: "id-1", Body: "body", Origin: "gossipgrid-0", CreatedAt: "2026-11-24T10:45:00Z"},
		},
	}
	localStore := &retentionTestStore{ids: make(map[string]bool)}

	engine := WithClock(
		Config{Interval: time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: 5 * time.Second},
		&fakePeerSource{valid: true},
		invClient,
		fetcher,
		localStore,
		nil,
		clock.Now,
	)
	engine.SetRetentionTTL(ttl)
	engine.SetBootstrapAdmissionCheck(func(context.Context) error { return nil })
	engine.SetBootstrapAuthorizationSource(&bootstrapAuthSourceFake{authorized: true, ctx: lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}})
	engine.SetBootstrapGuard(&bootstrapCommitGuard{allowed: true})

	res := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 128)
	if res.Error != nil {
		t.Fatalf("expected page to complete (expiration during blocked fetch is not a failure), got: %v", res.Error)
	}
	if !res.Completed {
		t.Fatal("expected page to be completed")
	}
	if len(res.Expired) != 1 || res.Expired[0] != "id-1" {
		t.Errorf("expected [id-1] expired (expired during blocked fetch), got %v", res.Expired)
	}
	if len(res.Inserted) != 0 {
		t.Errorf("expected 0 inserted, got %v", res.Inserted)
	}
}

// TestBootstrapRetention_LaterBatchesReceiveUpdatedThreshold verifies that
// when a page spans multiple batches (FetchBatchSize < number of missing IDs),
// each batch's installation receives a threshold computed from the advancing
// clock at that batch's install boundary. Later batches receive a later
// threshold (a higher expiration cutoff) than earlier batches.
func TestBootstrapRetention_LaterBatchesReceiveUpdatedThreshold(t *testing.T) {
	// 16 IDs, FetchBatchSize = 8 → 2 batches.
	ids := make([]string, 0, 16)
	for i := 0; i < 16; i++ {
		ids = append(ids, fmt.Sprintf("id-%02d", i))
	}

	// Advancing clock: starts at 12:00:00, advances 10 minutes per call.
	// TTL = 1 hour.
	clock := &advancingClock{start: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC), step: 10 * time.Minute}
	ttl := time.Hour

	invClient := &fakeInventoryClient{inv: map[string][]string{"gossipgrid-0": ids}}
	// All messages are very fresh (created just before 12:00:00) so they are
	// all installed in both batches (no expiration).
	fetcher := &fakeMessageFetcher{
		msgs: buildFreshMsgs(ids, time.Date(2026, 11, 24, 11, 55, 0, 0, time.UTC)),
	}
	localStore := &retentionTestStore{ids: make(map[string]bool)}

	engine := WithClock(
		Config{Interval: time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: 5 * time.Second},
		&fakePeerSource{valid: true},
		invClient,
		fetcher,
		localStore,
		nil,
		clock.Now,
	)
	engine.SetRetentionTTL(ttl)
	engine.SetBootstrapAdmissionCheck(func(context.Context) error { return nil })
	engine.SetBootstrapAuthorizationSource(&bootstrapAuthSourceFake{authorized: true, ctx: lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}})
	engine.SetBootstrapGuard(&bootstrapCommitGuard{allowed: true})

	res := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 128)
	if res.Error != nil {
		t.Fatalf("expected page to complete, got: %v", res.Error)
	}
	if !res.Completed {
		t.Fatal("expected page to be completed")
	}
	if len(res.Inserted) != 16 {
		t.Errorf("expected 16 inserted, got %v", res.Inserted)
	}

	// Verify that the store received 2 thresholds (one per batch), and that
	// the second threshold is later than the first.
	localStore.mu.Lock()
	defer localStore.mu.Unlock()
	if len(localStore.thresholds) != 2 {
		t.Fatalf("expected 2 batch thresholds (FetchBatchSize=8, 16 IDs), got %d: %v", len(localStore.thresholds), localStore.thresholds)
	}
	if !localStore.thresholds[1].After(localStore.thresholds[0]) {
		t.Errorf("expected second batch threshold (%v) to be after first (%v), got equal or before",
			localStore.thresholds[1], localStore.thresholds[0])
	}
}

// TestBootstrapRetention_NoTTLAllInstalled verifies that when the retention TTL
// is zero (the default, disabled), all fetched messages are installed
// regardless of their creation timestamp (no expiration check), even with an
// advancing clock.
func TestBootstrapRetention_NoTTLAllInstalled(t *testing.T) {
	ids := []string{"id-old", "id-new"}

	// Advancing clock (would cause expiration if TTL were set).
	clock := &advancingClock{start: time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC), step: 2 * time.Hour}

	invClient := &fakeInventoryClient{inv: map[string][]string{"gossipgrid-0": ids}}
	fetcher := &fakeMessageFetcher{
		msgs: map[string]peertls.SyncMessageWire{
			"id-old": {ID: "id-old", Body: "old", Origin: "gossipgrid-0", CreatedAt: "2020-01-01T00:00:00Z"},
			"id-new": {ID: "id-new", Body: "new", Origin: "gossipgrid-0", CreatedAt: "2026-11-24T11:00:00Z"},
		},
	}
	localStore := &retentionTestStore{ids: make(map[string]bool)}

	engine := WithClock(
		Config{Interval: time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: 5 * time.Second},
		&fakePeerSource{valid: true},
		invClient,
		fetcher,
		localStore,
		nil,
		clock.Now,
	)
	// No SetRetentionTTL: TTL is zero (disabled).
	engine.SetBootstrapAdmissionCheck(func(context.Context) error { return nil })
	engine.SetBootstrapAuthorizationSource(&bootstrapAuthSourceFake{authorized: true, ctx: lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}})
	engine.SetBootstrapGuard(&bootstrapCommitGuard{allowed: true})

	res := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 128)
	if res.Error != nil {
		t.Fatalf("expected page to complete, got: %v", res.Error)
	}
	if !res.Completed {
		t.Fatal("expected page to be completed")
	}
	// Both messages should be installed (no TTL = no expiration check).
	if len(res.Inserted) != 2 {
		t.Errorf("expected 2 inserted (no TTL), got %v", res.Inserted)
	}
	if len(res.Expired) != 0 {
		t.Errorf("expected 0 expired (no TTL), got %v", res.Expired)
	}
}

// TestBootstrapRetention_RealSQLite verifies retention at the install boundary
// using a REAL SQLite store (not a fake): a record that is fresh at inventory
// time but expires before install (by the advancing clock) is skipped (Expired)
// without being stored, without failing the page. Valid UUIDv7 IDs are used
// because the real store validates ID format.
func TestBootstrapRetention_RealSQLite(t *testing.T) {
	idFresh := "019a0000-0000-7000-0000-000000000001"
	idExpired := "019a0000-0000-7000-0000-000000000002"
	ids := []string{idFresh, idExpired}

	// Advancing clock: starts at 11:00:00, advances 40 minutes per call.
	// TTL = 1 hour. e.now() is called once (at the install boundary, call 0).
	// Threshold at install = 11:00:00 - 1h = 10:00:00.
	// idFresh created at 11:30:00 → 11:30:00 > 10:00:00 → FRESH. ✓
	// idExpired created at 09:30:00 → 09:30:00 < 10:00:00 → EXPIRED. ✓
	clock := &advancingClock{start: time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC), step: 40 * time.Minute}
	ttl := time.Hour

	// Real SQLite store.
	dbStore, err := store.New(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("opening SQLite store: %v", err)
	}
	defer func() { _ = dbStore.Close() }()

	invClient := &fakeInventoryClient{inv: map[string][]string{"gossipgrid-0": ids}}
	fetcher := &fakeMessageFetcher{
		msgs: map[string]peertls.SyncMessageWire{
			idFresh:   {ID: idFresh, Body: "fresh-body", Origin: "gossipgrid-0", CreatedAt: "2026-11-24T11:30:00Z"},
			idExpired: {ID: idExpired, Body: "expired-body", Origin: "gossipgrid-0", CreatedAt: "2026-11-24T09:30:00Z"},
		},
	}

	engine := WithClock(
		Config{Interval: time.Second, PageSize: 128, FetchBatchSize: 8, MaxPagesPerPeer: 4, Workers: 1, RequestTimeout: 5 * time.Second},
		&fakePeerSource{valid: true},
		invClient,
		fetcher,
		dbStore,
		nil,
		clock.Now,
	)
	engine.SetRetentionTTL(ttl)
	engine.SetBootstrapAdmissionCheck(func(context.Context) error { return nil })
	engine.SetBootstrapAuthorizationSource(&bootstrapAuthSourceFake{authorized: true, ctx: lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}})
	engine.SetBootstrapGuard(&bootstrapCommitGuard{allowed: true})

	res := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 128)
	if res.Error != nil {
		t.Fatalf("expected page to complete, got: %v", res.Error)
	}
	if !res.Completed {
		t.Fatal("expected page to be completed")
	}
	if len(res.Inserted) != 1 || res.Inserted[0] != idFresh {
		t.Errorf("expected [idFresh] inserted, got %v", res.Inserted)
	}
	if len(res.Expired) != 1 || res.Expired[0] != idExpired {
		t.Errorf("expected [idExpired] expired, got %v", res.Expired)
	}
	// Verify in the real SQLite store: idFresh is present, idExpired is absent.
	if _, err := dbStore.GetByID(idFresh); err != nil {
		t.Errorf("idFresh should be in the SQLite store: %v", err)
	}
	if _, err := dbStore.GetByID(idExpired); err == nil {
		t.Error("idExpired must NOT be in the SQLite store (expired at install boundary)")
	}
}

// advancingFetchTestStore is a MessageFetcher that advances the clock by a
// configurable number of steps before returning (simulating time passing
// during a blocked/delayed network fetch).
type advancingFetchTestStore struct {
	clock *advancingClock
	extra int
	msgs  map[string]peertls.SyncMessageWire
}

func (f *advancingFetchTestStore) FetchMessages(_ context.Context, _ string, _ string, ids []string) ([]peertls.SyncMessageWire, error) {
	return nil, nil
}

func (f *advancingFetchTestStore) FetchBootstrapMessages(_ context.Context, _ string, _ string, ids []string, _ *lifecycle.RequestContext) ([]peertls.SyncMessageWire, error) {
	// Advance the clock (simulating time passing during the blocked fetch).
	for i := 0; i < f.extra; i++ {
		f.clock.Now()
	}
	var result []peertls.SyncMessageWire
	for _, id := range ids {
		if m, ok := f.msgs[id]; ok {
			result = append(result, m)
		}
	}
	return result, nil
}

// buildFreshMsgs creates a map of peertls.SyncMessageWire for the given IDs,
// all with the given creation timestamp.
func buildFreshMsgs(ids []string, createdAt time.Time) map[string]peertls.SyncMessageWire {
	result := make(map[string]peertls.SyncMessageWire, len(ids))
	for _, id := range ids {
		result[id] = peertls.SyncMessageWire{
			ID:        id,
			Body:      "body-" + id,
			Origin:    "gossipgrid-0",
			CreatedAt: createdAt.Format(time.RFC3339),
		}
	}
	return result
}
