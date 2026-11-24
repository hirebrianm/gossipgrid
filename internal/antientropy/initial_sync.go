package antientropy

import (
	"sync"
	"time"

	"gossipgrid/internal/readiness"
)

// InitialSyncTracker is a concurrency-safe, in-memory tracker for the
// opt-in initial anti-entropy synchronization barrier (Milestone 7A
// enhancement, synchronization-aware Kubernetes readiness).
//
// It answers one question at readiness time: has this replica, since its
// process started, completed a successful full anti-entropy inventory pass
// against every expected remote peer in the configured StatefulSet
// membership? That answer is the single condition that holds the pod in
// NotReady (HTTP 503 from /readyz) until the barrier is satisfied.
//
// Design contract:
//
//   - Begin pending on every process start: New creates a tracker in the
//     pending state. A fresh process has not synchronized with anyone.
//   - One-shot: RecordComplete can only transition pending -> complete.
//     Once complete, it cannot be undone. A transient peer outage after the
//     barrier is satisfied does NOT re-open it (no readiness cascade).
//   - No timeout: the tracker never marks itself complete because a deadline
//     elapsed. Only a verified full sweep sets the complete flag.
//   - No network, no SQLite: every method reads or writes in-memory state
//     under a single mutex or atomic. The readiness handler must not
//     perform synchronous network requests or SQLite inventory scans, and
//     this tracker is the component it queries.
//   - Bounded: expected IDs and per-peer progress are bounded by the
//     configured membership (a few dozen entries). No per-request allocation
//     of unbounded structures.
//
// A completed inventory pass establishes a useful rollout barrier — the
// replacement replica has retrieved the surviving peers' message inventory
// (and fetched the missing records) before Kubernetes marks it Ready. It is
// NOT a mathematically atomic cluster snapshot: messages written to a peer
// after that peer's last full sweep is observed here will not be in this
// replica's local store until the next anti-entropy cycle runs. The barrier
// guarantees recovery from the surviving peers' inventories as of the last
// full sweep, not a frozen point-in-time view of the whole cluster.
type InitialSyncTracker struct {
	expected []string // the configured StatefulSet pod identities (all replicas)

	mu           sync.Mutex
	complete     bool
	completeAt   time.Time
	completedSet map[string]bool // expected remote IDs with a verified full pass

	// diagnostics (not readiness-relevant; bounded by membership).
	lastReason string
}

// NewInitialSyncTracker creates a tracker in the pending state with the given
// expected StatefulSet membership. The local replica identity (localID) is
// excluded from the set of expected remote peers: a single-replica
// deployment (len(expected) == 1) has no remote peers, so the barrier is
// vacuously satisfied (Complete() returns true immediately).
func NewInitialSyncTracker(expected []string, localID string) *InitialSyncTracker {
	remote := make(map[string]bool)
	for _, id := range expected {
		if id == "" {
			continue
		}
		if id == localID {
			continue // exclude the local replica
		}
		remote[id] = true
	}
	// Preserve deterministic ordering for diagnostics (not required for
	// correctness, but keeps the missing list stable in tests and logs).
	expectedRemote := make([]string, 0, len(remote))
	for id := range remote {
		expectedRemote = append(expectedRemote, id)
	}
	sortStrings(expectedRemote)

	t := &InitialSyncTracker{
		expected:     expectedRemote,
		completedSet: make(map[string]bool),
	}
	// A single-replica deployment (no remote peers) has a vacuously-satisfied
	// barrier: there is no one to synchronize with, so the pod may become
	// Ready on its own (scenario 8). The same applies to an empty/misconfigured
	// expected membership. The barrier only constrains readiness when there is
	// at least one expected remote peer.
	if len(expectedRemote) == 0 {
		t.complete = true
		t.completeAt = time.Now()
		t.lastReason = "no remote peers; barrier vacuously satisfied"
	}
	return t
}

// RecordComplete records a verified full anti-entropy inventory pass for the
// given peer. It is the single linearization point where a completed sweep
// (has_more=false, no unresolved conflict or error — the same accounting the
// engine uses for CompletedPasses) is published to the barrier. It is called
// from the anti-entropy engine's worker (syncPeer) immediately after the
// engine has committed the full-sweep cursor and incremented CompletedPasses.
//
// RecordComplete only adds the peer to the completed set and, if that was the
// last outstanding expected peer, flips the complete flag. It never removes a
// peer from the set and never flips the flag back to pending (one-shot). It
// returns true exactly once — for the single call that transitions the
// barrier from pending to complete (the pod may now become Ready, subject to
// lifecycle authorization). Subsequent calls, for any peer (including the
// peer that completed the barrier and any unexpected peer), return false
// because the barrier is already complete.
//
// It must be called with a peer ID that is part of the configured expected
// membership: a peer discovered via DNS that is not in the expected
// StatefulSet membership (e.g. a stale record from a scaled-down cluster)
// must NOT count toward the barrier, because the barrier is defined by the
// configured membership, not the current DNS view.
func (t *InitialSyncTracker) RecordComplete(peerID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.complete {
		// The barrier is already complete (one-shot). This call did not
		// transition it; the single transition returned true on the call
		// that flipped pending -> complete.
		return false
	}
	if !t.isExpected(peerID) {
		// A peer outside the expected membership does not count.
		t.lastReason = "peer " + peerID + " not in expected membership"
		return false
	}
	if t.completedSet[peerID] {
		// Already recorded for this peer; the barrier did not transition
		// on this call (it would have transitioned when the last
		// outstanding peer was recorded).
		return false
	}
	t.completedSet[peerID] = true
	t.lastReason = "initial synchronization in progress"
	if t.allExpectedComplete() {
		t.complete = true
		t.completeAt = time.Now()
		t.lastReason = "initial synchronization complete"
		return true
	}
	return false
}

// Complete reports whether the initial barrier is satisfied. When the tracker
// is disabled (nil receiver, see (*InitialSyncTracker).Enabled), it is
// treated as complete: the barrier imposes no constraint. This lets callers
// use a single expression (lifecycleAuthorized && tracker.Complete()) without
// a nil check in the hot path.
func (t *InitialSyncTracker) Complete() bool {
	if t == nil {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.complete
}

// Enabled reports whether the barrier is active (a non-nil tracker). A nil
// tracker means the barrier is disabled and imposes no constraint.
func (t *InitialSyncTracker) Enabled() bool {
	return t != nil
}

// Status returns a concurrency-safe point-in-time view of the barrier state
// for diagnostics and the /readyz response. It reports the barrier state,
// the reason, the count of completed expected peers, the total expected
// remote peers, and the set of peers still outstanding (the missing list).
//
// No network or SQLite access is performed. The missing list is bounded by
// the configured membership. The status type is readiness.Status (a leaf
// package shared with the server and web packages) so those packages can
// query the barrier without importing antientropy (which would form an
// import cycle).
func (t *InitialSyncTracker) Status() readiness.Status {
	if t == nil {
		return readiness.Status{Complete: true, State: "disabled"}
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	missing := make([]string, 0, len(t.expected))
	completed := 0
	for _, id := range t.expected {
		if t.completedSet[id] {
			completed++
		} else {
			missing = append(missing, id)
		}
	}

	state := "complete"
	if !t.complete {
		state = "pending"
	}
	return readiness.Status{
		Complete:     t.complete,
		State:        state,
		Reason:       t.lastReason,
		Completed:    completed,
		Expected:     len(t.expected),
		MissingPeers: missing,
	}
}

// isExpected reports whether the given peer ID is in the configured expected
// membership. It must be called with t.mu held.
func (t *InitialSyncTracker) isExpected(peerID string) bool {
	for _, id := range t.expected {
		if id == peerID {
			return true
		}
	}
	return false
}

// allExpectedComplete reports whether every expected remote peer has a
// verified full pass. It must be called with t.mu held.
func (t *InitialSyncTracker) allExpectedComplete() bool {
	for _, id := range t.expected {
		if !t.completedSet[id] {
			return false
		}
	}
	return true
}

// sortStrings sorts a slice of strings in place (ascending).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// InitialSyncStatus is an alias for readiness.Status, the point-in-time,
// concurrency-safe view of the initial-sync barrier for diagnostics. The
// concrete type lives in the readiness leaf package (shared with the server
// and web packages) so those packages can query the barrier without
// importing antientropy (which would form an import cycle). It is safe to
// expose in the /readyz and /api/diagnostics responses: it carries only peer
// identities and counts, never credentials, private keys, or message
// contents.
type InitialSyncStatus = readiness.Status

// InitialSyncTrackerProvider is an alias for readiness.TrackerProvider, the
// narrow interface the readiness handler and web diagnostics use to query the
// initial-sync barrier. It is implemented by *InitialSyncTracker (including
// the nil receiver, which represents the disabled state: Complete() returns
// true and Status() returns the disabled/complete view). The interface lives
// in the readiness leaf package so the server and web packages can query the
// barrier without depending on antientropy (which would form an import
// cycle).
type InitialSyncTrackerProvider = readiness.TrackerProvider

// NewInitialSyncBarrier wraps the tracker construction with a config gate.
// It returns a nil tracker (disabled) when enabled is false, or a tracker
// initialized from the expected membership and local identity when enabled
// is true. The nil return value is the "disabled" sentinel: a nil tracker
// imposes no constraint on readiness (Complete() returns true for nil).
//
// This helper exists so the caller (main.go) has a single expression that
// reflects the config flag, rather than an if/else at every construction
// site.
func NewInitialSyncBarrier(enabled bool, expected []string, localID string) *InitialSyncTracker {
	if !enabled {
		return nil
	}
	return NewInitialSyncTracker(expected, localID)
}

// Compile-time assertion that *InitialSyncTracker satisfies the narrow
// InitialSyncTrackerProvider interface used by the readiness handler and
// web diagnostics. A signature change in either breaks the build.
var _ InitialSyncTrackerProvider = (*InitialSyncTracker)(nil)
