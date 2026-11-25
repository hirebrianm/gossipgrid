// Package lifecycle implements the replica lifecycle state machine for
// GossipGrid (Milestone 5A).
//
// The state machine tracks whether a replica is authorized to participate in
// a valid cluster epoch. It is concurrency-safe, rejects invalid transitions,
// and records transition reasons and timestamps for diagnostics.
//
// The package is opt-in: when lifecycle protection is disabled (the default),
// the Controller is not created and all existing Milestone 4 behavior is
// preserved.
//
// The state machine is a small, focused implementation — not a generic
// workflow framework. Permitted transitions are defined explicitly in
// transitionTable; every other transition is rejected with an error.
//
// Key properties:
//   - No caller can silently force READY: the only path to READY is through
//     explicit controller methods that validate the current state.
//   - GENESIS is never inferred from an empty database.
//   - READY is never inferred from a started listener or a completed
//     anti-entropy page.
//   - RECOVERY_REQUIRED does not imply data has been permanently lost.
//   - Transitions are concurrency-safe: concurrent Snapshot() calls never
//     observe an inconsistent state, and concurrent transitions are
//     serialized.
package lifecycle

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State is the lifecycle state of a GossipGrid replica.
type State int

// Lifecycle states.
const (
	// STARTING: local SQLite, configuration, and credentials are initializing.
	STARTING State = iota

	// DISCOVERING: local dependencies are available, but the replica has not
	// established authorization to participate in a valid cluster epoch.
	DISCOVERING

	// GENESIS: explicitly authorized first initialization. This state is
	// entered only by an explicit controller method, not by an inference
	// from missing peers or empty data.
	GENESIS

	// SYNCING: the replica has an authorized epoch and is obtaining an
	// initial data baseline. It is not an authoritative bootstrap source.
	SYNCING

	// READY: the replica has established its permitted epoch and completed
	// the required initialization criteria. It may accept normal requests and
	// supply synchronization data.
	READY

	// DEGRADED: the replica previously became READY and retains valid local
	// authority but is experiencing a peer or convergence failure. It may
	// remain ready when the problem is only peer availability.
	DEGRADED

	// RECOVERY_REQUIRED: the replica cannot safely establish or continue
	// participation. Examples include epoch mismatch, uncertain metadata,
	// incompatible local database binding, or a known cluster without an
	// eligible recovery source.
	RECOVERY_REQUIRED
)

var stateNames = map[State]string{
	STARTING:          "STARTING",
	DISCOVERING:       "DISCOVERING",
	GENESIS:           "GENESIS",
	SYNCING:           "SYNCING",
	READY:             "READY",
	DEGRADED:          "DEGRADED",
	RECOVERY_REQUIRED: "RECOVERY_REQUIRED",
}

// String returns the stable, human-readable name of the state.
func (s State) String() string {
	if name, ok := stateNames[s]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN(%d)", int(s))
}

// IsReady reports whether the state is an eligible state for serving
// readiness (200 on /readyz). READY and DEGRADED are ready; all others
// are not.
func (s State) IsReady() bool {
	return s == READY || s == DEGRADED
}

// IsBootstrapSourceEligible reports whether a replica in this state is
// eligible to supply an authoritative bootstrap snapshot to other unsynced
// replicas. A replica in a ready state (READY or DEGRADED) is eligible: DEGRADED
// retains valid local authority (it was previously READY and is only experiencing
// a peer or convergence failure), so it can still serve a same-epoch baseline.
// All non-ready states (STARTING, DISCOVERING, GENESIS, SYNCING,
// RECOVERY_REQUIRED) are never eligible — in particular, a SYNCING replica is
// obtaining its own baseline and must not serve one to others.
//
// This is a state-only predicate: it does not itself check the durable binding,
// the authoritative observation, or authorization. The serving-side check
// (the peer status handler) combines it with the full authorization snapshot
// (fresh, valid, matching observation + matching binding, not stopped) so a
// peer reported eligible is one the handlers would authorize to serve.
func (s State) IsBootstrapSourceEligible() bool {
	return s.IsReady()
}

// transitionRecord is an immutable record of a state transition.
type transitionRecord struct {
	From      State
	To        State
	Reason    string
	Timestamp time.Time
}

// ErrInvalidTransition is returned when a state transition is not permitted
// by the transition table.
var ErrInvalidTransition = errors.New("invalid lifecycle state transition")

// ErrNoTransition is returned when a transition method is called from a state
// that has no applicable transition to the target.
var ErrNoTransition = errors.New("no transition available from current state")

// TransitionLog is a bounded, concurrency-safe log of state transitions.
// It holds up to maxTransitionLogEntries entries (newest first).
type TransitionLog struct {
	mu      sync.RWMutex
	entries []transitionRecord
}

const maxTransitionLogEntries = 64

// Add records a transition.
func (l *TransitionLog) Add(rec transitionRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append([]transitionRecord{rec}, l.entries...)
	if len(l.entries) > maxTransitionLogEntries {
		l.entries = l.entries[:maxTransitionLogEntries]
	}
}

// Snapshot returns a copy of the transition log (newest first).
func (l *TransitionLog) Snapshot() []transitionRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]transitionRecord, len(l.entries))
	copy(out, l.entries)
	return out
}

// StateSnapshot is an immutable, concurrency-safe view of the current
// lifecycle state and diagnostic information.
type StateSnapshot struct {
	State         State
	Reason        string
	Timestamp     time.Time
	Transitions   []transitionRecord
	PreviousState State
}

// transitionTable defines the permitted state transitions. The key is the
// from-state; the value is the set of to-states that are permitted.
var transitionTable = map[State]map[State]bool{
	STARTING: {
		DISCOVERING:       true,
		RECOVERY_REQUIRED: true,
	},
	DISCOVERING: {
		GENESIS:           true,
		SYNCING:           true,
		RECOVERY_REQUIRED: true,
		READY:             true,
	},
	GENESIS: {
		READY:             true,
		RECOVERY_REQUIRED: true,
	},
	SYNCING: {
		DISCOVERING:       true,
		READY:             true,
		RECOVERY_REQUIRED: true,
	},
	READY: {
		DEGRADED:          true,
		RECOVERY_REQUIRED: true,
	},
	DEGRADED: {
		READY:             true,
		RECOVERY_REQUIRED: true,
	},
	RECOVERY_REQUIRED: {
		DISCOVERING: true,
	},
}

// canTransition reports whether a transition from to is permitted.
func canTransition(from, to State) bool {
	if from == to {
		return false
	}
	targets, ok := transitionTable[from]
	if !ok {
		return false
	}
	return targets[to]
}

// Machine is the core lifecycle state machine. It is safe for concurrent use.
type Machine struct {
	mu            sync.RWMutex
	state         State
	reason        string
	timestamp     time.Time
	previous      State
	transitionLog TransitionLog
	now           func() time.Time
}

// NewMachine creates a state machine in the STARTING state.
func NewMachine(now func() time.Time) *Machine {
	if now == nil {
		now = time.Now
	}
	return &Machine{
		state:     STARTING,
		timestamp: now(),
		now:       now,
	}
}

// Snapshot returns a concurrency-safe, immutable view of the current state.
//
// Lock-order contract: this method acquires m.mu RLock on its own. When
// called from a context that already holds c.mu RLock (the Controller's
// Snapshot and authorization methods), the caller must acquire m.mu RLock
// first and use snapshotLocked instead, to maintain the c.mu → m.mu lock
// order and avoid a deadlock with a concurrent transition (which acquires
// c.mu RLock then m.mu Lock).
func (m *Machine) Snapshot() StateSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshotLocked()
}

// snapshotLocked returns a StateSnapshot without acquiring m.mu. It must be
// called with m.mu RLock already held. It is used by the Controller's
// Snapshot and authorization methods, which already hold both c.mu RLock and
// m.mu RLock in the correct lock order (c.mu → m.mu).
func (m *Machine) snapshotLocked() StateSnapshot {
	return StateSnapshot{
		State:         m.state,
		Reason:        m.reason,
		Timestamp:     m.timestamp,
		Transitions:   m.transitionLog.Snapshot(),
		PreviousState: m.previous,
	}
}

// State returns the current state (concurrency-safe).
func (m *Machine) State() State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// transition performs a validated state transition. It is called by the
// Controller's exported methods; it is not exported to prevent arbitrary
// state forcing.
//
// Lock-order contract: this method acquires m.mu Lock on its own. When called
// from a context that already holds c.mu RLock (the Controller's Set* methods
// and reevaluateState), the caller must acquire m.mu Lock after c.mu RLock,
// maintaining the c.mu → m.mu order. This ensures that a transition is
// serialized with the commit path (which holds c.mu RLock + m.mu RLock):
// a transition that starts before the commit is observed, and a transition
// that starts after the commit is queued behind the commit's RLocks.
func (m *Machine) transition(to State, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.transitionLocked(to, reason)
}

// transitionLocked performs a validated state transition without acquiring
// m.mu. It must be called with m.mu Lock already held. It is used by the
// Controller's Set* methods, which acquire c.mu RLock then m.mu Lock in the
// correct lock order (c.mu → m.mu).
func (m *Machine) transitionLocked(to State, reason string) error {
	if !canTransition(m.state, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, m.state.String(), to.String())
	}

	m.previous = m.state
	m.state = to
	m.reason = reason
	m.timestamp = m.now()
	m.transitionLog.Add(transitionRecord{
		From:      m.previous,
		To:        to,
		Reason:    reason,
		Timestamp: m.timestamp,
	})
	return nil
}

// forceTransition is used only during initial setup (before any external
// interaction) to set the starting state. It bypasses the transition table
// because the machine is created in STARTING and the initial state is
// always STARTING.
//
// Lock-order contract: acquires m.mu Lock on its own. When called from a
// context that already holds c.mu RLock, the caller must acquire m.mu Lock
// after c.mu RLock (c.mu → m.mu order).
func (m *Machine) forceTransition(to State, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forceTransitionLocked(to, reason)
}

// forceTransitionLocked is the lock-free variant of forceTransition. It must
// be called with m.mu Lock already held.
func (m *Machine) forceTransitionLocked(to State, reason string) {
	m.previous = m.state
	m.state = to
	m.reason = reason
	m.timestamp = m.now()
	m.transitionLog.Add(transitionRecord{
		From:      m.previous,
		To:        to,
		Reason:    reason,
		Timestamp: m.timestamp,
	})
}

// ForceTransitionForTest is an exported test-only method that bypasses the
// transition table. It is used by tests in other packages to set up specific
// initial states. Production code must NOT call this; it exists solely to
// support cross-package testing.
func (m *Machine) ForceTransitionForTest(to State, reason string) {
	m.forceTransition(to, reason)
}

// TransitionForTest is an exported test-only method that performs a validated
// transition. It is used by tests in other packages to advance the state
// machine. Production code must NOT call this; it exists solely to support
// cross-package testing.
func (m *Machine) TransitionForTest(to State, reason string) error {
	return m.transition(to, reason)
}
