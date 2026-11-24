// Package readiness holds the narrow, shared types that express the
// synchronization-aware readiness barrier (Milestone 7A enhancement) to the
// readiness handler and the web diagnostics, without the server and
// anti-entropy packages importing each other (which would form an import
// cycle: server -> antientropy -> peertls, and peertls's test imports server).
//
// The barrier is the single linearization point where the anti-entropy engine
// publishes a verified full inventory pass, and the readiness handler / web
// diagnostics query it. Both the engine (antientropy) and the HTTP layer
// (server, web) depend on these types, so they live in a leaf package that
// imports nothing from the application.
package readiness

// Status is a point-in-time, concurrency-safe view of the initial-sync
// barrier for diagnostics and the /readyz response. It is safe to expose in
// the /readyz and /api/diagnostics responses: it carries only peer
// identities and counts, never credentials, private keys, or message
// contents.
type Status struct {
	Complete     bool     `json:"complete"`
	State        string   `json:"state"` // "complete" | "pending" | "disabled"
	Reason       string   `json:"reason,omitempty"`
	Completed    int      `json:"completed"`
	Expected     int      `json:"expected"`
	MissingPeers []string `json:"missing_peers,omitempty"`
}

// TrackerProvider is the narrow interface the readiness handler and web
// diagnostics use to query the initial-sync barrier. It is implemented by the
// anti-entropy InitialSyncTracker (including the nil receiver, which
// represents the disabled state: Complete() returns true and Status()
// returns the disabled/complete view). The interface lets the server and web
// packages query the barrier without depending on the concrete tracker type
// (and without creating an import cycle with the anti-entropy package).
type TrackerProvider interface {
	// Complete reports whether the barrier is satisfied. A nil provider
	// (disabled) is treated as complete (the barrier imposes no constraint).
	Complete() bool
	// Enabled reports whether the barrier is active (a non-nil tracker).
	Enabled() bool
	// Status returns a concurrency-safe point-in-time view of the barrier
	// state for diagnostics. A nil provider returns the disabled/complete
	// view.
	Status() Status
}
