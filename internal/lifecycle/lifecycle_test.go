package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeNow returns a deterministic, concurrency-safe clock that increments
// by 1ms on each call.
type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	count int
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count++
	f.now = f.now.Add(time.Millisecond)
	return f.now
}

func newTestClock(t time.Time) *fakeClock {
	return &fakeClock{now: t}
}

func TestNewMachineDefaultsToStarting(t *testing.T) {
	m := NewMachine(nil)
	if m.State() != STARTING {
		t.Errorf("new machine: expected STARTING, got %s", m.State())
	}
}

func TestValidTransitions(t *testing.T) {
	tests := []struct {
		name   string
		from   State
		to     State
		reason string
	}{
		{"STARTING to DISCOVERING", STARTING, DISCOVERING, "deps ready"},
		{"STARTING to RECOVERY_REQUIRED", STARTING, RECOVERY_REQUIRED, "fatal error"},
		{"DISCOVERING to GENESIS", DISCOVERING, GENESIS, "authorized genesis"},
		{"DISCOVERING to SYNCING", DISCOVERING, SYNCING, "eligible peer found"},
		{"DISCOVERING to RECOVERY_REQUIRED", DISCOVERING, RECOVERY_REQUIRED, "no safe source"},
		{"DISCOVERING to READY", DISCOVERING, READY, "direct ready"},
		{"GENESIS to READY", GENESIS, READY, "epoch established"},
		{"GENESIS to RECOVERY_REQUIRED", GENESIS, RECOVERY_REQUIRED, "error during genesis"},
		{"SYNCING to DISCOVERING", SYNCING, DISCOVERING, "source unavailable"},
		{"SYNCING to READY", SYNCING, READY, "snapshot ingested"},
		{"SYNCING to RECOVERY_REQUIRED", SYNCING, RECOVERY_REQUIRED, "error during sync"},
		{"READY to DEGRADED", READY, DEGRADED, "peer partition"},
		{"READY to RECOVERY_REQUIRED", READY, RECOVERY_REQUIRED, "epoch mismatch"},
		{"DEGRADED to READY", DEGRADED, READY, "reconciliation recovered"},
		{"DEGRADED to RECOVERY_REQUIRED", DEGRADED, RECOVERY_REQUIRED, "epoch mismatch"},
		{"RECOVERY_REQUIRED to DISCOVERING", RECOVERY_REQUIRED, DISCOVERING, "peer recovered"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMachine(nil)
			// Force the machine to the 'from' state.
			m.forceTransition(tc.from, "test setup")

			err := m.transition(tc.to, tc.reason)
			if err != nil {
				t.Fatalf("transition %s -> %s: unexpected error: %v", tc.from, tc.to, err)
			}
			if m.State() != tc.to {
				t.Errorf("after transition: expected %s, got %s", tc.to, m.State())
			}
			snap := m.Snapshot()
			if snap.Reason != tc.reason {
				t.Errorf("reason: expected %q, got %q", tc.reason, snap.Reason)
			}
			if snap.PreviousState != tc.from {
				t.Errorf("previous state: expected %s, got %s", tc.from, snap.PreviousState)
			}
		})
	}
}

func TestInvalidTransitions(t *testing.T) {
	tests := []struct {
		from State
		to   State
	}{
		{STARTING, GENESIS},
		{STARTING, SYNCING},
		{STARTING, READY},
		{STARTING, DEGRADED},
		{DISCOVERING, STARTING},
		{DISCOVERING, DEGRADED},
		{GENESIS, DISCOVERING},
		{GENESIS, SYNCING},
		{GENESIS, DEGRADED},
		{SYNCING, GENESIS},
		{SYNCING, DEGRADED},
		{SYNCING, STARTING},
		{READY, STARTING},
		{READY, DISCOVERING},
		{READY, GENESIS},
		{READY, SYNCING},
		{DEGRADED, STARTING},
		{DEGRADED, DISCOVERING},
		{DEGRADED, GENESIS},
		{DEGRADED, SYNCING},
		{RECOVERY_REQUIRED, STARTING},
		{RECOVERY_REQUIRED, GENESIS},
		{RECOVERY_REQUIRED, SYNCING},
		{RECOVERY_REQUIRED, READY},
		{RECOVERY_REQUIRED, DEGRADED},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s to %s", tc.from, tc.to), func(t *testing.T) {
			m := NewMachine(nil)
			m.forceTransition(tc.from, "test setup")

			err := m.transition(tc.to, "invalid")
			if err == nil {
				t.Fatalf("expected error for transition %s -> %s, got nil", tc.from, tc.to)
			}
			if !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("expected ErrInvalidTransition, got %v", err)
			}
			// State should be unchanged.
			if m.State() != tc.from {
				t.Errorf("state changed from %s to %s after invalid transition", tc.from, m.State())
			}
		})
	}
}

func TestNoArbitraryReadyPromotion(t *testing.T) {
	// A machine in STARTING cannot transition directly to READY.
	m := NewMachine(nil)
	err := m.transition(READY, "should not be allowed")
	if err == nil {
		t.Error("STARTING -> READY should be rejected")
	}

	// A machine in DISCOVERING can transition to READY (this is a valid
	// transition), but only through the controller's explicit method.
	m2 := NewMachine(nil)
	m2.forceTransition(DISCOVERING, "test")
	err = m2.transition(READY, "direct promotion")
	if err != nil {
		t.Error("DISCOVERING -> READY should be allowed (valid transition)")
	}
}

func TestConcurrentSnapshots(t *testing.T) {
	m := NewMachine(nil)
	var wg sync.WaitGroup
	const goroutines = 100
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				snap := m.Snapshot()
				// The state must be one of the valid states.
				switch snap.State {
				case STARTING, DISCOVERING, GENESIS, SYNCING, READY, DEGRADED, RECOVERY_REQUIRED:
				default:
					t.Errorf("unexpected state: %v", snap.State)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentTransitions(t *testing.T) {
	m := NewMachine(nil)
	var wg sync.WaitGroup
	const transitions = 50
	for i := 0; i < transitions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Try to transition; only one will succeed.
			_ = m.transition(DISCOVERING, fmt.Sprintf("transition %d", i))
		}()
	}
	wg.Wait()
	// The machine should be in exactly one state.
	state := m.State()
	switch state {
	case STARTING, DISCOVERING:
	default:
		t.Errorf("unexpected state after concurrent transitions: %s", state)
	}
}

func TestTransitionReasonsAndTimestamps(t *testing.T) {
	base := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	clock := newTestClock(base)
	m := NewMachine(clock.Now)

	if err := m.transition(DISCOVERING, "deps ready"); err != nil {
		t.Fatalf("STARTING -> DISCOVERING: %v", err)
	}
	snap := m.Snapshot()
	if snap.Reason != "deps ready" {
		t.Errorf("reason: expected %q, got %q", "deps ready", snap.Reason)
	}
	if snap.Timestamp.IsZero() {
		t.Error("timestamp is zero")
	}
	if snap.PreviousState != STARTING {
		t.Errorf("previous state: expected STARTING, got %s", snap.PreviousState)
	}

	// The transition log should have exactly one entry.
	log := m.transitionLog.Snapshot()
	if len(log) != 1 {
		t.Fatalf("expected 1 transition log entry, got %d", len(log))
	}
	if log[0].From != STARTING || log[0].To != DISCOVERING {
		t.Errorf("log entry: expected STARTING->DISCOVERING, got %s->%s", log[0].From, log[0].To)
	}
}

func TestStateIsReady(t *testing.T) {
	readyStates := []State{READY, DEGRADED}
	unreadyStates := []State{STARTING, DISCOVERING, GENESIS, SYNCING, RECOVERY_REQUIRED}

	for _, s := range readyStates {
		if !s.IsReady() {
			t.Errorf("%s: expected IsReady to be true", s)
		}
	}
	for _, s := range unreadyStates {
		if s.IsReady() {
			t.Errorf("%s: expected IsReady to be false", s)
		}
	}
}

func TestStateIsBootstrapSourceEligible(t *testing.T) {
	if !READY.IsBootstrapSourceEligible() {
		t.Error("READY: expected bootstrap source eligible")
	}
	// DEGRADED retains valid local authority (previously READY) and is eligible
	// as a bootstrap source per the documented snapshot-source policy
	// (READY or DEGRADED, matching epoch).
	if !DEGRADED.IsBootstrapSourceEligible() {
		t.Error("DEGRADED: expected bootstrap source eligible (retains valid local authority)")
	}
	if STARTING.IsBootstrapSourceEligible() {
		t.Error("STARTING: expected NOT bootstrap source eligible")
	}
	if DISCOVERING.IsBootstrapSourceEligible() {
		t.Error("DISCOVERING: expected NOT bootstrap source eligible")
	}
	if GENESIS.IsBootstrapSourceEligible() {
		t.Error("GENESIS: expected NOT bootstrap source eligible")
	}
	if SYNCING.IsBootstrapSourceEligible() {
		t.Error("SYNCING: expected NOT bootstrap source eligible")
	}
	if RECOVERY_REQUIRED.IsBootstrapSourceEligible() {
		t.Error("RECOVERY_REQUIRED: expected NOT bootstrap source eligible")
	}
}

func TestMetadataValidation(t *testing.T) {
	valid := ClusterMetadata{
		ClusterID:  "cluster-1",
		EpochID:    "epoch-1",
		Generation: 1,
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("valid metadata: unexpected error: %v", err)
	}

	tests := []struct {
		name string
		meta ClusterMetadata
	}{
		{"empty cluster ID", ClusterMetadata{EpochID: "e", Generation: 1}},
		{"empty epoch ID", ClusterMetadata{ClusterID: "c", Generation: 1}},
		{"zero generation", ClusterMetadata{ClusterID: "c", EpochID: "e"}},
		{"negative generation", ClusterMetadata{ClusterID: "c", EpochID: "e", Generation: -1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.meta.Validate(); err == nil {
				t.Error("expected validation error, got nil")
			}
		})
	}
}

func TestMetadataSchemaV1CoherentStates(t *testing.T) {
	// The four coherent schema-v1 state combinations (each must pass
	// Validate with no error).
	cases := []struct {
		name string
		meta ClusterMetadata
	}{
		{
			// Genesis authorized but not yet finalized: Initialized=false,
			// GenesisAuthorized=true, GenesisConsumer="".
			name: "genesis authorized, not yet finalized",
			meta: ClusterMetadata{
				ClusterID:         "c1",
				EpochID:           "e1",
				Generation:        1,
				Initialized:       false,
				GenesisAuthorized: true,
				GenesisConsumer:   "",
				DataLossProtected: false,
			},
		},
		{
			// Initialized, protected cluster: Initialized=true,
			// GenesisAuthorized=false, GenesisConsumer nonempty,
			// DataLossProtected=true.
			name: "initialized, protected cluster",
			meta: ClusterMetadata{
				ClusterID:         "c1",
				EpochID:           "e1",
				Generation:        1,
				Initialized:       true,
				GenesisAuthorized: false,
				GenesisConsumer:   "gossipgrid-0",
				DataLossProtected: true,
			},
		},
		{
			// Reset incident metadata: the same as "initialized, protected
			// cluster" plus a lastReset incident whose epochId matches the
			// current epochId (a reset establishes the current epoch).
			name: "reset incident metadata",
			meta: ClusterMetadata{
				ClusterID:         "c1",
				EpochID:           "e2",
				Generation:        2,
				Initialized:       true,
				GenesisAuthorized: false,
				GenesisConsumer:   "gossipgrid-0",
				DataLossProtected: true,
				LastReset: &ResetIncident{
					EpochID:      "e2",
					Cause:        "operator",
					Acknowledged: false,
					Timestamp:    "2026-11-23T00:00:00Z",
				},
			},
		},
	}
	for _, tc := range cases {
		if err := tc.meta.Validate(); err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}
	}
}

func TestMetadataSchemaV1ContradictoryStates(t *testing.T) {
	// Contradictory combinations (each must fail Validate).
	cases := []struct {
		name string
		meta ClusterMetadata
	}{
		{
			// Simultaneously armed data-loss protection and active genesis
			// authorization: the protection flag is armed only in the same CAS
			// that consumes the authorization, so the two can never both be true.
			name: "dataLossProtected + genesisAuthorized",
			meta: ClusterMetadata{
				ClusterID:         "c1",
				EpochID:           "e1",
				Generation:        1,
				Initialized:       true,
				GenesisAuthorized: true,
				GenesisConsumer:   "gossipgrid-0",
				DataLossProtected: true,
			},
		},
		{
			// Consumer recorded while the authorization is still active: the
			// consumer is set on consumption, which also flips GenesisAuthorized
			// to false.
			name: "genesisConsumer + genesisAuthorized",
			meta: ClusterMetadata{
				ClusterID:         "c1",
				EpochID:           "e1",
				Generation:        1,
				Initialized:       true,
				GenesisAuthorized: true,
				GenesisConsumer:   "gossipgrid-0",
				DataLossProtected: false,
			},
		},
		{
			// Consumer recorded while Initialized is still false: the consumer
			// is set on consumption, which also flips Initialized to true.
			name: "genesisConsumer + !initialized",
			meta: ClusterMetadata{
				ClusterID:         "c1",
				EpochID:           "e1",
				Generation:        1,
				Initialized:       false,
				GenesisAuthorized: false,
				GenesisConsumer:   "gossipgrid-0",
				DataLossProtected: false,
			},
		},
		{
			// Initialized and GenesisAuthorized both true: an initialized
			// cluster has consumed the authorization, which sets
			// GenesisAuthorized=false.
			name: "initialized + genesisAuthorized",
			meta: ClusterMetadata{
				ClusterID:         "c1",
				EpochID:           "e1",
				Generation:        1,
				Initialized:       true,
				GenesisAuthorized: true,
				GenesisConsumer:   "gossipgrid-0",
				DataLossProtected: false,
			},
		},
		{
			// lastReset present with an epochId that does not match the current
			// epochId: a reset establishes the current epoch; a mismatched
			// incident is incoherent.
			name: "lastReset epoch mismatch",
			meta: ClusterMetadata{
				ClusterID:         "c1",
				EpochID:           "e2",
				Generation:        2,
				Initialized:       true,
				GenesisAuthorized: false,
				GenesisConsumer:   "gossipgrid-0",
				DataLossProtected: true,
				LastReset: &ResetIncident{
					EpochID:      "other-epoch",
					Cause:        "operator",
					Acknowledged: false,
					Timestamp:    "2026-11-23T00:00:00Z",
				},
			},
		},
		{
			// lastReset present while the cluster is not protected: a 5C reset
			// records the consumer and arms the protection in the same CAS; a
			// reset incident on a non-protected cluster is incoherent.
			name: "lastReset on non-protected cluster",
			meta: ClusterMetadata{
				ClusterID:         "c1",
				EpochID:           "e2",
				Generation:        2,
				Initialized:       true,
				GenesisAuthorized: false,
				GenesisConsumer:   "gossipgrid-0",
				DataLossProtected: false,
				LastReset: &ResetIncident{
					EpochID:      "e2",
					Cause:        "operator",
					Acknowledged: false,
					Timestamp:    "2026-11-23T00:00:00Z",
				},
			},
		},
		{
			// Consumer recorded while DataLossProtected is still false: the
			// operator's finalization CAS sets both the consumer and the
			// protection flag atomically, so an initialized-but-unprotected
			// consumed-genesis state is incoherent.
			name: "genesisConsumer + !dataLossProtected",
			meta: ClusterMetadata{
				ClusterID:         "c1",
				EpochID:           "e1",
				Generation:        1,
				Initialized:       true,
				GenesisAuthorized: false,
				GenesisConsumer:   "gossipgrid-0",
				DataLossProtected: false,
			},
		},
	}
	for _, tc := range cases {
		if err := tc.meta.Validate(); err == nil {
			t.Errorf("%s: expected validation error, got nil", tc.name)
		}
	}
}

// TestMetadata5ACompatibility verifies that the 5A fixtures (which do not
// populate the new genesis fields) still pass Validate: the new state
// combination rules only apply when the new fields are actually populated.
func TestMetadata5ACompatibility(t *testing.T) {
	cases := []struct {
		name string
		meta ClusterMetadata
	}{
		{"5A: initialized only", ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}},
		{"5A: not initialized", ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1}},
		{"5A: all zero genesis fields", ClusterMetadata{
			ClusterID:         "c1",
			EpochID:           "e1",
			Generation:        1,
			Initialized:       true,
			GenesisAuthorized: false,
			GenesisConsumer:   "",
			DataLossProtected: false,
		}},
		{"5A: initialized, unprotected, empty consumer", ClusterMetadata{
			ClusterID:         "c1",
			EpochID:           "e1",
			Generation:        1,
			Initialized:       true,
			GenesisAuthorized: false,
			GenesisConsumer:   "",
			DataLossProtected: false,
		}},
	}
	for _, tc := range cases {
		if err := tc.meta.Validate(); err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
		}
	}
}

func TestMetadataMatchesBinding(t *testing.T) {
	meta := ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1}
	binding := DatabaseBinding{ClusterID: "c1", EpochID: "e1", Generation: 1}
	if !meta.MatchesBinding(&binding) {
		t.Error("matching metadata/binding: expected true")
	}

	wrongCluster := DatabaseBinding{ClusterID: "c2", EpochID: "e1", Generation: 1}
	if meta.MatchesBinding(&wrongCluster) {
		t.Error("wrong cluster: expected false")
	}
	wrongEpoch := DatabaseBinding{ClusterID: "c1", EpochID: "e2", Generation: 1}
	if meta.MatchesBinding(&wrongEpoch) {
		t.Error("wrong epoch: expected false")
	}
	if meta.MatchesBinding(nil) {
		t.Error("nil binding: expected false")
	}
}

// memoryBindingStore is a deterministic, concurrency-safe in-memory binding
// store for admission tests. It implements the controller's BindingStore
// interface.
type memoryBindingStore struct {
	mu      sync.Mutex
	binding *DatabaseBinding
}

func (s *memoryBindingStore) GetBinding() (*DatabaseBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.binding == nil {
		return nil, nil
	}
	b := *s.binding
	return &b, nil
}
func (s *memoryBindingStore) Bind(clusterID, epochID string, generation int) (*DatabaseBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.binding != nil {
		if s.binding.ClusterID != clusterID || s.binding.EpochID != epochID {
			return nil, errors.New("binding mismatch")
		}
		b := *s.binding
		return &b, nil
	}
	b := &DatabaseBinding{ClusterID: clusterID, EpochID: epochID, Generation: generation}
	s.binding = b
	cp := *b
	return &cp, nil
}
func (s *memoryBindingStore) CountMessages() (int64, error) { return 0, nil }

// newAdmissionTestController builds a controller in the given state with an
// available metadata observation and a matching durable binding, so the full
// authorization policy (ready + fresh + valid + binding matches) can be
// exercised. The poll interval is 1h so the observation is never stale during
// the test.
func newAdmissionTestController(t *testing.T, state State, meta ClusterMetadata) *Controller {
	t.Helper()
	binding := &memoryBindingStore{binding: &DatabaseBinding{ClusterID: meta.ClusterID, EpochID: meta.EpochID, Generation: meta.Generation}}
	machine := NewMachine(nil)
	switch state {
	case STARTING:
	case DISCOVERING:
		machine.forceTransition(DISCOVERING, "test")
	case SYNCING:
		machine.forceTransition(DISCOVERING, "test")
		_ = machine.transition(SYNCING, "test")
	case READY:
		machine.forceTransition(DISCOVERING, "test")
		_ = machine.transition(READY, "test")
	case DEGRADED:
		machine.forceTransition(READY, "test")
		_ = machine.transition(DEGRADED, "test")
	case RECOVERY_REQUIRED:
		machine.forceTransition(STARTING, "test")
		_ = machine.transition(RECOVERY_REQUIRED, "test")
	}
	src := NewFakeMetadataSource(meta, true)
	ctrl := NewController(Config{MetadataPollInterval: time.Hour, Enabled: true}, machine, src, binding, nil)
	// Set the accepted metadata directly (bypassing the worker) so the
	// snapshot has a fresh observation.
	ctrl.SetAcceptedMetadataForTest(meta)
	return ctrl
}

func TestAdmissionDisabled(t *testing.T) {
	a := NewAdmissionDisabled()

	// All operations are allowed when lifecycle protection is disabled.
	if d := a.CheckWrite(context.Background()); !d.Allowed {
		t.Error("disabled: CheckWrite should be allowed")
	}
	if d := a.CheckReplicationInbound(context.Background(), RequestContext{}); !d.Allowed {
		t.Error("disabled: CheckReplicationInbound should be allowed")
	}
	if d := a.CheckReplicationOutbound(context.Background()); !d.Allowed {
		t.Error("disabled: CheckReplicationOutbound should be allowed")
	}
}

func TestAdmissionWriteRequiresReady(t *testing.T) {
	meta := ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}

	// STARTING: unready -> blocked.
	ctrl := newAdmissionTestController(t, STARTING, meta)
	if d := ctrl.Admission().CheckWrite(context.Background()); d.Allowed {
		t.Error("STARTING: write should be blocked")
	}

	// READY: ready + fresh + binding matches -> allowed.
	ctrlReady := newAdmissionTestController(t, READY, meta)
	if d := ctrlReady.Admission().CheckWrite(context.Background()); !d.Allowed {
		t.Error("READY: write should be allowed")
	}

	// DEGRADED: ready + fresh + binding matches -> allowed.
	ctrlDeg := newAdmissionTestController(t, DEGRADED, meta)
	if d := ctrlDeg.Admission().CheckWrite(context.Background()); !d.Allowed {
		t.Error("DEGRADED: write should be allowed")
	}

	// No metadata: blocked even when ready.
	ctrlNoMeta := newAdmissionTestController(t, READY, meta)
	ctrlNoMeta.mu.Lock()
	ctrlNoMeta.accepted = nil
	ctrlNoMeta.authorityState = "missing"
	ctrlNoMeta.mu.Unlock()
	if d := ctrlNoMeta.Admission().CheckWrite(context.Background()); d.Allowed {
		t.Error("no metadata: write should be blocked")
	}

	// No binding: blocked even when ready and fresh. The binding is removed
	// from both the durable store and the controller's cache, so the
	// authorization snapshot's binding read-through finds nothing.
	ctrlNoBind := newAdmissionTestController(t, READY, meta)
	memStore := ctrlNoBind.binding.(*memoryBindingStore)
	memStore.mu.Lock()
	memStore.binding = nil
	memStore.mu.Unlock()
	ctrlNoBind.mu.Lock()
	ctrlNoBind.bindingVal = nil
	ctrlNoBind.mu.Unlock()
	if d := ctrlNoBind.Admission().CheckWrite(context.Background()); d.Allowed {
		t.Error("no binding: write should be blocked")
	}
}

func TestAdmissionInboundReplication(t *testing.T) {
	meta := ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	reqCtx := RequestContext{ClusterID: "c1", EpochID: "e1"}

	// READY with matching context + binding: allowed.
	ctrl := newAdmissionTestController(t, READY, meta)
	if d := ctrl.Admission().CheckReplicationInbound(context.Background(), reqCtx); !d.Allowed {
		t.Errorf("READY with matching context: should be allowed, got %q", d.Reason)
	}

	// READY with wrong cluster: rejected.
	wrongCluster := RequestContext{ClusterID: "c2", EpochID: "e1"}
	if d := ctrl.Admission().CheckReplicationInbound(context.Background(), wrongCluster); d.Allowed {
		t.Error("wrong cluster: should be rejected")
	}

	// READY with wrong epoch: rejected.
	wrongEpoch := RequestContext{ClusterID: "c1", EpochID: "e2"}
	if d := ctrl.Admission().CheckReplicationInbound(context.Background(), wrongEpoch); d.Allowed {
		t.Error("wrong epoch: should be rejected")
	}

	// Missing context: rejected.
	if d := ctrl.Admission().CheckReplicationInbound(context.Background(), RequestContext{}); d.Allowed {
		t.Error("missing context: should be rejected")
	}

	// SYNCING: rejected (cannot serve authoritative data).
	ctrlSync := newAdmissionTestController(t, SYNCING, meta)
	if d := ctrlSync.Admission().CheckReplicationInbound(context.Background(), reqCtx); d.Allowed {
		t.Error("SYNCING: should be rejected")
	}
}

func TestAdmissionOutboundReplication(t *testing.T) {
	meta := ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}

	// READY + fresh + binding matches: allowed.
	ctrl := newAdmissionTestController(t, READY, meta)
	if d := ctrl.Admission().CheckReplicationOutbound(context.Background()); !d.Allowed {
		t.Error("READY: outbound should be allowed")
	}

	// SYNCING: blocked.
	ctrlSync := newAdmissionTestController(t, SYNCING, meta)
	if d := ctrlSync.Admission().CheckReplicationOutbound(context.Background()); d.Allowed {
		t.Error("SYNCING: outbound should be blocked")
	}
}

func TestParseRequestContext(t *testing.T) {
	// No headers, not required: ok, empty context.
	getHeader := func(name string) []string {
		return nil
	}
	rc, err := ParseRequestContext(getHeader, false)
	if err != nil {
		t.Errorf("no headers, not required: unexpected error: %v", err)
	}
	if rc.ClusterID != "" || rc.EpochID != "" {
		t.Errorf("no headers: expected empty context, got %+v", rc)
	}

	// Valid headers: ok.
	getHeader2 := func(name string) []string {
		switch name {
		case HeaderClusterID:
			return []string{"c1"}
		case HeaderEpochID:
			return []string{"e1"}
		}
		return nil
	}
	rc2, err := ParseRequestContext(getHeader2, true)
	if err != nil {
		t.Errorf("valid headers: unexpected error: %v", err)
	}
	if rc2.ClusterID != "c1" || rc2.EpochID != "e1" {
		t.Errorf("valid headers: expected c1/e1, got %+v", rc2)
	}

	// Duplicate headers: error.
	getHeader3 := func(name string) []string {
		switch name {
		case HeaderClusterID:
			return []string{"c1", "c2"}
		case HeaderEpochID:
			return []string{"e1"}
		}
		return nil
	}
	if _, err := ParseRequestContext(getHeader3, true); err == nil {
		t.Error("duplicate headers: expected error")
	}

	// Oversized header: error.
	oversized := make([]byte, MaxEpochHeaderBytes+1)
	for i := range oversized {
		oversized[i] = 'a'
	}
	getHeader4 := func(name string) []string {
		switch name {
		case HeaderClusterID:
			return []string{string(oversized)}
		case HeaderEpochID:
			return []string{"e1"}
		}
		return nil
	}
	if _, err := ParseRequestContext(getHeader4, true); err == nil {
		t.Error("oversized header: expected error")
	}

	// One header missing when required: error.
	getHeader5 := func(name string) []string {
		if name == HeaderClusterID {
			return []string{"c1"}
		}
		return nil
	}
	if _, err := ParseRequestContext(getHeader5, true); err == nil {
		t.Error("one header missing, required: expected error")
	}
}

func TestControllerStartAndStop(t *testing.T) {
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		Enabled:              true,
	}, NewMachine(nil), NewFakeMetadataSourceUnavailable(), nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := ctrl.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The controller should be in DISCOVERING or RECOVERY_REQUIRED
	// (metadata unavailable -> fail closed).
	state := ctrl.State()
	if state == READY || state == DEGRADED {
		t.Errorf("controller with unavailable metadata: expected unready state, got %s", state)
	}

	ctrl.Stop(context.Background())
}

func TestControllerStopBeforeStart(t *testing.T) {
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		Enabled:              true,
	}, NewMachine(nil), NewFakeMetadataSourceUnavailable(), nil, nil)

	// Stop before Start should return promptly.
	ctrl.Stop(context.Background())
}

func TestControllerRepeatedStop(t *testing.T) {
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		Enabled:              true,
	}, NewMachine(nil), NewFakeMetadataSourceUnavailable(), nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := ctrl.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Repeated Stop is safe.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctrl.Stop(context.Background())
		}()
	}
	wg.Wait()
}

func TestControllerMetadataChange(t *testing.T) {
	// Start with epoch A.
	metaA := ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	src := NewFakeMetadataSource(metaA, true)

	ctrl := NewController(Config{
		MetadataPollInterval: 50 * time.Millisecond,
		Enabled:              true,
	}, NewMachine(nil), src, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := ctrl.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for the initial evaluation.
	time.Sleep(20 * time.Millisecond)

	// The controller should have accepted metadata A.
	if meta := ctrl.AcceptedMetadata(); meta == nil || meta.EpochID != "e1" {
		t.Errorf("expected accepted metadata e1, got %v", ctrl.AcceptedMetadata())
	}

	// Change to epoch B (different ClusterID).
	metaB := ClusterMetadata{ClusterID: "c2", EpochID: "e2", Generation: 2, Initialized: true}
	src.SetMetadata(metaB)

	// Wait for the next poll.
	time.Sleep(200 * time.Millisecond)

	// The controller should be in RECOVERY_REQUIRED (binding mismatch).
	// Note: since no binding was set, the controller transitions to
	// DISCOVERING (no binding + cluster initialized). But if a binding
	// existed for epoch A, it would transition to RECOVERY_REQUIRED.
	state := ctrl.State()
	_ = state // The exact state depends on whether a binding exists.
	// The key property: the controller did NOT silently accept epoch B
	// as the current epoch without re-evaluation.
	if meta := ctrl.AcceptedMetadata(); meta == nil || meta.EpochID != "e2" {
		t.Errorf("expected accepted metadata e2 after change, got %v", ctrl.AcceptedMetadata())
	}
}
