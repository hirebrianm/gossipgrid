package antientropy

// DiscoveryPeerSource implements PeerSource by reading from a discovery
// snapshot function. It derives expected certificate identities from pod
// identities using the configured identity suffix, and excludes the local
// replica. Stale DNS membership is respected: for dns mode, a stale snapshot
// yields no peers (no new sync is performed while the snapshot is stale) and
// reports non-authoritative membership (cursor state is preserved). Static
// mode is deterministic and never stale.
//
// The source does NOT hold discovery locks during network operations: it
// calls the snapshot function (which returns an immutable copy) exactly once
// per Snapshot() call and then processes the result without holding any
// lock. This guarantees that the peer targets and the membership-validity
// flag are derived from the same discovery generation.
type DiscoveryPeerSource struct {
	IdentitySuffix string
	SnapshotFunc   func() PeerSourceSnapshot
}

// PeerSourceSnapshot is a minimal read-only view of the discovery snapshot
// that the peer source needs. It mirrors the relevant fields of
// discovery.Snapshot without importing the discovery package (avoiding a
// circular dependency).
type PeerSourceSnapshot struct {
	Peers              []PeerSourceInfo
	LocalNode          *PeerSourceInfo
	ExpectedIdentities []string
	Stale              bool
	Mode               string
}

// PeerSourceInfo is a single peer entry.
type PeerSourceInfo struct {
	Hostname    string
	PodIdentity string
	Address     string
}

// NewDiscoveryPeerSource creates a DiscoveryPeerSource with the given identity
// suffix and snapshot function.
func NewDiscoveryPeerSource(identitySuffix string, snapshotFunc func() PeerSourceSnapshot) *DiscoveryPeerSource {
	return &DiscoveryPeerSource{
		IdentitySuffix: identitySuffix,
		SnapshotFunc:   snapshotFunc,
	}
}

// Snapshot implements PeerSource. It reads the discovery snapshot exactly once
// and derives the eligible peer targets, the expected replica identities, and
// the membership-validity flag from that single snapshot. This prevents a
// stale-to-fresh (or fresh-to-stale) transition between two separate reads
// from producing a contradictory view (e.g. an empty peer list from a stale
// snapshot combined with a validity flag from a fresh one).
//
// For a nil snapshot function, it returns no peers, no expected identities,
// and non-authoritative membership (no cleanup decisions can be made).
func (s *DiscoveryPeerSource) Snapshot() MembershipSnapshot {
	if s.SnapshotFunc == nil {
		return MembershipSnapshot{Authoritative: false}
	}
	snap := s.SnapshotFunc()

	// Determine whether the membership is authoritative. A non-stale snapshot
	// is authoritative (true); a stale DNS snapshot is not (false) because it
	// reflects a last-known-good state after a failed refresh. Static mode is
	// deterministic and never stale, so it is always authoritative.
	authoritative := !snap.Stale || snap.Mode != "dns"

	result := MembershipSnapshot{
		ExpectedIdentities: snap.ExpectedIdentities,
		Authoritative:      authoritative,
	}

	// Stale DNS membership: no sync while the snapshot is stale. The expected
	// identities are still carried (they are independent of DNS state) so
	// cursor cleanup can be based on them if the snapshot is later used.
	if snap.Stale && snap.Mode == "dns" {
		return result
	}

	if len(snap.Peers) == 0 {
		return result
	}

	// Determine the local node's identity for exclusion.
	localID := ""
	if snap.LocalNode != nil {
		localID = snap.LocalNode.PodIdentity
	}

	var targets []PeerTarget
	for _, p := range snap.Peers {
		peerID := p.PodIdentity
		if peerID == "" {
			peerID = p.Hostname
		}
		// Exclude the local replica.
		if peerID == localID {
			continue
		}
		// Exclude peers without a valid pod identity (unrelated hosts).
		if p.PodIdentity == "" {
			continue
		}
		// Derive the expected certificate identity.
		identity := p.PodIdentity + "." + s.IdentitySuffix
		targets = append(targets, PeerTarget{
			DialAddr:         p.Address,
			ExpectedIdentity: identity,
			PodIdentity:      p.PodIdentity,
		})
	}
	result.Peers = targets
	return result
}
