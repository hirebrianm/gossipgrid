// Package lifecycle: deterministic, concurrency-safe fake metadata source
// for testing.
//
// FakeMetadataSource implements MetadataSource with controlled observations.
// Tests can set specific metadata values, simulate unavailability, simulate
// malformed metadata, and observe read counts. All operations are safe for
// concurrent use.
package lifecycle

import (
	"context"
	"sync"
	"time"
)

// FakeMetadataSource is a deterministic, concurrency-safe fake implementation
// of MetadataSource for testing.
type FakeMetadataSource struct {
	mu         sync.RWMutex
	metadata   ClusterMetadata
	available  bool
	malformed  bool
	readCount  int
	lastReadAt time.Time
	readErr    error
	now        func() time.Time
}

// NewFakeMetadataSource creates a FakeMetadataSource with the given metadata
// and availability state.
func NewFakeMetadataSource(meta ClusterMetadata, available bool) *FakeMetadataSource {
	return &FakeMetadataSource{
		metadata:  meta,
		available: available,
		now:       time.Now,
	}
}

// NewFakeMetadataSourceUnavailable creates a FakeMetadataSource that is
// initially unavailable (no metadata).
func NewFakeMetadataSourceUnavailable() *FakeMetadataSource {
	return &FakeMetadataSource{
		available: false,
		now:       time.Now,
	}
}

// SetMetadata atomically sets the metadata and marks the source available.
func (f *FakeMetadataSource) SetMetadata(meta ClusterMetadata) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metadata = meta
	f.available = true
	f.malformed = false
	f.readErr = nil
}

// SetUnavailable atomically marks the source unavailable.
func (f *FakeMetadataSource) SetUnavailable() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.available = false
}

// SetMalformed atomically marks the source as returning malformed metadata
// (failing ClusterMetadata.Validate).
func (f *FakeMetadataSource) SetMalformed() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.malformed = true
	f.available = true
}

// SetError atomically sets a custom error to be returned by Read.
func (f *FakeMetadataSource) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readErr = err
	f.available = true
}

// Read implements MetadataSource.
func (f *FakeMetadataSource) Read(_ context.Context) (ClusterMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.readCount++
	f.lastReadAt = f.now()

	if f.readErr != nil {
		return ClusterMetadata{}, f.readErr
	}
	if !f.available {
		return ClusterMetadata{}, ErrMetadataUnavailable
	}
	if f.malformed {
		// Return metadata that fails validation.
		return ClusterMetadata{}, ErrMetadataMalformed
	}
	// Validate the metadata; a malformed stored value is an error.
	if err := f.metadata.Validate(); err != nil {
		return ClusterMetadata{}, ErrMetadataMalformed
	}
	return f.metadata, nil
}

// ReadCount returns the number of Read calls made.
func (f *FakeMetadataSource) ReadCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.readCount
}

// LastReadAt returns the time of the last Read call.
func (f *FakeMetadataSource) LastReadAt() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastReadAt
}

// Metadata returns the current stored metadata (for test assertions).
func (f *FakeMetadataSource) Metadata() ClusterMetadata {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.metadata
}

// Available reports whether the source is currently available.
func (f *FakeMetadataSource) Available() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.available
}

// WithClock returns a copy of the source with an injectable clock.
func (f *FakeMetadataSource) WithClock(now func() time.Time) *FakeMetadataSource {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = now
	return f
}
