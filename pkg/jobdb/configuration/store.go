// Package configuration implements tenant configuration validation and storage.
// Its Store port is public so independently maintained backends can implement it.
package configuration

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// Store returns owned snapshots. Update must serialize mutations for a tenant,
// invoke change on the current committed snapshot, and commit its result atomically
// before returning. A change error commits nothing. Never reset an empty tenant's
// revision. Missing tenants use Empty. Implementations must honor cancellation.
// The callback must not reenter the store or perform external I/O.
type Store interface {
	Read(context.Context, string) (jobdb.ConfigurationSnapshot, error)
	Update(context.Context, string, func(jobdb.ConfigurationSnapshot) (jobdb.ConfigurationSnapshot, error)) (jobdb.ConfigurationSnapshot, error)
}

func Empty() jobdb.ConfigurationSnapshot {
	return jobdb.ConfigurationSnapshot{Revision: "cfg-0", Values: map[string]json.RawMessage{}}
}
func Clone(s jobdb.ConfigurationSnapshot) jobdb.ConfigurationSnapshot {
	out := jobdb.ConfigurationSnapshot{Revision: s.Revision, Values: make(map[string]json.RawMessage, len(s.Values))}
	for k, v := range s.Values {
		out.Values[k] = append(json.RawMessage(nil), v...)
	}
	return out
}

// MemoryStore is the toy backend's authoritative storage, not a client cache.
// Its zero value is ready to use.
type MemoryStore struct {
	mu      sync.Mutex
	tenants map[string]jobdb.ConfigurationSnapshot
}

func (s *MemoryStore) current(tenant string) jobdb.ConfigurationSnapshot {
	v, ok := s.tenants[tenant]
	if !ok {
		return Empty()
	}
	return Clone(v)
}
func (s *MemoryStore) Read(ctx context.Context, tenant string) (jobdb.ConfigurationSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	return s.current(tenant), nil
}
func (s *MemoryStore) Update(ctx context.Context, tenant string, change func(jobdb.ConfigurationSnapshot) (jobdb.ConfigurationSnapshot, error)) (jobdb.ConfigurationSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	next, err := change(s.current(tenant))
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	if s.tenants == nil {
		s.tenants = make(map[string]jobdb.ConfigurationSnapshot)
	}
	s.tenants[tenant] = Clone(next)
	return Clone(next), nil
}
