// Package projectcatalog implements explicit project identity without changing
// resource keys or historical bytes. Stores serialize catalog changes and durable
// admission seals; resource stores may commit independently after admission.
package projectcatalog

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

const version = 1

type record struct {
	Project jobdb.Project `json:"project"`
	Deleted bool          `json:"deleted,omitempty"`
	Sealed  bool          `json:"sealed,omitempty"`
}
type receipt struct {
	Descriptor string                `json:"descriptor"`
	Result     jobdb.ProjectCreation `json:"result"`
}

// State is the versioned storage encoding, not a resource or HTTP contract.
// Raw metadata and receipt bodies deliberately bypass floating-point decoding.
type State struct {
	Version   int                `json:"version"`
	ID        string             `json:"id"`
	Manifest  string             `json:"manifest"`
	CursorKey []byte             `json:"cursorKey"`
	Projects  map[string]record  `json:"projects"`
	Receipts  map[string]receipt `json:"receipts"`
}

// Store's callback sees an isolated state. Update must serialize across every
// process/handle, and atomically commit all changes only on a nil callback error.
// A nil state means not installed; only Install may populate it.
type Store interface {
	Read(context.Context) (*State, error)
	Update(context.Context, func(**State) error) error
}

type MemoryStore struct {
	mu    sync.Mutex
	state *State
}

func clone(s *State) (*State, error) {
	if s == nil {
		return nil, nil
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var result State
	err = json.Unmarshal(raw, &result)
	return &result, err
}
func (s *MemoryStore) Read(ctx context.Context) (*State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return clone(s.state)
}
func (s *MemoryStore) Update(ctx context.Context, f func(**State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	next, err := clone(s.state)
	if err != nil {
		return err
	}
	if err = f(&next); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	s.state, err = clone(next)
	return err
}
