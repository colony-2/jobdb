// Package projectview contains explicit public presentation DTOs. Shared Go
// models and retained tenant JSON tags remain unchanged.
package projectview

import (
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"time"
)

type Cell struct {
	ProjectID   string                     `json:"projectId"`
	ID          string                     `json:"id"`
	Name        string                     `json:"name"`
	Repo        string                     `json:"repo"`
	Description string                     `json:"description"`
	Metadata    map[string]json.RawMessage `json:"metadata"`
	State       jobdb.CellState            `json:"state"`
	Aliases     []string                   `json:"aliases"`
	Revision    string                     `json:"revision"`
	CreatedAt   time.Time                  `json:"createdAt"`
	UpdatedAt   time.Time                  `json:"updatedAt"`
	RetiredAt   *time.Time                 `json:"retiredAt,omitempty"`
}
