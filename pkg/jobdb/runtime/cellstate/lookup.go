package cellstate

import "github.com/colony-2/jobdb/pkg/jobdb"

// Lookup returns registry metadata without submission authorization. The caller
// must validate a live lease and select its tenant's state in the same lock or
// transaction domain. The returned value is independent of later registry edits.
func (s *State) Lookup(name string) (jobdb.ResolvedCell, error) {
	if err := jobdb.ValidateCellName(name); err != nil {
		return jobdb.ResolvedCell{}, err
	}
	c, ok := s.ByName(name)
	if !ok {
		return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
	}
	if c.State == jobdb.CellRetired {
		return jobdb.ResolvedCell{}, jobdb.ErrCellRetired
	}
	return c.Resolved(), nil
}
