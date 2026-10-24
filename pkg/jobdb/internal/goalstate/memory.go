package goalstate

import (
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"maps"
	"sort"
	"time"
)

type CandidateKey struct {
	Goal  jobdb.GoalKey
	JobID string
}

// Memory holds goal state only; job facts remain owned by the scheduler.
// Callers hold their scheduler mutex. Clone provides rollback for transactions.
type Memory struct {
	GoalRows    map[jobdb.GoalKey]jobdb.GoalInfo
	Extras      map[CandidateKey]time.Time
	WaitRows    map[jobdb.DependencyWaitKey]jobdb.DependencyWaitInfo
	LookupJob   func(jobdb.JobKey) (Job, error)
	PrimaryJobs func(jobdb.GoalKey) []Job
}

func NewMemory(lookup func(jobdb.JobKey) (Job, error), primary func(jobdb.GoalKey) []Job) *Memory {
	return &Memory{GoalRows: map[jobdb.GoalKey]jobdb.GoalInfo{}, Extras: map[CandidateKey]time.Time{}, WaitRows: map[jobdb.DependencyWaitKey]jobdb.DependencyWaitInfo{}, LookupJob: lookup, PrimaryJobs: primary}
}
func (s *Memory) Clone() *Memory {
	c := *s
	c.GoalRows = maps.Clone(s.GoalRows)
	c.Extras = maps.Clone(s.Extras)
	c.WaitRows = maps.Clone(s.WaitRows)
	return &c
}
func copyValue[T any](v T) T {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out T
	if err = json.Unmarshal(raw, &out); err != nil {
		panic(err)
	}
	return out
}
func (s *Memory) Job(k jobdb.JobKey) (Job, error) { return s.LookupJob(k) }
func (s *Memory) Goal(k jobdb.GoalKey) (jobdb.GoalInfo, error) {
	v, ok := s.GoalRows[k]
	if !ok {
		return v, jobdb.ErrGoalNotFound
	}
	return copyValue(v), nil
}
func (s *Memory) SaveGoal(g jobdb.GoalInfo) error { s.GoalRows[g.GoalKey] = copyValue(g); return nil }
func (s *Memory) Candidates(k jobdb.GoalKey) ([]Job, error) {
	out := s.PrimaryJobs(k)
	for pair := range s.Extras {
		if pair.Goal == k {
			j, err := s.Job(jobdb.JobKey{TenantId: k.TenantId, JobId: pair.JobID})
			if err != nil {
				return nil, err
			}
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key.JobId < out[j].Key.JobId })
	return out, nil
}
func (s *Memory) Goals(k jobdb.JobKey) ([]jobdb.GoalKey, error) {
	j, err := s.Job(k)
	if err != nil {
		return nil, err
	}
	out := []jobdb.GoalKey{{TenantId: k.TenantId, GoalID: j.GoalID}}
	for pair := range s.Extras {
		if pair.Goal.TenantId == k.TenantId && pair.JobID == k.JobId {
			out = append(out, pair.Goal)
		}
	}
	return out, nil
}
func (s *Memory) Candidate(k jobdb.GoalKey, id string) (time.Time, bool, error) {
	j, err := s.Job(jobdb.JobKey{TenantId: k.TenantId, JobId: id})
	if err != nil {
		return time.Time{}, false, err
	}
	if j.GoalID == k.GoalID {
		return j.CreatedAt, true, nil
	}
	at, ok := s.Extras[CandidateKey{k, id}]
	return at, ok, nil
}
func (s *Memory) InsertCandidate(k jobdb.GoalKey, id string, at time.Time) error {
	s.Extras[CandidateKey{k, id}] = at
	return nil
}
func (s *Memory) Wait(k jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	w, ok := s.WaitRows[k]
	if !ok {
		return w, jobdb.ErrDependencyWaitNotFound
	}
	return copyValue(w), nil
}
func (s *Memory) SaveWait(w jobdb.DependencyWaitInfo) error {
	s.WaitRows[jobdb.DependencyWaitKey{JobKey: w.JobKey, WaitID: w.WaitID}] = copyValue(w)
	return nil
}
func (s *Memory) ConsumerWaits(k jobdb.JobKey) ([]jobdb.DependencyWaitInfo, error) {
	out := []jobdb.DependencyWaitInfo{}
	for key, w := range s.WaitRows {
		if key.JobKey == k && w.State == jobdb.DependencyPending {
			out = append(out, copyValue(w))
		}
	}
	return out, nil
}
func (s *Memory) TargetWaits(tenant string, t jobdb.DependencyTarget) ([]jobdb.DependencyWaitInfo, error) {
	out := []jobdb.DependencyWaitInfo{}
	for key, w := range s.WaitRows {
		if key.JobKey.TenantId != tenant || w.State != jobdb.DependencyPending {
			continue
		}
		for _, d := range w.Dependencies {
			if d.Target == t && d.State == jobdb.DependencyPending {
				out = append(out, copyValue(w))
				break
			}
		}
	}
	return out, nil
}
