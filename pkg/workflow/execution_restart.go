package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type continuationBudget struct {
	Deadline      time.Time `json:"deadline"`
	TotalDeadline time.Time `json:"total_deadline"`
	Attempt       int       `json:"attempt"`
}

// applyRestart runs before the live edge activates timers. This prevents copied
// deadlines from expiring a deliberately fresh continuation.
func (c *executionContext) applyRestart(id string, opts ScopeOptions) error {
	e := c.run
	restart := e.restarts[id]
	if restart == nil {
		return nil
	}
	if len(restart.Payload) > 0 && opts.OnRestart == nil {
		return e.control(fmt.Errorf("restart target %s has no handler", id))
	}
	var budgets map[string]continuationBudget
	next := e.peek()
	if next != nil && next.record.Kind == "continuation" && next.record.ID == id {
		if err := json.Unmarshal(next.record.Payload, &budgets); err != nil {
			return e.control(err)
		}
	} else {
		if next != nil {
			return e.control(fmt.Errorf("restart boundary %s was not reached at prefix end", id))
		}
		budgets = map[string]continuationBudget{}
		now := time.Now().UTC()
		// Compute outermost first so every nested deadline is bounded by its parent.
		var chain []*executionContext
		for p := c; p != nil && p.id != ""; p = p.parent {
			chain = append(chain, p)
		}
		var enclosing time.Time
		for i := len(chain) - 1; i >= 0; i-- {
			p := chain[i]
			total := earlier(deadlineFrom(now, p.policy.TotalTimeout), enclosing)
			deadline := earlier(deadlineFrom(now, p.policy.AttemptTimeout), total)
			budgets[p.id] = continuationBudget{Deadline: deadline, TotalDeadline: total, Attempt: p.attempt}
			enclosing = deadline
		}
	}
	e.mu.Lock()
	e.budgets = budgets
	if e.restart != nil && e.restart.ID == id {
		e.restartApplied = true
	}
	for p := c; p != nil; p = p.parent {
		if b, ok := budgets[p.id]; ok {
			p.deadline = b.Deadline
			p.totalDeadline = b.TotalDeadline
			if p.timer != nil {
				p.timer.Stop()
				p.timer = nil
			}
		}
	}
	e.mu.Unlock()
	raw, _ := json.Marshal(budgets)
	_, err := c.event("continuation", id, func() *jobdb.ExecutionRecord { return &jobdb.ExecutionRecord{Payload: raw} })
	if err == nil && len(restart.Prerequisites) > 0 {
		var admission jobdb.ExecutionRecord
		admission, err = c.event("admission", id+"/restart", func() *jobdb.ExecutionRecord {
			failure := e.r.checkPrerequisites(c.ctx, chapterMeta{Prerequisites: restart.Prerequisites})
			record, _ := recordResult(c.ctx, NewTaskDataOrPanic(true), failure)
			return record
		})
		if err == nil {
			_, err = resultData(admission)
		}
	}
	return err
}
