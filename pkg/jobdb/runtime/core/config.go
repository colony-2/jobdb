package runtimecore

import (
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// Config wires the durable backend ports used by runtime core.
type Config struct {
	// SecretProvider is caller-owned and independent of scheduler and chapter storage.
	SecretProvider jobdb.SecretProvider
	SecretLimits   jobdb.SecretLimits
	Scheduler      Scheduler
	Chapters       ChapterLog
	Schemas        SchemaStore
	Now            func() time.Time
}

// ValidateConfig validates the public runtime-core backend wiring.
func ValidateConfig(cfg Config) error {
	if cfg.Scheduler == nil {
		return fmt.Errorf("runtime core scheduler is required")
	}
	if cfg.Chapters == nil {
		return fmt.Errorf("runtime core chapter log is required")
	}
	if cfg.Schemas == nil {
		return fmt.Errorf("runtime core schema store is required")
	}
	return nil
}

func nowFunc(now func() time.Time) func() time.Time {
	if now != nil {
		return func() time.Time {
			t := now()
			if t.IsZero() {
				return time.Now().UTC()
			}
			return t.UTC()
		}
	}
	return func() time.Time {
		return time.Now().UTC()
	}
}
