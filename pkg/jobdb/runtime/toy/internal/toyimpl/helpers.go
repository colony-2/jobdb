package toyimpl

import (
	"time"
)

func cloneTime(src *time.Time) *time.Time {
	if src == nil {
		return nil
	}
	value := src.UTC()
	return &value
}
