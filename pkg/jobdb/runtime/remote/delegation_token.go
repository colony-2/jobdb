package remote

import (
	"fmt"
	"slices"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func namesAllow(s jobdb.DelegationNames, name string) bool {
	return s.Any || slices.Contains(s.Names, name)
}
func chaptersAllow(s *jobdb.DelegationChapters, ordinal int64) bool {
	if s == nil || ordinal < 0 {
		return false
	}
	if s.Any || slices.Contains(s.Ordinals, ordinal) {
		return true
	}
	for _, r := range s.Ranges {
		if ordinal >= r.Start && ordinal <= r.End {
			return true
		}
	}
	return false
}

func validateDelegationRequest(req *jobdb.CreateDelegationRequest) error {
	if req.NotAfter != nil && !req.NotAfter.After(time.Now()) {
		return fmt.Errorf("notAfter must be in the future")
	}
	return validateDelegationPolicy(req.Permissions)
}

func validateDelegationPolicy(p jobdb.DelegationPermissions) error {
	names := func(s jobdb.DelegationNames) bool {
		if s.Any && len(s.Names) > 0 || len(s.Names) > 256 {
			return false
		}
		for _, n := range s.Names {
			if n == "" || len(n) > 1024 {
				return false
			}
		}
		return true
	}
	ordinals := func(ns []int64) bool {
		if len(ns) > 256 {
			return false
		}
		for _, n := range ns {
			if n < 0 {
				return false
			}
		}
		return true
	}
	if p.Submit != nil && (!names(p.Submit.JobTypes) || !names(p.Submit.JobIDs)) {
		return fmt.Errorf("invalid submission selectors")
	}
	if !names(jobdb.DelegationNames{Names: p.Secrets}) {
		return fmt.Errorf("invalid secret names")
	}
	if p.Read != nil {
		if !ordinals(p.Read.Ordinals) || len(p.Read.Ranges) > 256 || p.Read.Any && (len(p.Read.Ordinals) > 0 || len(p.Read.Ranges) > 0) {
			return fmt.Errorf("invalid chapter selectors")
		}
		for _, r := range p.Read.Ranges {
			if r.Start < 0 || r.End < r.Start {
				return fmt.Errorf("invalid chapter range")
			}
		}
	}
	if p.Write != nil && (!ordinals(p.Write.Ordinals) || !names(p.Write.TaskTypes) || !names(p.Write.ArtifactNames)) {
		return fmt.Errorf("invalid chapter write selectors")
	}
	return nil
}
