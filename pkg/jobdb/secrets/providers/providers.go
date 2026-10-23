// Package secretproviders composes independent secret providers in priority order.
package secretproviders

import (
	"context"
	"fmt"
	"reflect"
	"regexp"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsutil"
)

type Member struct {
	ID       string
	Provider jobdb.SecretProvider
}

// Selection contains provenance only, never credentials or provider error text.
type Selection struct{ TenantID, JobID, Name, ProviderID string }

type Options struct {
	MaxParallel int
	Limits      jobdb.SecretLimits
	// Observe is optional, must be concurrency-safe and return promptly.
	Observe func(Selection)
}

type Collection struct {
	members []Member
	options Options
	slots   chan struct{}
}

var validID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

// ValidID reports whether an ID is a bounded symbolic configuration name.
func ValidID(id string) bool { return validID.MatchString(id) }

// New preserves caller ownership of every member. The collection has no Close.
func New(members []Member, options Options) (*Collection, error) {
	if len(members) == 0 || options.MaxParallel < 0 {
		return nil, fmt.Errorf("secret collection: invalid configuration")
	}
	seen := map[string]bool{}
	for _, m := range members {
		if !ValidID(m.ID) || seen[m.ID] || isNil(m.Provider) {
			return nil, fmt.Errorf("secret collection: invalid member")
		}
		seen[m.ID] = true
	}
	if options.MaxParallel == 0 {
		options.MaxParallel = 4
	}
	options.MaxParallel = min(options.MaxParallel, len(members))
	options.Limits = options.Limits.WithDefaults()
	return &Collection{members: append([]Member(nil), members...), options: options, slots: make(chan struct{}, options.MaxParallel)}, nil
}

func isNil(p jobdb.SecretProvider) bool {
	if p == nil {
		return true
	}
	v := reflect.ValueOf(p)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Func, reflect.Interface, reflect.Slice, reflect.Chan:
		return v.IsNil()
	}
	return false
}

func cloneRequest(r jobdb.SecretResolveRequest) jobdb.SecretResolveRequest {
	r.Names = append([]string(nil), r.Names...)
	r.AppMetadata = append([]byte(nil), r.AppMetadata...)
	if r.Principal != nil {
		p := *r.Principal
		r.Principal = &p
	}
	return r
}

type outcome struct {
	result jobdb.SecretResolveResult
	err    error
}

func (c *Collection) ResolveSecrets(ctx context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	if err := secretsutil.Names(req.Names, c.options.Limits.MaxRequestBytes); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.options.Limits.LookupTimeout)
	defer cancel()
	// A sliding window bounds both active calls and out-of-order buffered results.
	window := make([]chan outcome, c.options.MaxParallel)
	launch := func(index int) {
		ch := make(chan outcome, 1)
		window[index%len(window)] = ch
		go func() {
			select {
			case c.slots <- struct{}{}:
			case <-ctx.Done():
				ch <- outcome{err: jobdb.ErrSecretsUnavailable}
				return
			}
			defer func() { <-c.slots }()
			if ctx.Err() != nil {
				ch <- outcome{err: jobdb.ErrSecretsUnavailable}
				return
			}
			r, err := c.members[index].Provider.ResolveSecrets(ctx, cloneRequest(req))
			if err != nil {
				ch <- outcome{err: secretsutil.SafeError(err)}
				return
			}
			r, err = secretsutil.Result(r, req.Names, c.options.Limits.MaxResponseBytes)
			if err == nil {
				err = secretsutil.LeaseBounds(r, req.Lease)
			}
			ch <- outcome{r, err}
		}()
	}
	for i := range window {
		launch(i)
	}
	result := jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{}}
	var memberErr, mergeErr error
	for i := range c.members {
		var got outcome
		select {
		case got = <-window[i%len(window)]:
		case <-ctx.Done():
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretsUnavailable
		}
		if got.err != nil && memberErr == nil {
			memberErr = got.err
		}
		if got.err == nil && memberErr == nil && mergeErr == nil {
			for name, value := range got.result.Secrets {
				if _, exists := result.Secrets[name]; exists {
					continue
				}
				value.ProviderID = c.members[i].ID
				if value.ExpiresAt != nil {
					expiry := *value.ExpiresAt
					value.ExpiresAt = &expiry
				}
				result.Secrets[name] = value
			}
			_, mergeErr = secretsutil.Result(result, req.Names, c.options.Limits.MaxResponseBytes)
		}
		if memberErr != nil || mergeErr != nil {
			result.Secrets = nil
		}
		if next := i + len(window); next < len(c.members) {
			launch(next)
		}
	}
	if ctx.Err() != nil {
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretsUnavailable
	}
	if memberErr != nil {
		return jobdb.SecretResolveResult{}, memberErr
	}
	if mergeErr != nil {
		return jobdb.SecretResolveResult{}, mergeErr
	}
	result, err := secretsutil.Result(result, req.Names, c.options.Limits.MaxResponseBytes)
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	if c.options.Observe != nil {
		for _, name := range req.Names {
			if v, ok := result.Secrets[name]; ok {
				c.options.Observe(Selection{req.TenantID, req.JobID, name, v.ProviderID})
			}
		}
	}
	if ctx.Err() != nil {
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretsUnavailable
	}
	return result, nil
}

var _ jobdb.SecretProvider = (*Collection)(nil)
