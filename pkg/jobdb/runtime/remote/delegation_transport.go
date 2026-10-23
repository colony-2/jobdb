package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

// Lazy refresh is attached to the existing client, so all lease-token operations
// (including PutChapter and ResolveSecrets) use it without a second credential.
// Cache keys are exact bearer strings, never unverified IDs from token claims.
type delegationTransport struct {
	original   *http.Client
	restricted *http.Client
	mu         sync.Mutex
	states     map[string]*delegationTokenState
}
type delegationTokenState struct {
	mu     sync.Mutex
	token  string
	expiry time.Time
}

func newDelegationTransport(client *http.Client) *delegationTransport {
	if client == nil {
		client = http.DefaultClient
	}
	restricted := *client
	restricted.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &delegationTransport{original: client, restricted: &restricted, states: map[string]*delegationTokenState{}}
}
func (d *delegationTransport) state(token string) *delegationTokenState {
	d.mu.Lock()
	defer d.mu.Unlock()
	if state := d.states[token]; state != nil {
		return state
	}
	// Eviction affects efficiency only: old signed tokens can still refresh.
	if len(d.states) >= 256 {
		clear(d.states)
	}
	state := &delegationTokenState{token: token}
	d.states[token] = state
	return state
}
func (d *delegationTransport) cached(token string) (string, time.Time) {
	if d == nil || !isDelegationToken(token) {
		return token, time.Time{}
	}
	state := d.state(token)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.token, state.expiry
}
func delegationExpiredResponse(resp *http.Response) bool {
	if resp == nil || resp.StatusCode != http.StatusUnauthorized || resp.Body == nil {
		return false
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	resp.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(raw), resp.Body), resp.Body}
	var body struct {
		Code string `json:"code"`
	}
	return err == nil && len(raw) <= 4096 && json.Unmarshal(raw, &body) == nil && body.Code == "delegation_access_token_expired"
}
func (d *delegationTransport) Do(req *http.Request) (*http.Response, error) {
	original := req.Header.Get("X-JobDB-Lease-Token")
	if !isDelegationToken(original) {
		if isSecretResolutionPath(req.URL.EscapedPath()) || isConfigurationPath(req.URL.EscapedPath()) {
			return d.restricted.Do(req)
		}
		return d.original.Do(req)
	}
	state := d.state(original)
	state.mu.Lock()
	token := state.token
	state.mu.Unlock()
	current := req.Clone(req.Context())
	current.Header.Set("X-JobDB-Lease-Token", token)
	resp, err := d.restricted.Do(current)
	// No retry for network errors, provider failures, permissions, parent loss,
	// or ambiguous mutation results. In particular, never recursively renew.
	if err != nil || strings.HasSuffix(req.URL.Path, "/keepalive") || !delegationExpiredResponse(resp) {
		return resp, err
	}
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		return resp, nil
	}
	_ = resp.Body.Close()
	state.mu.Lock()
	if state.token == token {
		fresh, expiry, refreshErr := d.refresh(req.Context(), req, token)
		if refreshErr != nil {
			state.mu.Unlock()
			return nil, refreshErr
		}
		state.token = fresh
		state.expiry = expiry
		// Alias fresh tokens to the same state, so handles that still retain the
		// original token observe every later refresh, including its current expiry.
		d.mu.Lock()
		if len(d.states) >= 256 {
			clear(d.states)
			d.states[original] = state
		}
		d.states[fresh] = state
		d.mu.Unlock()
	}
	next := state.token
	state.mu.Unlock()
	retry := req.Clone(req.Context())
	retry.Header.Set("X-JobDB-Lease-Token", next)
	if req.GetBody != nil {
		retry.Body, err = req.GetBody()
		if err != nil {
			return nil, err
		}
	}
	return d.restricted.Do(retry)
}
func (d *delegationTransport) refresh(ctx context.Context, original *http.Request, token string) (string, time.Time, error) {
	// The server already verified this token and its binding before responding
	// with the pre-dispatch expiry code. Use the original URL's lease prefix;
	// no URL, audience or callback is ever obtained from credential claims.
	parts := strings.Split(original.URL.EscapedPath(), "/")
	if len(parts) < 8 {
		return "", time.Time{}, jobdb.ErrExecutionLeaseLost
	}
	target := *original.URL
	target.RawPath = strings.Join(parts[:8], "/") + "/keepalive"
	decoded, err := url.PathUnescape(target.RawPath)
	if err != nil {
		return "", time.Time{}, jobdb.ErrExecutionLeaseLost
	}
	target.Path = decoded
	target.RawQuery = ""
	target.Fragment = ""
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), nil)
	if err != nil {
		return "", time.Time{}, jobdb.ErrExecutionLeaseLost
	}
	// Preserve deployment authentication headers supplied to the original call.
	req.Header = original.Header.Clone()
	req.Header.Del("Content-Type")
	req.Header.Del("Content-Length")
	req.Header.Set("X-JobDB-Lease-Token", token)
	resp, err := d.restricted.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("delegation renewal transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 {
			return "", time.Time{}, jobdb.ErrLeasePermissionDenied
		}
		if resp.StatusCode == 401 || resp.StatusCode == 404 || resp.StatusCode == 409 {
			return "", time.Time{}, jobdb.ErrExecutionLeaseLost
		}
		return "", time.Time{}, fmt.Errorf("delegation renewal failed: HTTP %d", resp.StatusCode)
	}
	var body runtimeapi.KeepAliveLeaseResponse
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 64<<10))
	if decoder.Decode(&body) != nil || body.Lease == nil || !isDelegationToken(body.LeaseToken) || body.Lease.LeaseToken != body.LeaseToken || body.Lease.ExpiresAt == nil || !body.Lease.ExpiresAt.After(time.Now()) || body.Lease.Delegation == nil {
		return "", time.Time{}, fmt.Errorf("invalid delegation renewal response")
	}
	return body.LeaseToken, *body.Lease.ExpiresAt, nil
}
