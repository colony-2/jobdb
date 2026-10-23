package remote

import "net/http"

// Credentials never follow redirects on delegated, secret or configuration
// requests. Macaroon grants need no access-token refresh cache or request replay.
type delegationTransport struct {
	original   *http.Client
	restricted *http.Client
}

func newDelegationTransport(client *http.Client) *delegationTransport {
	if client == nil {
		client = http.DefaultClient
	}
	restricted := *client
	restricted.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &delegationTransport{original: client, restricted: &restricted}
}
func (d *delegationTransport) Do(req *http.Request) (*http.Response, error) {
	if isDelegationToken(req.Header.Get("X-JobDB-Lease-Token")) || isSecretResolutionPath(req.URL.EscapedPath()) || isConfigurationPath(req.URL.EscapedPath()) {
		return d.restricted.Do(req)
	}
	return d.original.Do(req)
}
