package remote

import (
	"context"
	"net/http"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

func (s *proxyServer) GetJobGoalCapabilities(_ context.Context, _ runtimeapi.GetJobGoalCapabilitiesRequestObject) (runtimeapi.GetJobGoalCapabilitiesResponseObject, error) {
	var capabilities jobdb.JobGoalCapabilities
	if provider, ok := s.runtime.(jobdb.JobGoalCapability); ok {
		capabilities = provider.JobGoalCapabilities()
	}
	return runtimeapi.GetJobGoalCapabilities200JSONResponse{
		ImmutableMembership:    capabilities.ImmutableMembership,
		AtomicAdmission:        capabilities.AtomicAdmission,
		TerminalFailureRecords: capabilities.TerminalFailureRecords,
	}, nil
}

// GetJobGoalCapabilities queries backend guarantees without inspecting goal
// state or attempting a mutation. Absent fields are false. An unavailable
// discovery route never implies support from the selection-only capability.
func (r *Runtime) GetJobGoalCapabilities(ctx context.Context) (jobdb.JobGoalCapabilities, error) {
	response, err := r.client.GetJobGoalCapabilitiesWithResponse(ctx)
	if err != nil {
		return jobdb.JobGoalCapabilities{}, err
	}
	if response.StatusCode() != http.StatusOK || response.JSON200 == nil {
		switch response.StatusCode() {
		case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented,
			http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
			http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			if typed := goalResponseError(response.Body); typed != nil {
				return jobdb.JobGoalCapabilities{}, typed
			}
			return jobdb.JobGoalCapabilities{}, jobdb.ErrJobGoalSemanticsUnsupported
		}
		return jobdb.JobGoalCapabilities{}, responseError("get job goal capabilities", response.StatusCode(), response.Body, nil)
	}
	return jobdb.JobGoalCapabilities{
		ImmutableMembership:    response.JSON200.ImmutableMembership,
		AtomicAdmission:        response.JSON200.AtomicAdmission,
		TerminalFailureRecords: response.JSON200.TerminalFailureRecords,
	}, nil
}

func supportsGoalPublication(runtime jobdb.WorkflowRuntime, failure bool) bool {
	provider, ok := runtime.(jobdb.JobGoalCapability)
	if !ok {
		return false
	}
	caps := provider.JobGoalCapabilities()
	if failure {
		return caps.TerminalFailureRecords
	}
	return caps.ImmutableMembership && caps.AtomicAdmission
}

func (r *Runtime) requireGoalPublication(ctx context.Context, failure bool) error {
	caps, err := r.GetJobGoalCapabilities(ctx)
	if err != nil {
		return err
	}
	if failure && caps.TerminalFailureRecords || !failure && caps.ImmutableMembership && caps.AtomicAdmission {
		return nil
	}
	return jobdb.ErrJobGoalSemanticsUnsupported
}
