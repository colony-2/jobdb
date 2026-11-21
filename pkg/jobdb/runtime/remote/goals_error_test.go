package remote

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestGoalResponseErrorIdentityAndText(t *testing.T) {
	for _, entry := range goalErrors {
		t.Run(entry.code, func(t *testing.T) {
			for _, message := range []string{"", entry.err.Error(), "distinct detail", "context: " + entry.err.Error(), entry.err.Error() + ": detail"} {
				raw, err := json.Marshal(map[string]string{"code": entry.code, "message": message})
				require.NoError(t, err)
				decoded := goalResponseError(raw)
				expected := message
				if expected == "" {
					expected = entry.err.Error()
				}
				for range 4 {
					require.ErrorIs(t, decoded, entry.err)
					require.EqualError(t, decoded, expected)
					w := httptest.NewRecorder()
					require.True(t, writeGoalError(w, decoded))
					decoded = goalResponseError(w.Body.Bytes())
				}
			}
		})
	}
}

func TestGoalResponseErrorUnknownIsUntyped(t *testing.T) {
	for _, raw := range []string{`{"code":"unknown","message":"primary goal selection is forbidden"}`, `{"message":"primary goal selection is forbidden"}`, `not json`} {
		require.Nil(t, goalResponseError([]byte(raw)))
		err := responseError("submit", 403, []byte(raw), nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, jobdb.ErrPrimaryGoalSelectionForbidden, fmt.Sprint(raw))
	}
}

func TestProtectedRejectionsPreserveIdentityAcrossResponseHelpers(t *testing.T) {
	for _, entry := range goalErrors {
		if entry.status < 500 {
			continue
		}
		raw, err := json.Marshal(map[string]string{"code": entry.code, "message": entry.err.Error()})
		require.NoError(t, err)
		for _, decode := range []func(int, []byte) error{
			func(status int, body []byte) error { return responseError("restart", status, body, nil) },
			func(status int, body []byte) error {
				return responseErrorWithConflict("restart", status, body, nil, nil, nil, nil)
			},
			func(status int, body []byte) error { return explicitJobCreateError("restart", status, body, nil) },
		} {
			require.ErrorIs(t, decode(entry.status, raw), entry.err)
			require.False(t, jobdb.IsExecutionInfrastructureError(jobdb.ClassifyRuntimeIO(decode(entry.status, raw), "restart", nil, nil)))
			// A success or arbitrary server status carrying a semantic-looking
			// body must still be treated as malformed infrastructure.
			for _, wrong := range []int{200, 502} {
				require.True(t, jobdb.IsExecutionInfrastructureError(decode(wrong, raw)))
			}
		}
	}
}
