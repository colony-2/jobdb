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
