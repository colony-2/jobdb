package remote

import (
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"testing"
)

func TestLeaseEventConformance(t *testing.T) {
	runPayloadAndRouteConformance(t, runtimetest.RunLeaseEventConformance)
}
