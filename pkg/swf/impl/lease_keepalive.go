package impl

import (
	"github.com/colony-2/pgwf-go/pkg/pgwf"
	_ "unsafe"
)

type keepAliveStopper interface {
	StopKeepAlive()
}

func stopLeaseKeepAlive(lease *pgwf.Lease) {
	if lease == nil {
		return
	}
	if stopper, ok := any(lease).(keepAliveStopper); ok {
		stopper.StopKeepAlive()
		return
	}
	stopKeepAliveFallback(lease)
}

//go:linkname stopKeepAliveFallback github.com/colony-2/pgwf-go/pkg/pgwf.(*Lease).stopKeepAlive
func stopKeepAliveFallback(*pgwf.Lease)
