// The external checker builds this executable against the published module.
package main

import (
	"encoding/json"
	"os"
	"runtime/debug"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func main() {
	build, ok := debug.ReadBuildInfo()
	if !ok {
		panic("linked build information unavailable")
	}
	var linked *debug.Module
	for _, dep := range build.Deps {
		if dep.Path == "github.com/colony-2/jobdb" {
			linked = dep
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(struct {
		Current jobdb.ModuleBuildInfo
		Linked  *debug.Module
	}{jobdb.CurrentBuildInfo(), linked}); err != nil {
		panic(err)
	}
}
