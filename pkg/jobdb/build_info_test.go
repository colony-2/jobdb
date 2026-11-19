package jobdb

import (
	"github.com/stretchr/testify/require"
	"runtime/debug"
	"testing"
)

func TestModuleBuildInfo(t *testing.T) {
	for _, tc := range []struct {
		name        string
		info        *debug.BuildInfo
		version     string
		replacement bool
		revision    bool
	}{
		{"missing", nil, "(devel)", false, false},
		{"dependency", &debug.BuildInfo{Main: debug.Module{Path: "host", Version: "v8"}, Deps: []*debug.Module{{Path: modulePath, Version: "v0.0.0-20260101000000-abcdef123456"}}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "host-revision"}}}, "v0.0.0-20260101000000-abcdef123456", false, false},
		{"replacement", &debug.BuildInfo{Deps: []*debug.Module{{Path: modulePath, Version: "v1", Replace: &debug.Module{Path: "fork", Version: "v2"}}}}, "v2", true, false},
		{"local", &debug.BuildInfo{Deps: []*debug.Module{{Path: modulePath, Version: "v1", Replace: &debug.Module{Path: "../jobdb"}}}}, "(devel)", true, false},
		{"main", &debug.BuildInfo{Main: debug.Module{Path: modulePath, Version: "(devel)"}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}, {Key: "vcs.modified", Value: "true"}}}, "(devel)", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := moduleBuildInfo(tc.info)
			require.Equal(t, modulePath, out.Path)
			require.Equal(t, tc.version, out.Version)
			require.Equal(t, tc.replacement, out.Replacement != nil)
			require.Equal(t, tc.revision, out.Revision != nil)
			if tc.revision {
				require.Equal(t, "abc", *out.Revision)
				require.True(t, *out.Modified)
			} else {
				require.Nil(t, out.Modified)
			}
		})
	}
}
