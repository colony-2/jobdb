package jobdb

import "runtime/debug"

const modulePath = "github.com/colony-2/jobdb"

type ModuleReplacement struct {
	Path    string
	Version string
}
type ModuleBuildInfo struct {
	Path        string
	Version     string
	Revision    *string
	Modified    *bool
	Replacement *ModuleReplacement
}

// CurrentBuildInfo identifies the linked JobDB module, never the consumer's VCS.
func CurrentBuildInfo() ModuleBuildInfo {
	info, _ := debug.ReadBuildInfo()
	return moduleBuildInfo(info)
}
func moduleBuildInfo(info *debug.BuildInfo) ModuleBuildInfo {
	out := ModuleBuildInfo{Path: modulePath, Version: "(devel)"}
	if info == nil {
		return out
	}
	var found *debug.Module
	main := info.Main.Path == modulePath
	if main {
		found = &info.Main
	} else {
		for _, dep := range info.Deps {
			if dep.Path == modulePath {
				found = dep
				break
			}
		}
	}
	if found == nil {
		return out
	}
	if found.Version != "" {
		out.Version = found.Version
	}
	if found.Replace != nil {
		out.Replacement = &ModuleReplacement{Path: found.Replace.Path, Version: found.Replace.Version}
		out.Version = "(devel)"
		if found.Replace.Version != "" {
			out.Version = found.Replace.Version
		}
	}
	if main {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				v := s.Value
				out.Revision = &v
			case "vcs.modified":
				if s.Value == "true" || s.Value == "false" {
					v := s.Value == "true"
					out.Modified = &v
				}
			}
		}
	}
	return out
}
