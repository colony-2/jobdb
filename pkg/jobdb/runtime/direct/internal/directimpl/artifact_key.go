package directimpl

import "github.com/colony-2/jobdb/pkg/jobdb"

func assignArtifactKeys(artifacts []jobdb.Artifact, jobID string, ordinal int64) {
	if jobID == "" || ordinal < 0 {
		return
	}
	for _, art := range artifacts {
		if art == nil {
			continue
		}
		name := art.Name()
		if name == "" {
			continue
		}
		jobdb.AssignArtifactKey(art, jobdb.ArtifactKey{
			JobId:       jobID,
			TaskOrdinal: ordinal,
			Name:        name,
			SizeBytes:   art.Size(),
		})
	}
}
