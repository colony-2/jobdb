package runtimecore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
)

// PutChapter validates the current lease and appends a visible chapter.
func (r *Runtime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	if err := r.validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := req.Ref.JobKey.Validate(); err != nil {
		return err
	}
	if req.LeaseID == "" {
		return fmt.Errorf("lease id is required for PutChapter")
	}
	if req.Ref.Ordinal < 0 || req.Chapter.Ordinal != req.Ref.Ordinal {
		return fmt.Errorf("chapter ordinal does not match target ordinal")
	}
	if authorized, err := leaseauth.Authorize(ctx, req.Ref.JobKey, req.LeaseID); err != nil {
		return err
	} else if authorized {
		claims, _ := leaseauth.ClaimsFromContext(ctx)
		if claims.WorkerID == "" {
			return jobdb.ErrExecutionLeaseLost
		}
	}
	stored, err := r.scheduler.GetJob(ctx, req.Ref.JobKey)
	if err != nil {
		return err
	}
	if stored.LeaseWorkerID == "" {
		return jobdb.ErrExecutionLeaseLost
	}
	if claims, ok := leaseauth.ClaimsFromContext(ctx); ok && claims.WorkerID != stored.LeaseWorkerID {
		return jobdb.ErrExecutionLeaseLost
	}
	if _, err := r.scheduler.ValidateLease(ctx, LeaseIdentity{
		JobKey: req.Ref.JobKey, LeaseID: req.LeaseID,
		WorkerID: stored.LeaseWorkerID,
	}); err != nil {
		return err
	}
	logKey := ChapterLogKey{JobKey: req.Ref.JobKey}
	count, err := r.chapters.Count(ctx, logKey)
	if err != nil {
		return err
	}
	if req.Ref.Ordinal != count {
		return fmt.Errorf("%w: chapter ordinal %d is not appendable; expected %d",
			jobdb.ErrConflict, req.Ref.Ordinal, count)
	}
	chapter, uploads, err := prepareChapterWrite(req.Chapter, req.ArtifactUploads)
	if err != nil {
		return err
	}
	if err := ValidateOrdinaryChapter(ctx, r.schemas,
		jobdb.JobSchemaKey{TenantId: req.Ref.JobKey.TenantId,
			SchemaHash: stored.SchemaHash}, chapter); err != nil {
		return err
	}
	encoded, err := EncodeChapter(EncodeChapterRequest{Chapter: chapter, ArtifactUploads: uploads})
	if err != nil {
		return err
	}
	if err := r.chapters.Append(ctx, logKey, encoded); err != nil {
		if errors.Is(err, jobdb.ErrConflict) {
			return fmt.Errorf("%w: chapter ordinal %d already exists", jobdb.ErrConflict, req.Ref.Ordinal)
		}
		return err
	}
	return nil
}

func prepareChapterWrite(chapter jobdb.Chapter, input []jobdb.ArtifactUpload) (jobdb.Chapter, []jobdb.ArtifactUpload, error) {
	if len(input) == 0 {
		if len(chapter.Artifacts) > 0 {
			return jobdb.Chapter{}, nil, fmt.Errorf("chapter has artifact descriptors but no uploads")
		}
		return chapter, nil, nil
	}
	descriptors := make([]jobdb.StoredArtifact, 0, len(input))
	uploads := make([]jobdb.ArtifactUpload, 0, len(input))
	for _, item := range input {
		if item.Name == "" || item.Open == nil {
			return jobdb.Chapter{}, nil, fmt.Errorf("artifact name and opener are required")
		}
		stream, err := item.Open()
		if err != nil {
			return jobdb.Chapter{}, nil, err
		}
		body, readErr := io.ReadAll(stream)
		closeErr := stream.Close()
		if readErr != nil {
			return jobdb.Chapter{}, nil, readErr
		}
		if closeErr != nil {
			return jobdb.Chapter{}, nil, closeErr
		}
		hash := sha256.Sum256(body)
		descriptors = append(descriptors, jobdb.StoredArtifact{
			Name: item.Name, Digest: fmt.Sprintf("%x", hash), Size: int64(len(body)),
		})
		copied := append([]byte(nil), body...)
		uploads = append(uploads, jobdb.ArtifactUpload{
			Name: item.Name, Size: int64(len(copied)),
			Open: func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(copied)), nil
			},
		})
	}
	if len(chapter.Artifacts) > 0 {
		if len(chapter.Artifacts) != len(descriptors) {
			return jobdb.Chapter{}, nil, fmt.Errorf("chapter artifact descriptor count differs from uploads")
		}
		for i, want := range chapter.Artifacts {
			got := descriptors[i]
			if want.Name != got.Name || (want.Size != 0 && want.Size != got.Size) ||
				(want.Digest != "" && want.Digest != got.Digest) {
				return jobdb.Chapter{}, nil, fmt.Errorf("chapter artifact %q differs from upload", got.Name)
			}
		}
	}
	chapter.Artifacts = descriptors
	return chapter, uploads, nil
}
