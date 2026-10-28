package jobdb

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type forbiddenArtifact struct {
	Artifact
	opens *int
}

func (a forbiddenArtifact) Bytes(context.Context) ([]byte, error) {
	*a.opens++
	return nil, errors.New("unexpected artifact read")
}

type forbiddenGetter struct{ calls *int }

func (g forbiddenGetter) GetArtifact(string, ArtifactKey) (Artifact, error) {
	*g.calls++
	return nil, errors.New("opened lazily")
}
func TestExecutionArtifactsAreLazyAndKeepLogicalNames(t *testing.T) {
	calls := 0
	a := NewArtifactFromBytes("logical", []byte("bytes"))
	AssignArtifactKey(a, ArtifactKey{JobId: "ancestor", TaskOrdinal: 3, Name: "stored", SizeBytes: 5})
	snapshot, err := CaptureExecutionArtifact(context.Background(), forbiddenArtifact{a, &calls})
	require.NoError(t, err)
	require.Zero(t, calls)
	lazy := snapshot.ToArtifact(forbiddenGetter{&calls}, "tenant")
	require.Equal(t, "logical", lazy.Name())
	require.EqualValues(t, 5, lazy.Size())
	require.Zero(t, calls)
	_, err = lazy.Open()
	require.ErrorContains(t, err, "opened lazily")
	require.Equal(t, 1, calls)
}
func TestStagedExecutionArtifactPublishesItsKey(t *testing.T) {
	a, err := CaptureExecutionArtifact(context.Background(), NewArtifactFromBytes("logical", []byte("data")))
	require.NoError(t, err)
	upload, err := a.Stage(ArtifactKey{JobId: "job", TaskOrdinal: 4, Name: "stored"})
	require.NoError(t, err)
	require.Equal(t, "stored", upload.Name)
	out := a.ToArtifact(nil, "tenant")
	require.Equal(t, "logical", out.Name())
	key, err := out.ArtifactKey()
	require.NoError(t, err)
	require.Equal(t, *a.Key, key)
	r, err := upload.Open()
	require.NoError(t, err)
	defer r.Close()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), data)
}
