package story

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/artifact"
)

// Chapter represents both staged and persisted chapter content.
type Chapter interface {
	Ordinal() int64
	Body() []byte
	Artifacts() []artifact.Artifact
	AddArtifact(artifact.Artifact) Chapter
}

type ChapterBuilder struct {
	inner *chapterImpl
}

func NewChapter() *ChapterBuilder {
	return &ChapterBuilder{inner: &chapterImpl{ordinal: -1}}
}

func NewPersistedChapter(ordinal int64, body []byte, arts []artifact.Artifact) Chapter {
	ch := &chapterImpl{ordinal: ordinal, body: append([]byte(nil), body...)}
	for _, art := range arts {
		ch.AddArtifact(art)
	}
	return ch
}

func (b *ChapterBuilder) WithJSON(r io.Reader) *ChapterBuilder {
	data, err := io.ReadAll(r)
	if err != nil {
		b.inner.bodyErr = err
		return b
	}
	if !json.Valid(data) {
		b.inner.bodyErr = fmt.Errorf("chapter: body is not valid JSON")
		return b
	}
	b.inner.body = append([]byte(nil), data...)
	return b
}

func (b *ChapterBuilder) WithBytes(data []byte) *ChapterBuilder {
	b.inner.body = append([]byte(nil), data...)
	return b
}

func (b *ChapterBuilder) WithOrdinal(ordinal int64) *ChapterBuilder {
	b.inner.ordinal = ordinal
	return b
}

func (b *ChapterBuilder) AddArtifact(a artifact.Artifact) Chapter {
	b.inner.AddArtifact(a)
	return b
}

func (b *ChapterBuilder) Ordinal() int64                 { return b.inner.Ordinal() }
func (b *ChapterBuilder) Body() []byte                   { return b.inner.Body() }
func (b *ChapterBuilder) Artifacts() []artifact.Artifact { return b.inner.Artifacts() }

type chapterImpl struct {
	ordinal int64
	body    []byte
	bodyErr error

	mu        sync.RWMutex
	artifacts []artifact.Artifact
}

func (c *chapterImpl) Ordinal() int64 { return c.ordinal }

func (c *chapterImpl) Body() []byte {
	if c.bodyErr != nil {
		return nil
	}
	out := make([]byte, len(c.body))
	copy(out, c.body)
	return out
}

func (c *chapterImpl) AddArtifact(a artifact.Artifact) Chapter {
	if a == nil {
		return c
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.artifacts = append(c.artifacts, a)
	return c
}

func (c *chapterImpl) Artifacts() []artifact.Artifact {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]artifact.Artifact, len(c.artifacts))
	copy(out, c.artifacts)
	return out
}
