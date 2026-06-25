package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
)

const DefaultMaxMaterializeBytes int64 = 4 << 20

type Artifact interface {
	ID() string
	Name() string
	ContentType() string
	SizeBytes() int64
	Sha256(context.Context) (string, error)

	WriteTo(ctx context.Context, w io.Writer) error
	SaveToFile(ctx context.Context, path string) error
	Bytes(ctx context.Context) ([]byte, error)

	ToInput(ctx context.Context) (Descriptor, io.ReadCloser, error)
}

type Descriptor struct {
	Name        string
	ContentType string
	SizeBytes   int64
	Sha256      string
}

type Option func(*artifactImpl)

func WithMaxMaterializeBytes(limit int64) Option {
	return func(a *artifactImpl) {
		if limit > 0 {
			a.maxMaterialize = limit
		}
	}
}

func WithID(id string) Option {
	return func(a *artifactImpl) {
		a.id = id
	}
}

func WithSha256(digest string) Option {
	return func(a *artifactImpl) {
		a.sha256 = digest
	}
}

func FromBytes(name, contentType string, data []byte, opts ...Option) Artifact {
	cp := append([]byte(nil), data...)
	return newArtifact(name, contentType, int64(len(cp)), &bytesSource{data: cp}, opts...)
}

func FromReader(name, contentType string, size int64, opener func(context.Context) (io.ReadCloser, error), opts ...Option) Artifact {
	return newArtifact(name, contentType, size, readerSource(opener), opts...)
}

func FromFile(path, contentType string, opts ...Option) (Artifact, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("artifact: stat file: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("artifact: %s is a directory", path)
	}
	opener := func(context.Context) (io.ReadCloser, error) {
		return os.Open(path)
	}
	return newArtifact(filepath.Base(path), contentType, info.Size(), readerSource(opener), opts...), nil
}

type artifactImpl struct {
	id          string
	name        string
	contentType string
	size        int64
	sha256      string

	maxMaterialize int64
	src            bodySource

	cacheOnce sync.Once
	cache     []byte
	cacheErr  error
}

func newArtifact(name, contentType string, size int64, src bodySource, opts ...Option) Artifact {
	a := &artifactImpl{
		name:           name,
		contentType:    fallback(contentType, "application/octet-stream"),
		size:           size,
		src:            src,
		maxMaterialize: DefaultMaxMaterializeBytes,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *artifactImpl) ID() string          { return a.id }
func (a *artifactImpl) Name() string        { return a.name }
func (a *artifactImpl) ContentType() string { return a.contentType }
func (a *artifactImpl) SizeBytes() int64    { return a.size }
func (a *artifactImpl) Sha256(ctx context.Context) (string, error) {
	return a.Digest(ctx)
}

func (a *artifactImpl) WriteTo(ctx context.Context, w io.Writer) error {
	reader, err := a.src.Open(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()
	_, err = io.Copy(w, reader)
	return err
}

func (a *artifactImpl) SaveToFile(ctx context.Context, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return a.WriteTo(ctx, f)
}

func (a *artifactImpl) Bytes(ctx context.Context) ([]byte, error) {
	a.cacheOnce.Do(func() {
		if a.size > 0 && a.maxMaterialize > 0 && a.size > a.maxMaterialize {
			a.cacheErr = core.ErrTooLarge
			return
		}
		reader, err := a.src.Open(ctx)
		if err != nil {
			a.cacheErr = err
			return
		}
		defer reader.Close()

		var buf bytes.Buffer
		if a.maxMaterialize > 0 {
			limited := io.LimitReader(reader, a.maxMaterialize+1)
			if _, err = buf.ReadFrom(limited); err != nil {
				a.cacheErr = err
				return
			}
			if int64(buf.Len()) > a.maxMaterialize {
				a.cacheErr = core.ErrTooLarge
				return
			}
		} else if _, err = buf.ReadFrom(reader); err != nil {
			a.cacheErr = err
			return
		}
		a.cache = buf.Bytes()
	})
	if a.cacheErr != nil {
		return nil, a.cacheErr
	}
	return append([]byte(nil), a.cache...), nil
}

func (a *artifactImpl) Digest(ctx context.Context) (string, error) {
	if a.sha256 != "" {
		return a.sha256, nil
	}
	reader, err := a.src.Open(ctx)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	hash := sha256.New()
	n, err := io.Copy(hash, reader)
	if err != nil {
		return "", err
	}
	if a.size > 0 && n != a.size {
		return "", fmt.Errorf("artifact: read %d bytes, expected %d", n, a.size)
	}
	a.sha256 = fmt.Sprintf("%x", hash.Sum(nil))
	return a.sha256, nil
}

func (a *artifactImpl) ToInput(ctx context.Context) (Descriptor, io.ReadCloser, error) {
	reader, err := a.src.Open(ctx)
	if err != nil {
		return Descriptor{}, nil, err
	}
	return Descriptor{Name: a.name, ContentType: a.contentType, SizeBytes: a.size, Sha256: a.sha256}, reader, nil
}

type bodySource interface {
	Open(ctx context.Context) (io.ReadCloser, error)
}

type bytesSource struct {
	data []byte
}

func (b *bytesSource) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(append([]byte(nil), b.data...))), nil
}

type funcSource struct {
	fn func(context.Context) (io.ReadCloser, error)
}

func (f *funcSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return f.fn(ctx)
}

func readerSource(fn func(context.Context) (io.ReadCloser, error)) bodySource {
	return &funcSource{fn: fn}
}

func fallback(value, def string) string {
	if strings.TrimSpace(value) == "" {
		return def
	}
	return value
}
