package toy

import (
	"log/slog"

	"github.com/colony-2/jobdb/pkg/jobdb"
	toyimpl "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy/internal/toyimpl"
)

type Runtime = toyimpl.Runtime
type JobIDGenerator = toyimpl.JobIDGenerator
type Option = toyimpl.Option

func New(opts ...Option) *Runtime {
	return toyimpl.New(opts...)
}

func WithLogger(logger *slog.Logger) Option {
	return toyimpl.WithLogger(logger)
}

func WithJobIDGenerator(gen JobIDGenerator) Option {
	return toyimpl.WithJobIDGenerator(gen)
}

// WithSecretProvider sets a caller-owned provider independently of job storage.
func WithSecretProvider(provider jobdb.SecretProvider) Option {
	return toyimpl.WithSecretProvider(provider)
}

// WithSecretLimits bounds provider calls and their results.
func WithSecretLimits(limits jobdb.SecretLimits) Option { return toyimpl.WithSecretLimits(limits) }

// WithConfigurationLimits bounds tenant configuration reads and writes.
func WithConfigurationLimits(limits jobdb.ConfigurationLimits) Option {
	return toyimpl.WithConfigurationLimits(limits)
}

// WithMaxChapterApplicationPayloadBytes sets the per-chapter application budget.
// Zero uses 256 KiB. A negative limit panics when constructing the toy runtime.
func WithMaxChapterApplicationPayloadBytes(limit int64) Option {
	return toyimpl.WithMaxChapterApplicationPayloadBytes(limit)
}
