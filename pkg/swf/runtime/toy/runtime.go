package toy

import (
	"github.com/colony-2/swf-go/pkg/swf"
	toyengine "github.com/colony-2/swf-go/pkg/swf/toy"
)

// Runtime builds SWF engines backed by the in-memory toy implementation.
type Runtime struct {
	opts []toyengine.Option
}

func New(opts ...toyengine.Option) *Runtime {
	cloned := make([]toyengine.Option, len(opts))
	copy(cloned, opts)
	return &Runtime{opts: cloned}
}

func (r *Runtime) BuildEngine(workers []swf.WorkSet, opts swf.RuntimeBuildOptions) (swf.SWFEngine, error) {
	engineOpts := make([]toyengine.Option, 0, len(r.opts)+1)
	engineOpts = append(engineOpts, r.opts...)
	if opts.Logger != nil {
		engineOpts = append(engineOpts, toyengine.WithLogger(opts.Logger))
	}
	return toyengine.NewToyEngine(workers, engineOpts...), nil
}
