package impl

import "github.com/colony-2/swf-go/pkg/swf"

type noopReplayObserver struct{}

func (noopReplayObserver) OnJobStart(event swf.JobStartEvent)  {}
func (noopReplayObserver) OnTaskStart(event swf.TaskStartEvent) {}
func (noopReplayObserver) OnTaskEnd(event swf.TaskEndEvent)     {}
func (noopReplayObserver) OnJobEnd(event swf.JobEndEvent)       {}
