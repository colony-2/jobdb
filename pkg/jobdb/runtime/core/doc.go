// Package runtimecore defines the public backend boundary for composing a
// JobDB WorkflowRuntime from external scheduler, chapter-log, artifact, and
// schema persistence.
//
// The package intentionally exposes logical JobDB concepts instead of the
// internal encodings used by built-in runtimes. Scheduler implementations own
// durable state and atomic lease mutations. Runtime core owns workflow
// semantics such as schema resolution, chapter validation, retry policy, and
// conversion to public jobdb API values.
package runtimecore
