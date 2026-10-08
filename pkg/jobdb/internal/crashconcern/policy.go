// Package crashconcern defines the built-in schedulers' crash-concern policy.
package crashconcern

// DefaultThreshold matches pgjobdb.crash_concern_threshold(). Counters advance
// when an expired lease is reclaimed, not on reads or renewal. A live lease is
// ACTIVE; once it expires, a counter at this threshold prevents further claims.
// Successful rescheduling resets the consecutive counter.
const DefaultThreshold = 5
