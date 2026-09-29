package storage

import (
	"context"
	"database/sql"
)

type appendMutationKey struct{}

// WithAppendMutation attaches a scheduler mutation to the next SQL chapter
// append. It runs inside the append transaction, before chapter rows are written.
// Artifact preparation happens before this transaction starts.
func WithAppendMutation(ctx context.Context, mutation func(*sql.Tx) error) context.Context {
	return context.WithValue(ctx, appendMutationKey{}, mutation)
}

// ApplyAppendMutation runs the mutation, if any. An error aborts the append.
func ApplyAppendMutation(ctx context.Context, tx *sql.Tx) error {
	if mutation, ok := ctx.Value(appendMutationKey{}).(func(*sql.Tx) error); ok {
		return mutation(tx)
	}
	return nil
}
