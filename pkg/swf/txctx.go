package swf

import (
	"context"

	"gorm.io/gorm"
)

type ctxKeyTx struct{}

// WithTx attaches a GORM transaction to the context for reuse by job/task APIs.
// Callers remain responsible for committing or rolling back the transaction.
func WithTx(ctx context.Context, tx *gorm.DB) context.Context {
	if tx == nil {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ctxKeyTx{}, tx)
}

// TxFromCtx extracts a GORM transaction previously stored with WithTx.
// ok is false when no transaction is present.
func TxFromCtx(ctx context.Context) (*gorm.DB, bool) {
	if ctx == nil {
		return nil, false
	}
	tx, ok := ctx.Value(ctxKeyTx{}).(*gorm.DB)
	if !ok || tx == nil {
		return nil, false
	}
	return tx, true
}
