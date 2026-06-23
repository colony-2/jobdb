package directimpl

import (
	"context"
	"errors"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
)

const (
	completionStatusSuccess       pgwf.CompletionStatus = "success"
	completionStatusFailedApp     pgwf.CompletionStatus = "failed_app"
	completionStatusFailedSystem  pgwf.CompletionStatus = "failed_system"
	completionStatusFailedTimeout pgwf.CompletionStatus = "failed_timeout"
	completionStatusCancelled     pgwf.CompletionStatus = "cancelled"
)

func completionStatusAndDetail(err error) (pgwf.CompletionStatus, string) {
	if err == nil {
		return completionStatusSuccess, ""
	}

	if errors.Is(err, jobdb.ErrJobCancelled) || errors.Is(err, context.Canceled) {
		return completionStatusCancelled, err.Error()
	}

	var te jobdb.TimeoutError
	if errors.As(err, &te) {
		return completionStatusFailedTimeout, messageOrFallback(te.Payload.Message, err)
	}

	var ae jobdb.AppError
	if errors.As(err, &ae) {
		return completionStatusFailedApp, messageOrFallback(ae.Payload.Message, err)
	}

	var se jobdb.SystemError
	if errors.As(err, &se) {
		return completionStatusFailedSystem, messageOrFallback(se.Payload.Message, err)
	}

	return completionStatusFailedSystem, messageOrFallback("", err)
}

func messageOrFallback(message string, err error) string {
	if message != "" {
		return message
	}
	if err != nil {
		return err.Error()
	}
	return ""
}
