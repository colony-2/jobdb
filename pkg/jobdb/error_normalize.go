package jobdb

func normalizeComparableError(err error) error {
	switch e := err.(type) {
	case nil:
		return nil
	case *JobFailedError:
		if e == nil {
			return nil
		}
		cause := normalizeComparableError(e.Cause)
		if cause == e.Cause {
			return e
		}
		return &JobFailedError{Cause: cause}
	case *AppError:
		return e
	case AppError:
		copy := e
		copy.Payload.Attrs = cloneAttrs(e.Payload.Attrs)
		copy.Payload.Stacktrace = append([]string(nil), e.Payload.Stacktrace...)
		return &copy
	case *SystemError:
		return e
	case SystemError:
		copy := e
		copy.Payload.Stacktrace = append([]string(nil), e.Payload.Stacktrace...)
		return &copy
	case *TimeoutError:
		return e
	case TimeoutError:
		copy := e
		return &copy
	default:
		return err
	}
}
