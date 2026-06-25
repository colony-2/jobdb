package core

import "errors"

var (
	ErrNotFound     = errors.New("chapterstore: resource not found")
	ErrConflict     = errors.New("chapterstore: conflict")
	ErrUnauthorized = errors.New("chapterstore: unauthorized")
	ErrTooLarge     = errors.New("chapterstore: payload too large")
)
