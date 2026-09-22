package model

import "errors"

var (
	// ErrNotFound indicates a run id is unknown to the store.
	ErrNotFound = errors.New("run not found")
)
