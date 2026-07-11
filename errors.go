package lsm

import "errors"

var (
	ErrKeyNotFound  = errors.New("key not found")
	ErrCorrupted    = errors.New("data corrupted")
	ErrEngineClosed = errors.New("engine is closed")
)
