package common

import "errors"

var (
	ERR_LOCK_EXISTS_ERROR = errors.New("锁已被占用")
)
