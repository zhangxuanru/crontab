package common

import "errors"

var (
	ERR_LOCK_EXISTS_ERROR = errors.New("锁已被占用")

	ERR_NO_LOCAL_IP_FOUND = errors.New("没有找到网卡IP")
)
