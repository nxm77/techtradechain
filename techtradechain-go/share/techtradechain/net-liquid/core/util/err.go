/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package util

import (
	"fmt"
	"net"
	"strings"
)

// ParseErrsToStr .
// @Description:
// @param errs
// @return string
func ParseErrsToStr(errs []error) string {
	tmp := "[%s]"
	res := ""
	for i := range errs {
		res += fmt.Sprintf(tmp, errs[i].Error())
	}
	return res
}

// IsNetError parse an error to a net.Error if it is an implementation of net.Error interface.
// IsNetError .
// @Description:
// @param err
// @return net.Error
// @return bool
func IsNetError(err error) (net.Error, bool) {
	e, ok := err.(net.Error)
	if ok {
		return e, ok
	}
	return nil, false
}

// IsNetErrorTemporary return the value of err.Temporary() if err is a net.Error.
// If err is not a net.Error, return false.
// IsNetErrorTemporary .
// @Description:
// @param err
// @return bool
func IsNetErrorTemporary(err error) bool {
	e, ok := IsNetError(err)
	if ok {
		return e.Temporary()
	}
	return false
}

// IsNetErrorTimeout return the value of err.Timeout() if err is a net.Error.
// If err is not a net.Error, return false.
// IsNetErrorTimeout .
// @Description:
// @param err
// @return bool
func IsNetErrorTimeout(err error) bool {
	e, ok := IsNetError(err)
	if ok {
		return e.Timeout()
	}
	return false
}

// IsConnClosedError return true if the info of err contains closed strings.
// IsConnClosedError .
// @Description:
// @param err
// @return bool
func IsConnClosedError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "Application error 0x0") || strings.Contains(errStr, "connection closed")
}
