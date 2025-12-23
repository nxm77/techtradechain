//go:build linux || darwin
// +build linux darwin

/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package platform

import (
	"syscall"
)

var (
	Mlock = syscall.Mlock
)
