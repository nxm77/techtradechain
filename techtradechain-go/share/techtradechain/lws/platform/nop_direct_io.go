//go:build !linux && !freebsd
// +build !linux,!freebsd

/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package platform

import "os"

var (
	O_DIRECT = os.O_RDONLY
)
