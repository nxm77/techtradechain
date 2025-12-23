/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package platform

import "runtime"

func IsWindows() bool {
	return runtime.GOOS == "windows"
}
