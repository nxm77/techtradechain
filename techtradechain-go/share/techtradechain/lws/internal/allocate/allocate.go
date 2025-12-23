/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package allocate

import (
	"errors"
	"os"
)

var (
	OsPageSize        = os.Getpagesize()
	strNegativeOffset = "negative offset"
	End               = errors.New("END")
	OsPageMask        = OsPageSize - 1
)
