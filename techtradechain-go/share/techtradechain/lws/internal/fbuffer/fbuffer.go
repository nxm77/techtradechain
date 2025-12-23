/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package fbuffer

import "os"

var (
	OsPageSize        = os.Getpagesize()
	strNegativeOffset = "negative offset"
	strSeekOffInvaild = "seek offset invaild"
	strInvaildArg     = "arguments invaild"
)

type area struct {
	off int64
	len int
}

func mergeArea(a area, b area) area {
	if a.len == 0 {
		return b
	}
	if b.len == 0 {
		return a
	}
	aEnd := a.off + int64(a.len)
	bEnd := b.off + int64(b.len)
	if a.off <= b.off && aEnd >= bEnd {
		return a
	}
	if a.off > b.off && aEnd < bEnd {
		return b
	}
	if bEnd < a.off {
		return area{
			off: b.off,
			len: int(aEnd - b.off),
		}
	} else if b.off > aEnd {
		return area{
			off: a.off,
			len: int(bEnd - a.off),
		}
	} else if a.off < b.off {
		a.off = b.off
	} else {
		a.len = b.len
	}
	return a
}

func overlapArea(a area, b area) area {
	aEnd := a.off + int64(a.len)
	bEnd := b.off + int64(b.len)
	if a.off <= b.off && aEnd >= bEnd {
		return b
	}
	if a.off > b.off && aEnd < bEnd {
		return a
	}
	if bEnd < a.off || b.off > aEnd {
		return area{}
	} else if a.off < b.off {
		return area{
			off: b.off,
			len: int(bEnd - a.off),
		}
	} else {
		return area{
			off: a.off,
			len: int(aEnd - b.off),
		}
	}
}

func alignUp(n, a uint64) uint64 {
	return (n + a - 1) &^ (a - 1)
}

func alignDown(n, a uint64) uint64 {
	return n &^ (a - 1)
}
