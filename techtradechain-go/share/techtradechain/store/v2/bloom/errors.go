/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bloom

import "errors"

var (
	//ErrNoDumpFile no specify dump file error
	ErrNoDumpFile = errors.New("No dump file specified")
	//ErrNoBlockLoader no specify block loader error
	ErrNoBlockLoader = errors.New("No block loader specified")
	//ErrInvalidFile bloom file is invalid
	ErrInvalidFile = errors.New("invalid bloom file")
	//ErrVersionMismatch version mismatch
	ErrVersionMismatch = errors.New("version mismatch")
	//ErrChecksum data is bad
	ErrChecksum = errors.New("checksum error")
	//ErrDumpBeforeSynced cant dump manually before sync complete
	ErrDumpBeforeSynced = errors.New("cant dump manually before sync complete")
	//ErrDumpBusy dump is doing by others
	ErrDumpBusy = errors.New("dump busily")
	//ErrMetaAllInvalid meta both are bad
	ErrMetaAllInvalid = errors.New("meta can't both invalid")
)
