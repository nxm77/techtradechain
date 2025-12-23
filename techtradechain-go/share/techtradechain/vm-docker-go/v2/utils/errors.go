/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import "errors"

var (
	ErrDuplicateTxId    = errors.New("duplicate txId")
	ErrMissingByteCode  = errors.New("missing bytecode")
	ErrClientReachLimit = errors.New("clients reach limit")
)
