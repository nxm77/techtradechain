/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import "errors"

var (
	// ErrDuplicateTxId is the duplicate tx id err
	ErrDuplicateTxId = errors.New("duplicate txId")
	// ErrMissingByteCode is the missing bytecode err
	ErrMissingByteCode = errors.New("missing bytecode")
	// ErrClientReachLimit is the client limit err
	ErrClientReachLimit = errors.New("clients reach limit")
)
