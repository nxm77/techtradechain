/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import "errors"

var (
	// ErrContractIdIsNil err msg
	ErrContractIdIsNil = errors.New("the contractId is empty")
	// ErrContractNotFound err msg
	ErrContractNotFound = errors.New("the contractName is not exist")
	// ErrTxTypeNotSupport err msg
	ErrTxTypeNotSupport = errors.New("the txType does not support")
	// ErrMethodNotFound err msg
	ErrMethodNotFound = errors.New("the method does not found")
	// ErrParamsEmpty err msg
	ErrParamsEmpty = errors.New("the params is empty")
	// ErrContractName err msg
	ErrContractName = errors.New("the contractName is error")
	// ErrOutOfRange err msg
	ErrOutOfRange = errors.New("out of range")
	// ErrParams err msg
	ErrParams = errors.New("params is error")
	// ErrSequence err msg
	ErrSequence = errors.New("sequence is error")
	// ErrUnmarshalFailed err msg
	ErrUnmarshalFailed = errors.New("unmarshal is error")
)
