//+build linux,amd64

/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bulletproofs_cgo

const SINGLE_PROOF_SIZE = 672
const POINT_SIZE = 32

const OK = 0
const ERR_INVALID_PROOF = -1
const ERR_INVALID_INPUT = -2
const ERR_INTERNAL_ERROR = -3
const ERR_NULL_INPUT = -4

const ERR_MSG_INVALID_PROOF = "invalid proof"
const ERR_MSG_INVALID_INPUT = "wrong input data format"
const ERR_MSG_NULL_INPUT = "input is null"
const ERR_MSG_DEFAULT = "unknown error"

func getErrMsg(code int64) string {
	switch code {
	case -1:
		return ERR_MSG_INVALID_PROOF
	case -2:
		return ERR_MSG_INVALID_INPUT
	case -4:
		return ERR_MSG_NULL_INPUT
	default:
		return ERR_MSG_DEFAULT
	}
}
