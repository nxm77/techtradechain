/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sdf

import "math/big"

// ECCSignature represents a ecc signature
type ECCSignature struct {
	R *big.Int
	S *big.Int
}
