/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

import (
	"regexp"
)

var (
	contractNameReg  = regexp.MustCompile("^[a-zA-Z0-9_]{1,128}$")
	evmAddressHexReg = regexp.MustCompile("^(0x)?[0-9a-fA-F]{40}$")
	zxlAddrHexReg    = regexp.MustCompile("^ZX[0-9a-fA-F]{40}$")
	chainIdReg       = regexp.MustCompile("^[a-zA-Z0-9_]{1,30}$")
	txIDReg          = regexp.MustCompile(`^\S{1,64}$`)
	//reservedAddressLen = 4 // FFF -> 4096
)

// CheckChainIdFormat check chain id format
func CheckChainIdFormat(chainId string) bool {
	return chainIdReg.MatchString(chainId)
}

// CheckContractNameFormat check contract name format
func CheckContractNameFormat(name string) bool {
	return contractNameReg.MatchString(name)
}

// CheckEvmAddressFormat check evm address format
func CheckEvmAddressFormat(addr string) bool {
	//if len(addr) <= reservedAddressLen {
	//	return false
	//}
	//return evmutils.FromDecimalString(addr) != nil
	return evmAddressHexReg.MatchString(addr)
}

// CheckZxlAddrFormat check evm address format
func CheckZxlAddrFormat(addr string) bool {
	return zxlAddrHexReg.MatchString(addr)
}

// CheckTxIDFormat check tx id format
func CheckTxIDFormat(txID string) bool {
	return txIDReg.MatchString(txID)
}
