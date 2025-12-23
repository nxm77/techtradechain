/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import (
	"errors"
	"fmt"

	"techtradechain.com/techtradechain/protocol/v2"
)

// CalculateGas calculate gas for syscall
func CalculateGas(
	defaultGas uint64,
	syscallGas uint64,
	gasPriceForParams uint64,
	paramsLen int,
	gasPriceForReturns uint64,
	returnsLen int) (uint64, error) {

	paramGas, err := mul64(gasPriceForParams, uint64(paramsLen))
	if err != nil {
		return paramGas, err
	}
	returnGas, err := mul64(gasPriceForReturns, uint64(returnsLen))
	if err != nil {
		return returnGas, err
	}

	finalGas, err := add64(defaultGas, syscallGas)
	if err != nil {
		return finalGas, err
	}
	finalGas, err = add64(finalGas, paramGas)
	if err != nil {
		return finalGas, err
	}

	return add64(finalGas, returnGas)
}

// GetDefaultGas get `default_gas` value from config
func GetDefaultGas(txSimContext protocol.TxSimContext) (uint64, error) {
	lastChainConfig := txSimContext.GetLastChainConfig()
	if lastChainConfig == nil {
		return 0, fmt.Errorf("can't get LastChainConfig")
	}
	if lastChainConfig.AccountConfig == nil {
		return 0, fmt.Errorf("lastChainConfig has not AccountConfig section")
	}

	if !lastChainConfig.AccountConfig.EnableGas {
		return 0, nil
	}

	return lastChainConfig.AccountConfig.DefaultGas, nil
}

// ErrOverflow was return when uint64 overflow
var ErrOverflow = errors.New("uint64 overflow")

func mul64(x uint64, y uint64) (uint64, error) {
	z := x * y
	if x == 0 || y == 0 {
		return z, nil
	}
	if z/x == y {
		return z, nil
	}

	return z, ErrOverflow
}

func add64(x uint64, y uint64) (uint64, error) {
	z := x + y
	if z-x == y {
		return z, nil
	}

	return z, ErrOverflow
}
