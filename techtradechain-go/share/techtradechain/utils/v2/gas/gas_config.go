/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package gas

import (
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
)

// GasConfig `gas config` born from GasAccountConfig
type GasConfig struct {
	baseGasForInstall  uint64
	baseGasForInvoke   uint64
	gasPriceForInstall float32
	gasPriceForInvoke  float32
}

// NewGasConfig create GasConfig Object
func NewGasConfig(config *configPb.GasAccountConfig) *GasConfig {
	if config == nil {
		return nil
	}

	return &GasConfig{
		baseGasForInstall:  config.InstallBaseGas,
		baseGasForInvoke:   config.DefaultGas,
		gasPriceForInstall: config.InstallGasPrice,
		gasPriceForInvoke:  config.DefaultGasPrice,
	}
}

// GetBaseGasForInstall get gas number for install user contract
func (c *GasConfig) GetBaseGasForInstall() uint64 {
	return c.baseGasForInstall
}

// GetBaseGasForInvoke get gas number for invoke user contract
func (c *GasConfig) GetBaseGasForInvoke() uint64 {
	return c.baseGasForInvoke
}

// GetGasPriceForInstall get gas price for install user contract
func (c *GasConfig) GetGasPriceForInstall() float32 {
	return c.gasPriceForInstall
}

// GetGasPriceForInvoke get gas price for invoke user contract
func (c *GasConfig) GetGasPriceForInvoke() float32 {
	return c.gasPriceForInvoke
}
