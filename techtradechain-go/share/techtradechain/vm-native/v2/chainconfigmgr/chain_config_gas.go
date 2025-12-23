/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package chainconfigmgr

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
)

// EnableOrDisableGas enable or able gas
func (r *ChainConfigRuntime) EnableOrDisableGas(txSimContext protocol.TxSimContext,
	params map[string][]byte) (result []byte, err error) {

	var chainConfig *configPb.ChainConfig
	chainConfig, err = common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}

	if chainConfig.AccountConfig != nil {
		chainConfig.AccountConfig.EnableGas = !chainConfig.AccountConfig.EnableGas
	} else {
		chainConfig.AccountConfig = &configPb.GasAccountConfig{
			EnableGas: true,
		}
	}
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return result, nil
}

// SetInvokeBaseGas set default gas for invoke
// @param set_invoke_base_gas
// @return *ChainConfigContract
func (r *ChainConfigRuntime) SetInvokeBaseGas(txSimContext protocol.TxSimContext,
	params map[string][]byte) (result []byte, err error) {
	var chainConfig *configPb.ChainConfig
	var invokeBaseGas uint64

	chainConfig, err = common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}

	gasAccountConfig := chainConfig.AccountConfig
	if gasAccountConfig == nil || !gasAccountConfig.EnableGas {
		return nil, fmt.Errorf("`enable_gas` flag is not opened")
	}

	invokeBaseGasBytes, ok := params[paramNameSetInvokeBaseGas]
	if !ok {
		err = fmt.Errorf("set invoke base gas failed, require param [%s], but not found", paramNameSetInvokeBaseGas)
		r.log.Error(err)
		return nil, err
	}

	invokeBaseGas, err = strconv.ParseUint(string(invokeBaseGasBytes), 10, 0)
	if err != nil {
		err = fmt.Errorf("set invoke base gas failed, failed to parse default gas from param[%s]", paramNameSetInvokeBaseGas)
		r.log.Error(err)
		return nil, err
	}

	chainConfig.AccountConfig.DefaultGas = invokeBaseGas
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return result, nil
}

// SetInvokeGasPrice set default gas for invoke
// @param set_invoke_gas_price
// @return *ChainConfigContract
func (r *ChainConfigRuntime) SetInvokeGasPrice(txSimContext protocol.TxSimContext,
	params map[string][]byte) (result []byte, err error) {
	var chainConfig *configPb.ChainConfig
	var invokeGasPrice float64

	chainConfig, err = common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}

	gasAccountConfig := chainConfig.AccountConfig
	if gasAccountConfig == nil || !gasAccountConfig.EnableGas {
		return nil, fmt.Errorf("`enable_gas` flag is not opened")
	}

	invokeGasPriceBytes, ok := params[paramNameSetInvokeGasPrice]
	if !ok {
		err = fmt.Errorf("set invoke gas price failed, require param [%s], but not found",
			paramNameSetInvokeGasPrice)
		r.log.Error(err)
		return nil, err
	}

	invokeGasPrice, err = strconv.ParseFloat(string(invokeGasPriceBytes), 32)
	if err != nil {
		err = fmt.Errorf("set invoke gas price failed, failed to parse default gas from param[%s]",
			paramNameSetInvokeGasPrice)
		r.log.Error(err)
		return nil, err
	}
	if invokeGasPrice < 0 {
		return nil, errors.New("gas price should not be less than 0")
	}

	chainConfig.AccountConfig.DefaultGasPrice = float32(invokeGasPrice)
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return result, nil
}

// SetInstallBaseGas set default gas for install contract
// @param set_install_base_gas
// @return *ChainConfigContract
func (r *ChainConfigRuntime) SetInstallBaseGas(txSimContext protocol.TxSimContext,
	params map[string][]byte) (result []byte, err error) {
	var chainConfig *configPb.ChainConfig
	var installBaseGas uint64

	chainConfig, err = common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}

	gasAccountConfig := chainConfig.AccountConfig
	if gasAccountConfig == nil || !gasAccountConfig.EnableGas {
		return nil, fmt.Errorf("`enable_gas` flag is not opened")
	}

	installBaseGasBytes, ok := params[paramNameSetInstallBaseGas]
	if !ok {
		err = fmt.Errorf("set install base gas failed, require param [%s], but not found",
			paramNameSetInstallBaseGas)
		r.log.Error(err)
		return nil, err
	}

	installBaseGas, err = strconv.ParseUint(string(installBaseGasBytes), 10, 0)
	if err != nil {
		err = fmt.Errorf("set install base gas failed, failed to parse default gas from param[%s]",
			paramNameSetInstallBaseGas)
		r.log.Error(err)
		return nil, err
	}

	chainConfig.AccountConfig.InstallBaseGas = installBaseGas
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return result, nil
}

// SetInstallGasPrice set default gas for install contract
// @param set_install_gas_price
// @return *ChainConfigContract
func (r *ChainConfigRuntime) SetInstallGasPrice(txSimContext protocol.TxSimContext,
	params map[string][]byte) (result []byte, err error) {
	var chainConfig *configPb.ChainConfig
	var installGasPrice float64

	chainConfig, err = common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}

	gasAccountConfig := chainConfig.AccountConfig
	if gasAccountConfig == nil || !gasAccountConfig.EnableGas {
		return nil, fmt.Errorf("`enable_gas` flag is not opened")
	}

	installGasPriceBytes, ok := params[paramNameSetInstallGasPrice]
	if !ok {
		err = fmt.Errorf("set install gas price failed, require param [%s], but not found",
			paramNameSetInstallGasPrice)
		r.log.Error(err)
		return nil, err
	}

	installGasPrice, err = strconv.ParseFloat(string(installGasPriceBytes), 32)
	if err != nil {
		err = fmt.Errorf("set install gas price failed, failed to parse default gas from param[%s]",
			paramNameSetInstallGasPrice)
		r.log.Error(err)
		return nil, err
	}
	if installGasPrice < 0 {
		return nil, errors.New("gas price should not be less than 0")
	}

	chainConfig.AccountConfig.InstallGasPrice = float32(installGasPrice)
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return result, nil
}

// SetAccountManagerAdmin 设置gas account的管理员
// @param txSimContext
// @param params
// @return []byte
// @return error
func (g *ChainConfigRuntime) SetAccountManagerAdmin(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	addressBytes, ok := params[addressKey]
	if !ok {
		err := fmt.Errorf(" params key %s not exist ", addressKey)
		g.log.Error(err.Error())
		return nil, err
	}

	if utils.IsAnyBlank(addressBytes) {
		err := fmt.Errorf(" %s, param[public_key]=%s,", common.ErrParams.Error(), addressBytes)
		g.log.Errorf(err.Error())
		return nil, err
	}

	address := string(addressBytes)
	if address, ok = g.verifyAddress(txSimContext, address); !ok {
		err := errors.New(addressIllegal)
		g.log.Error(err.Error())
		return nil, err
	}

	return g.setAccountManagerAdmin(txSimContext, address)
}

// verifyAddress 验证地址的合法性
// @param context
// @param address
// @return string
// @return bool
func (g *ChainConfigRuntime) verifyAddress(context protocol.TxSimContext, address string) (string, bool) {
	g.log.Infof("verify account address is:%v", address)
	//for old version
	if context.GetBlockVersion() < 2300 {
		if len(address) != 42 || address[:2] != zxPrefix {
			return emptyString, false
		}

		return address[:2] + strings.ToLower(address[2:]), true
	}
	//for new version
	chainCfg, _ := context.GetBlockchainStore().GetLastChainConfig()
	if chainCfg.Vm.AddrType == configPb.AddrType_ZXL {
		if len(address) != 42 || address[:2] != zxPrefix {
			return emptyString, false
		}

		return address[:2] + strings.ToLower(address[2:]), true
	}
	//evm address
	if !utils.CheckEvmAddressFormat(address) {
		return emptyString, false
	}
	//cm address
	return strings.ToLower(address), true

}

func (g *ChainConfigRuntime) setAccountManagerAdmin(txSimContext protocol.TxSimContext,
	address string) ([]byte, error) {
	var err error
	var chainConfig *configPb.ChainConfig
	var result []byte

	chainConfig, err = common.GetChainConfig(txSimContext)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if chainConfig.AccountConfig != nil {
		chainConfig.AccountConfig.GasAdminAddress = address
	} else {
		chainConfig.AccountConfig = &configPb.GasAccountConfig{
			GasAdminAddress: address,
		}
	}

	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}
	return result, nil
}

// AlterAddrType upgrade the type of address
func (r *ChainConfigRuntime) AlterAddrType(txSimContext protocol.TxSimContext, params map[string][]byte) (result []byte,
	err error) {
	var chainConfig *configPb.ChainConfig
	chainConfig, err = common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}

	addrTypeBytes, ok := params[paramNameAddrType]
	if !ok {
		err = fmt.Errorf("alter address type failed, require param [%s], but not found", paramNameAddrType)
		r.log.Error(err)
		return nil, err
	}

	addrType, err := strconv.ParseUint(string(addrTypeBytes), 10, 0)
	if err != nil {
		err = fmt.Errorf("alter address type failed, failed to parse address type from param[%s]", paramNameAddrType)
		r.log.Error(err)
		return nil, err
	}

	at := configPb.AddrType(int32(addrType))
	if txSimContext.GetBlockVersion() < 2210 {
		if at == chainConfig.Vm.AddrType {
			return []byte{}, nil
		}
	}

	chainConfig.Vm.AddrType = at
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return result, nil
}
