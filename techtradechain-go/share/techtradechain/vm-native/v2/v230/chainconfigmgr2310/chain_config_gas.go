/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package chainconfigmgr2310

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

	invokeBaseGasBytes, ok := params[paramNameSetInvokeBaseGas]
	if !ok {
		err = fmt.Errorf("set default gas failed, require param [%s], but not found", paramNameSetInvokeBaseGas)
		r.log.Error(err)
		return nil, err
	}

	invokeBaseGas, err = strconv.ParseUint(string(invokeBaseGasBytes), 10, 0)
	if err != nil {
		err = fmt.Errorf("set default gas failed, failed to parse default gas from param[%s]", paramNameSetInvokeBaseGas)
		r.log.Error(err)
		return nil, err
	}

	if chainConfig.AccountConfig != nil {
		chainConfig.AccountConfig.DefaultGas = invokeBaseGas
	} else {
		chainConfig.AccountConfig = &configPb.GasAccountConfig{
			DefaultGas: invokeBaseGas,
		}
	}
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
