/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import (
	"fmt"

	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"github.com/gogo/protobuf/proto"
)

var (
	chainConfigContractName = syscontract.SystemContract_CHAIN_CONFIG.String()
	keyChainConfig          = chainConfigContractName
)

// getChainConfigEmptyParams 无参数，获得当前最新链配置
// @param txSimContext
// @return *configPb.ChainConfig
// @return error
func getChainConfigEmptyParams(fGet func(contractName string, key []byte) ([]byte, error)) (
	*configPb.ChainConfig, error) {
	bytes, err := fGet(chainConfigContractName, []byte(keyChainConfig))
	if err != nil {
		msg := fmt.Errorf("txSimContext get failed, name[%s] key[%s] err: %+v",
			chainConfigContractName, keyChainConfig, err)
		return nil, msg
	}

	var chainConfig configPb.ChainConfig
	err = proto.Unmarshal(bytes, &chainConfig)
	if err != nil {
		msg := fmt.Errorf("unmarshal chainConfig failed, contractName %s err: %+v",
			chainConfigContractName, err)
		return nil, msg
	}
	return &chainConfig, nil
}

// GetChainConfig 获得当前的链配置,记入读集
// @param txSimContext
// @return *configPb.ChainConfig
// @return error
func GetChainConfig(txSimContext protocol.TxSimContext) (*configPb.ChainConfig, error) {
	return getChainConfigEmptyParams(txSimContext.Get)
}

// GetChainConfigNoRecord 获得当前的链配置,不记入读集
// @param txSimContext
// @return *configPb.ChainConfig
// @return error
func GetChainConfigNoRecord(txSimContext protocol.TxSimContext) (*configPb.ChainConfig, error) {
	return getChainConfigEmptyParams(txSimContext.GetNoRecord)
}
