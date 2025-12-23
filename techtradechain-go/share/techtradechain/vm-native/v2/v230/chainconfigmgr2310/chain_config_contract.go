/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package chainconfigmgr2310

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"techtradechain.com/techtradechain/chainconf/v2"
	"techtradechain.com/techtradechain/common/v2/msgbus"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
	"github.com/gogo/protobuf/proto"
)

const (
	paramNameOrgId      = "org_id"
	paramNameRoot       = "root"
	paramNameNodeIds    = "node_ids"
	paramNameNodeId     = "node_id"
	paramNameNewNodeId  = "new_node_id"
	paramNameMemberInfo = "member_info"
	paramNameRole       = "role"

	paramNameTxSchedulerTimeout         = "tx_scheduler_timeout"
	paramNameTxSchedulerValidateTimeout = "tx_scheduler_validate_timeout"
	paramNameEnableSenderGroup          = "enable_sender_group"
	paramNameEnableConflictsBitWindow   = "enable_conflicts_bit_window"
	paramNameEnableOptimizeChargeGas    = "enable_optimize_charge_gas"

	paramNameTxTimestampVerify    = "tx_timestamp_verify"
	paramNameTxTimeout            = "tx_timeout"
	paramNameChainBlockTxCapacity = "block_tx_capacity"
	paramNameChainBlockSize       = "block_size"
	paramNameChainBlockInterval   = "block_interval"
	paramNameChainTxParameterSize = "tx_parameter_size"
	paramNameChainBlockHeight     = "block_height"
	paramNameAddrType             = "addr_type"
	paramNameSetInvokeBaseGas     = "set_invoke_base_gas"
	paramNameBlockVersion         = "block_version"

	emptyString                      = ""
	addressIllegal                   = "account address is illegal"
	addressKey                       = "address_key"
	defaultConfigMaxValidateTimeout  = 60
	defaultConfigMaxSchedulerTimeout = 60
	zxPrefix                         = "ZX"

	paramNameMultiSignEnableManualRun = "multi_sign_enable_manual_run"
)

var (
	chainConfigContractName = syscontract.SystemContract_CHAIN_CONFIG.String()
	keyChainConfig          = chainConfigContractName
)

// ChainConfigContract 链配置管理合约对象
type ChainConfigContract struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewChainConfigContract 构造ChainConfigContract
// @param log
// @return *ChainConfigContract
func NewChainConfigContract(log protocol.Logger) *ChainConfigContract {
	return &ChainConfigContract{
		log:     log,
		methods: registerChainConfigContractMethods(log),
	}
}

// GetMethod get register method by name
func (c *ChainConfigContract) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

func registerChainConfigContractMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	// [core]
	coreRuntime := &ChainCoreRuntime{log: log}

	methodMap[syscontract.ChainConfigFunction_CORE_UPDATE.String()] = wrapEventResult(coreRuntime.CoreUpdate)

	// [block]
	blockRuntime := &ChainBlockRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_BLOCK_UPDATE.String()] = wrapEventResult(blockRuntime.BlockUpdate)

	// [trust_root]
	trustRootsRuntime := &ChainTrustRootsRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_TRUST_ROOT_ADD.String()] = wrapEventResult(
		trustRootsRuntime.TrustRootAdd)
	methodMap[syscontract.ChainConfigFunction_TRUST_ROOT_UPDATE.String()] = wrapEventResult(
		trustRootsRuntime.TrustRootUpdate)
	methodMap[syscontract.ChainConfigFunction_TRUST_ROOT_DELETE.String()] = wrapEventResult(
		trustRootsRuntime.TrustRootDelete)

	// [trust_Member]
	trustMembersRuntime := &ChainTrustMembersRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_TRUST_MEMBER_ADD.String()] = wrapEventResult(
		trustMembersRuntime.TrustMemberAdd)
	methodMap[syscontract.ChainConfigFunction_TRUST_MEMBER_DELETE.String()] = wrapEventResult(
		trustMembersRuntime.TrustMemberDelete)

	// [consensus]
	consensusRuntime := &ChainConsensusRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_NODE_ID_ADD.String()] = wrapEventResult(
		consensusRuntime.NodeIdAdd)
	methodMap[syscontract.ChainConfigFunction_NODE_ID_UPDATE.String()] = wrapEventResult(
		consensusRuntime.NodeIdUpdate)
	methodMap[syscontract.ChainConfigFunction_NODE_ID_DELETE.String()] = wrapEventResult(
		consensusRuntime.NodeIdDelete)
	methodMap[syscontract.ChainConfigFunction_NODE_ORG_ADD.String()] = wrapEventResult(
		consensusRuntime.NodeOrgAdd)
	methodMap[syscontract.ChainConfigFunction_NODE_ORG_UPDATE.String()] = wrapEventResult(
		consensusRuntime.NodeOrgUpdate)
	methodMap[syscontract.ChainConfigFunction_NODE_ORG_DELETE.String()] = wrapEventResult(
		consensusRuntime.NodeOrgDelete)
	methodMap[syscontract.ChainConfigFunction_CONSENSUS_EXT_ADD.String()] = wrapEventResult(
		consensusRuntime.ConsensusExtAdd)
	methodMap[syscontract.ChainConfigFunction_CONSENSUS_EXT_UPDATE.String()] = wrapEventResult(
		consensusRuntime.ConsensusExtUpdate)
	methodMap[syscontract.ChainConfigFunction_CONSENSUS_EXT_DELETE.String()] = wrapEventResult(
		consensusRuntime.ConsensusExtDelete)

	// [permission]
	pRuntime := &ChainPermissionRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_PERMISSION_ADD.String()] = wrapEventResult(
		pRuntime.ResourcePolicyAdd)
	methodMap[syscontract.ChainConfigFunction_PERMISSION_UPDATE.String()] = wrapEventResult(
		pRuntime.ResourcePolicyUpdate)
	methodMap[syscontract.ChainConfigFunction_PERMISSION_DELETE.String()] = wrapEventResult(
		pRuntime.ResourcePolicyDelete)
	methodMap[syscontract.ChainConfigFunction_PERMISSION_LIST.String()] = common.WrapResultFunc(
		pRuntime.ResourcePolicyList)

	// [chainConfig]
	ccRuntime := &ChainConfigRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_GET_CHAIN_CONFIG.String()] = common.WrapResultFunc(
		ccRuntime.GetChainConfig)
	methodMap[syscontract.ChainConfigFunction_GET_CHAIN_CONFIG_AT.String()] = common.WrapResultFunc(
		ccRuntime.GetChainConfigFromBlockHeight)
	methodMap[syscontract.ChainConfigFunction_UPDATE_VERSION.String()] = wrapEventResult(
		ccRuntime.UpdateVersion)

	// [account config]
	methodMap[syscontract.ChainConfigFunction_ENABLE_OR_DISABLE_GAS.String()] = wrapEventResult(
		ccRuntime.EnableOrDisableGas)
	methodMap[syscontract.ChainConfigFunction_SET_INVOKE_BASE_GAS.String()] = common.WrapResultFunc(
		ccRuntime.SetInvokeBaseGas)
	methodMap[syscontract.ChainConfigFunction_SET_ACCOUNT_MANAGER_ADMIN.String()] = common.WrapResultFunc(
		ccRuntime.SetAccountManagerAdmin)

	methodMap[syscontract.ChainConfigFunction_ALTER_ADDR_TYPE.String()] = wrapEventResult(
		ccRuntime.AlterAddrType)

	// [Vm]
	vmRuntime := &VmRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_MULTI_SIGN_ENABLE_MANUAL_RUN.String()] = wrapEventResult(
		vmRuntime.EnableOrDisableMultiSignManualRun)

	//// [archive]
	//archiveStoreRuntime := &ArchiveStoreRuntime{log: log}
	//methodMap[commonPb.ArchiveStoreContractFunction_ARCHIVE_BLOCK.String()] = wrapEventResult(
	//archiveStoreRuntime.ArchiveBlock
	//methodMap[commonPb.ArchiveStoreContractFunction_RESTORE_BLOCKS.String()] = wrapEventResult(
	//archiveStoreRuntime.RestoreBlock

	return methodMap
}

// SetChainConfig 写入新的链配置
// @param txSimContext
// @param chainConfig
// @return []byte
// @return error
func SetChainConfig(txSimContext protocol.TxSimContext, chainConfig *configPb.ChainConfig) ([]byte, error) {
	_, err := chainconf.VerifyChainConfig(chainConfig)
	if err != nil {
		return nil, err
	}

	chainConfig.Sequence = chainConfig.Sequence + 1
	pbccPayload, err := proto.Marshal(chainConfig)
	if err != nil {
		return nil, fmt.Errorf("proto marshal pbcc failed, err: %s", err.Error())
	}
	// 如果不存在对应的
	err = txSimContext.Put(chainConfigContractName, []byte(keyChainConfig), pbccPayload)
	if err != nil {
		return nil, fmt.Errorf("txSimContext put failed, err: %s", err.Error())
	}

	return pbccPayload, nil
}

// ChainConfigRuntime get chain config info
type ChainConfigRuntime struct {
	log protocol.Logger
}

// GetChainConfig get newest chain config
func (r *ChainConfigRuntime) GetChainConfig(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}
	bytes, err := proto.Marshal(chainConfig)
	if err != nil {
		return nil, fmt.Errorf("proto marshal chain config failed, err: %s", err.Error())
	}
	return bytes, nil
}

// GetChainConfigFromBlockHeight get chain config from less than or equal to block height
func (r *ChainConfigRuntime) GetChainConfigFromBlockHeight(txSimContext protocol.TxSimContext,
	params map[string][]byte) (result []byte, err error) {
	if params == nil {
		r.log.Error(common.ErrParamsEmpty)
		return nil, common.ErrParamsEmpty
	}
	blockHeightStr := params[paramNameChainBlockHeight]
	blockHeight, err := strconv.ParseUint(string(blockHeightStr), 10, 0)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	blockchainStore := txSimContext.GetBlockchainStore()
	header, err := blockchainStore.GetBlockHeaderByHeight(blockHeight)
	if err != nil {
		r.log.Errorf("get block header(height: %d) from store failed, %s", blockHeight, err)
		return nil, err
	}
	configHeight := blockHeight
	if header.BlockType != commonPb.BlockType_CONFIG_BLOCK {
		configHeight = header.PreConfHeight
	}
	configBlock, err := blockchainStore.GetBlock(configHeight)
	if err != nil {
		r.log.Errorf("get block(height: %d) from store failed, %s", configHeight, err)
		return nil, err
	}

	chainConfig, err := r.getConfigFromBlock(configBlock)
	if err != nil {
		r.log.Error("get chain config from block height failed, err: %s", err)
		return nil, err
	}
	bytes, err := proto.Marshal(chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return bytes, nil
}
func (c *ChainConfigRuntime) getConfigFromBlock(block *commonPb.Block) (*configPb.ChainConfig, error) {
	txConfig := block.Txs[0]
	if txConfig.Result == nil || txConfig.Result.ContractResult == nil || txConfig.Result.ContractResult.Result == nil {
		c.log.Errorf("tx(id: %s) is not config tx", txConfig.Payload.TxId)
		return nil, errors.New("tx is not config tx")
	}
	result := txConfig.Result.ContractResult.Result
	chainConfig := &configPb.ChainConfig{}
	err := proto.Unmarshal(result, chainConfig)
	if err != nil {
		return nil, err
	}

	return chainConfig, nil
}

// ChainCoreRuntime chain core config update
type ChainCoreRuntime struct {
	log protocol.Logger
}

// CoreUpdate update core for chain
// @param tx_scheduler_timeout   Max scheduling time of a block, in second. [0, 60]
// @param tx_scheduler_validate_timeout   Max validating time of a block, in second. [0, 60]
// @param enable_sender_group Used for handling txs with sender conflicts efficiently
// @param enable_conflicts_bit_window Used for dynamic tuning the capacity of tx execution goroutine pool
// @param enable_optimize_charge_gas enable gas
func (r *ChainCoreRuntime) CoreUpdate(txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte, error) {
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	changed := false
	if chainConfig.Core == nil {
		chainConfig.Core = &configPb.CoreConfig{}
	}

	// [0, 60] tx_scheduler_timeout
	if txSchedulerTimeout, ok := params[paramNameTxSchedulerTimeout]; ok {
		parseUint, err1 := strconv.ParseUint(string(txSchedulerTimeout), 10, 0)
		if err1 != nil {
			r.log.Error(err1)
			return nil, err
		}
		if parseUint > defaultConfigMaxValidateTimeout {
			r.log.Error(common.ErrOutOfRange)
			return nil, common.ErrOutOfRange
		}
		chainConfig.Core.TxSchedulerTimeout = parseUint
		changed = true
	}
	// [0, 60] tx_scheduler_validate_timeout
	if txSchedulerValidateTimeout, ok := params[paramNameTxSchedulerValidateTimeout]; ok {
		parseUint, err1 := strconv.ParseUint(string(txSchedulerValidateTimeout), 10, 0)
		if err1 != nil {
			r.log.Error(err1)
			return nil, err
		}
		if parseUint > defaultConfigMaxSchedulerTimeout {
			r.log.Error(common.ErrOutOfRange)
			return nil, common.ErrOutOfRange
		}
		chainConfig.Core.TxSchedulerValidateTimeout = parseUint
		changed = true
	}

	if enableSenderGroup, ok := params[paramNameEnableSenderGroup]; ok {
		parseBool, _ := strconv.ParseBool(string(enableSenderGroup))
		chainConfig.Core.EnableSenderGroup = parseBool
		changed = true
	}

	if enableConflictsBitWindow, ok := params[paramNameEnableConflictsBitWindow]; ok {
		parseBool, _ := strconv.ParseBool(string(enableConflictsBitWindow))
		chainConfig.Core.EnableConflictsBitWindow = parseBool
		changed = true
	}

	if enableOptimizeChargeGas, ok := params[paramNameEnableOptimizeChargeGas]; ok {
		if chainConfig.AccountConfig == nil || !chainConfig.AccountConfig.EnableGas {
			return nil, errors.New("can not set `enable_optimize_charge_gas` flag when `enable_gas` is disabled")
		}
		parseBool, _ := strconv.ParseBool(string(enableOptimizeChargeGas))
		chainConfig.Core.EnableOptimizeChargeGas = parseBool
		changed = true
	}

	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err := SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("core update update fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("core update success, params %+v", params)
	}
	return result, err
}

// ChainBlockRuntime chain block config update
type ChainBlockRuntime struct {
	log protocol.Logger
}

// BlockUpdate update block config for chain
// @param tx_timestamp_verify To enable this attribute, ensure that the clock of the node is consistent
//                            Verify the transaction timestamp or not
// @param tx_timeout Transaction timeout, in second.
// @param block_tx_capacity Max transaction count in a block.
// @param block_size Max block size, in MB, Ineffectual
// @param block_interval The interval of block proposing attempts, in millisecond
// @param tx_parameter_size maximum size of transaction's parameter, in MB
func (r *ChainBlockRuntime) BlockUpdate(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]start verify
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// tx_timestamp_verify
	changed1, err := utils.UpdateField(params, paramNameTxTimestampVerify, chainConfig.Block)
	if err != nil {
		return nil, err
	}
	// tx_timeout,(second)
	changed2, err := utils.UpdateField(params, paramNameTxTimeout, chainConfig.Block)
	if err != nil {
		return nil, err
	}
	// block_tx_capacity
	changed3, err := utils.UpdateField(params, paramNameChainBlockTxCapacity, chainConfig.Block)
	if err != nil {
		return nil, err
	}
	// block_size,(MB)
	changed4, err := utils.UpdateField(params, paramNameChainBlockSize, chainConfig.Block)
	if err != nil {
		return nil, err
	}
	// block_interval,(ms)
	changed5, err := utils.UpdateField(params, paramNameChainBlockInterval, chainConfig.Block)
	if err != nil {
		return nil, err
	}
	// tx_parameter_size,(MB)
	changed6, err := utils.UpdateField(params, paramNameChainTxParameterSize, chainConfig.Block)
	if err != nil {
		return nil, err
	}

	if !(changed1 || changed2 || changed3 || changed4 || changed5 || changed6) {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("block update fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("block update success, param ", params)
	}
	return result, err
}

func useCert(authType string) bool {
	authType = strings.ToLower(authType)
	return authType == protocol.PermissionedWithCert || authType == protocol.Identity || authType == ""
}

func usePK(authType string) bool {
	authType = strings.ToLower(authType)
	return authType == protocol.Public
}

func wrapEventResult(f func(txSimContext protocol.TxSimContext, parameters map[string][]byte) ([]byte, error)) func(
	txSimContext protocol.TxSimContext, parameters map[string][]byte) *commonPb.ContractResult {
	r := func(txSimContext protocol.TxSimContext, parameters map[string][]byte) *commonPb.ContractResult {
		configBytes, err := f(txSimContext, parameters)
		contractResult := common.ResultBytesAndError(configBytes, nil, err)
		if contractResult.Code == 0 {
			config := &configPb.ChainConfig{}
			_ = config.Unmarshal(configBytes)
			// 此处新增： 构建event供提交区块是 notify msgbug发送消息使用
			event := &commonPb.ContractEvent{
				Topic:           strconv.Itoa(int(msgbus.ChainConfig)),
				TxId:            txSimContext.GetTx().Payload.TxId,
				ContractName:    chainConfigContractName,
				ContractVersion: config.Version,
				EventData:       []string{hex.EncodeToString(configBytes)},
			}
			contractResult.ContractEvent = []*commonPb.ContractEvent{event}
		}
		return contractResult
	}
	return r
}

// UpdateVersion 更新ChainConfig.Version字段
// @param txSimContext
// @param params key：block_version
// @return result
// @return err
func (r *ChainConfigRuntime) UpdateVersion(txSimContext protocol.TxSimContext,
	params map[string][]byte) (result []byte, err error) {
	var chainConfig *configPb.ChainConfig
	var version uint64

	chainConfig, err = common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}

	versionBytes, ok := params[paramNameBlockVersion]
	if !ok {
		err = fmt.Errorf("set version failed, require param [%s], but not found", paramNameBlockVersion)
		r.log.Error(err)
		return nil, err
	}

	version, err = strconv.ParseUint(string(versionBytes), 10, 32)
	if err != nil {
		err = fmt.Errorf("set version failed, require int format from param[%s]", paramNameBlockVersion)
		r.log.Error(err)
		return nil, err
	}
	//检查设置的版本不能超过当前支持的版本
	if uint32(version) > protocol.DefaultBlockVersion {
		err = fmt.Errorf("set version failed, param[%s] must less or equal %d",
			paramNameBlockVersion, protocol.DefaultBlockVersion)
		r.log.Error(err)
		return nil, err
	}

	oldestVersion := uint64(2301)
	if version < oldestVersion {
		err = fmt.Errorf("set version failed, param[%s] must greater or equal %d",
			paramNameBlockVersion, oldestVersion)
		r.log.Error(err)
		return nil, err
	}

	chainConfig.Version = fmt.Sprintf("%d", version)
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return result, nil
}
