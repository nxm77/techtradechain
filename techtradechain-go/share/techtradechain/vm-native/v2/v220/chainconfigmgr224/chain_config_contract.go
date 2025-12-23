/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package chainconfigmgr224

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"techtradechain.com/techtradechain/chainconf/v2"
	"techtradechain.com/techtradechain/common/v2/sortedmap"
	acPb "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/consensus"
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

	defaultConfigMaxValidateTimeout  = 60
	defaultConfigMaxSchedulerTimeout = 60
)

var (
	chainConfigContractName = syscontract.SystemContract_CHAIN_CONFIG.String()
	keyChainConfig          = chainConfigContractName
)

// ChainConfigContract comment at next version
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

// GetMethod comment at next version
func (c *ChainConfigContract) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

func registerChainConfigContractMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	// [core]
	coreRuntime := &ChainCoreRuntime{log: log}

	methodMap[syscontract.ChainConfigFunction_CORE_UPDATE.String()] = common.WrapResultFunc(coreRuntime.CoreUpdate)

	// [block]
	blockRuntime := &ChainBlockRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_BLOCK_UPDATE.String()] = common.WrapResultFunc(blockRuntime.BlockUpdate)

	// [trust_root]
	trustRootsRuntime := &ChainTrustRootsRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_TRUST_ROOT_ADD.String()] = common.WrapResultFunc(
		trustRootsRuntime.TrustRootAdd)
	methodMap[syscontract.ChainConfigFunction_TRUST_ROOT_UPDATE.String()] = common.WrapResultFunc(
		trustRootsRuntime.TrustRootUpdate)
	methodMap[syscontract.ChainConfigFunction_TRUST_ROOT_DELETE.String()] = common.WrapResultFunc(
		trustRootsRuntime.TrustRootDelete)

	// [trust_Member]
	trustMembersRuntime := &ChainTrustMembersRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_TRUST_MEMBER_ADD.String()] = common.WrapResultFunc(
		trustMembersRuntime.TrustMemberAdd)
	methodMap[syscontract.ChainConfigFunction_TRUST_MEMBER_DELETE.String()] = common.WrapResultFunc(
		trustMembersRuntime.TrustMemberDelete)

	// [consensus]
	consensusRuntime := &ChainConsensusRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_NODE_ID_ADD.String()] = common.WrapResultFunc(
		consensusRuntime.NodeIdAdd)
	methodMap[syscontract.ChainConfigFunction_NODE_ID_UPDATE.String()] = common.WrapResultFunc(
		consensusRuntime.NodeIdUpdate)
	methodMap[syscontract.ChainConfigFunction_NODE_ID_DELETE.String()] = common.WrapResultFunc(
		consensusRuntime.NodeIdDelete)
	methodMap[syscontract.ChainConfigFunction_NODE_ORG_ADD.String()] = common.WrapResultFunc(
		consensusRuntime.NodeOrgAdd)
	methodMap[syscontract.ChainConfigFunction_NODE_ORG_UPDATE.String()] = common.WrapResultFunc(
		consensusRuntime.NodeOrgUpdate)
	methodMap[syscontract.ChainConfigFunction_NODE_ORG_DELETE.String()] = common.WrapResultFunc(
		consensusRuntime.NodeOrgDelete)
	methodMap[syscontract.ChainConfigFunction_CONSENSUS_EXT_ADD.String()] = common.WrapResultFunc(
		consensusRuntime.ConsensusExtAdd)
	methodMap[syscontract.ChainConfigFunction_CONSENSUS_EXT_UPDATE.String()] = common.WrapResultFunc(
		consensusRuntime.ConsensusExtUpdate)
	methodMap[syscontract.ChainConfigFunction_CONSENSUS_EXT_DELETE.String()] = common.WrapResultFunc(
		consensusRuntime.ConsensusExtDelete)

	// [permission]
	methodMap[syscontract.ChainConfigFunction_PERMISSION_ADD.String()] = common.WrapResultFunc(
		consensusRuntime.ResourcePolicyAdd)
	methodMap[syscontract.ChainConfigFunction_PERMISSION_UPDATE.String()] = common.WrapResultFunc(
		consensusRuntime.ResourcePolicyUpdate)
	methodMap[syscontract.ChainConfigFunction_PERMISSION_DELETE.String()] = common.WrapResultFunc(
		consensusRuntime.ResourcePolicyDelete)

	// [chainConfig]
	ccRuntime := &ChainConfigRuntime{log: log}
	methodMap[syscontract.ChainConfigFunction_GET_CHAIN_CONFIG.String()] = common.WrapResultFunc(
		ccRuntime.GetChainConfig)
	methodMap[syscontract.ChainConfigFunction_GET_CHAIN_CONFIG_AT.String()] = common.WrapResultFunc(
		ccRuntime.GetChainConfigFromBlockHeight)
	methodMap[syscontract.ChainConfigFunction_UPDATE_VERSION.String()] = common.WrapResultFunc(
		ccRuntime.UpdateVersion)

	// [account config]
	methodMap[syscontract.ChainConfigFunction_ENABLE_OR_DISABLE_GAS.String()] = common.WrapResultFunc(
		ccRuntime.EnableOrDisableGas)
	methodMap[syscontract.ChainConfigFunction_SET_INVOKE_BASE_GAS.String()] = common.WrapResultFunc(
		ccRuntime.SetInvokeBaseGas)

	methodMap[syscontract.ChainConfigFunction_ALTER_ADDR_TYPE.String()] = common.WrapResultFunc(
		ccRuntime.AlterAddrType)

	//// [archive]
	//archiveStoreRuntime := &ArchiveStoreRuntime{log: log}
	//methodMap[commonPb.ArchiveStoreContractFunction_ARCHIVE_BLOCK.String()] = common.WrapResultFunc(
	//archiveStoreRuntime.ArchiveBlock
	//methodMap[commonPb.ArchiveStoreContractFunction_RESTORE_BLOCKS.String()] = common.WrapResultFunc(
	//archiveStoreRuntime.RestoreBlock

	return methodMap
}

// GetChainConfig comment at next version
func GetChainConfig(txSimContext protocol.TxSimContext) (*configPb.ChainConfig, error) {
	return getChainConfig(txSimContext, make(map[string][]byte))
}
func getChainConfig(txSimContext protocol.TxSimContext, params map[string][]byte) (*configPb.ChainConfig, error) {
	if params == nil {
		return nil, common.ErrParamsEmpty
	}
	return GetChainConfigEmptyParams(txSimContext)
}

// GetChainConfigEmptyParams comment at next version
func GetChainConfigEmptyParams(txSimContext protocol.TxSimContext) (*configPb.ChainConfig, error) {
	bytes, err := txSimContext.Get(chainConfigContractName, []byte(keyChainConfig))
	if err != nil {
		msg := fmt.Errorf("txSimContext get failed, name[%s] key[%s] err: %+v",
			chainConfigContractName, keyChainConfig, err)
		return nil, msg
	}

	var chainConfig configPb.ChainConfig
	err = proto.Unmarshal(bytes, &chainConfig)
	if err != nil {
		msg := fmt.Errorf("unmarshal chainConfig failed, contractName %s err: %+v", chainConfigContractName, err)
		return nil, msg
	}
	return &chainConfig, nil
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
	chainConfig, err := getChainConfig(txSimContext, params)
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

// AlterAddrType upgrade the type of address
func (r *ChainConfigRuntime) AlterAddrType(txSimContext protocol.TxSimContext, params map[string][]byte) (result []byte,
	err error) {
	var chainConfig *configPb.ChainConfig
	chainConfig, err = getChainConfig(txSimContext, params)
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
	chainConfig.Vm.AddrType = at
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return result, nil
}

// EnableOrDisableGas enable or able gas
func (r *ChainConfigRuntime) EnableOrDisableGas(txSimContext protocol.TxSimContext,
	params map[string][]byte) (result []byte, err error) {

	var chainConfig *configPb.ChainConfig
	chainConfig, err = GetChainConfigEmptyParams(txSimContext)
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
func (r *ChainConfigRuntime) SetInvokeBaseGas(txSimContext protocol.TxSimContext,
	params map[string][]byte) (result []byte, err error) {
	var chainConfig *configPb.ChainConfig
	var invokeBaseGas uint64

	chainConfig, err = GetChainConfigEmptyParams(txSimContext)
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
	chainConfig, err := getChainConfig(txSimContext, params)
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
		if !chainConfig.AccountConfig.EnableGas {
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
	chainConfig, err := getChainConfig(txSimContext, params)
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

// ChainTrustRootsRuntime chain trust root manager
type ChainTrustRootsRuntime struct {
	log protocol.Logger
}

// TrustRootAdd add trustRoot
// @param org_id
// @param root
// @return chainConfig
func (r *ChainTrustRootsRuntime) TrustRootAdd(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	orgId := string(params[paramNameOrgId])
	rootCasStr := string(params[paramNameRoot])
	if utils.IsAnyBlank(orgId, rootCasStr) {
		err = fmt.Errorf("%s, add trust root cert require param [%s, %s] not found",
			common.ErrParams.Error(), paramNameOrgId, paramNameRoot)
		r.log.Error(err)
		return nil, err
	}
	root := strings.Split(rootCasStr, ",")

	existOrgId := false
	for i := 0; i < len(chainConfig.TrustRoots); i++ {
		if chainConfig.TrustRoots[i].OrgId == orgId {
			existOrgId = true
			chainConfig.TrustRoots[i].Root = append(chainConfig.TrustRoots[i].Root, root...)
		}
	}

	if !existOrgId {
		trustRoot := &configPb.TrustRootConfig{OrgId: orgId, Root: root}
		chainConfig.TrustRoots = append(chainConfig.TrustRoots, trustRoot)
	}

	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("trust root add fail, %s, orgId[%s] cert[%s]", err.Error(), orgId, rootCasStr)
	} else {
		r.log.Infof("trust root add success. orgId[%s] cert[%s]", orgId, rootCasStr)
	}
	return result, err
}

// TrustRootUpdate update the trustRoot
// @param org_id
// @param root
// @return chainConfig
func (r *ChainTrustRootsRuntime) TrustRootUpdate(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	orgId := string(params[paramNameOrgId])

	rootCasStr := string(params[paramNameRoot])
	if utils.IsAnyBlank(orgId, rootCasStr) {
		err = fmt.Errorf("%s, add trust root cert require param [%s, %s]  not found",
			common.ErrParams.Error(), paramNameOrgId, paramNameRoot)
		r.log.Error(err)
		return nil, err
	}

	// verify has consensus nodes
	if useCert(chainConfig.AuthType) {
		for _, node := range chainConfig.Consensus.Nodes {
			if orgId == node.OrgId {
				err = fmt.Errorf("update trust root cert failed, you must delete all consensus nodes under the organization first")
				r.log.Error(err)
				return nil, err
			}
		}
	}

	root := strings.Split(rootCasStr, ",")

	trustRoots := chainConfig.TrustRoots
	for i, trustRoot := range trustRoots {
		if orgId == trustRoot.OrgId {
			trustRoots[i] = &configPb.TrustRootConfig{OrgId: orgId, Root: root}
			result, err = SetChainConfig(txSimContext, chainConfig)
			if err != nil {
				r.log.Errorf("trust root update fail, %s, orgId[%s] cert[%s]", err.Error(), orgId, rootCasStr)
			} else {
				r.log.Infof("trust root update success. orgId[%s] cert[%s]", orgId, rootCasStr)
			}
			return result, err
		}
	}

	err = fmt.Errorf("%s can not found orgId[%s]", common.ErrParams.Error(), orgId)
	r.log.Error(err)
	return nil, err
}

// TrustRootDelete delete the org all trust root
// must not contain the consensus node cert issued by the cert
// @param org_id
// @return chainConfig
func (r *ChainTrustRootsRuntime) TrustRootDelete(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	orgId := string(params[paramNameOrgId])
	if utils.IsAnyBlank(orgId) {
		err = fmt.Errorf("delete trust root cert failed, require param [%s], but not found", paramNameOrgId)
		r.log.Error(err)
		return nil, err
	}

	index := -1
	trustRoots := chainConfig.TrustRoots
	for i, root := range trustRoots {
		if orgId == root.OrgId {
			index = i
			break
		}
	}

	if index == -1 {
		err = fmt.Errorf("delete trust root cert failed, param [%s] not found from TrustRoot", orgId)
		r.log.Error(err)
		return nil, err
	}

	if usePK(chainConfig.AuthType) {
		rootPem := string(params[paramNameRoot])
		if utils.IsAnyBlank(rootPem) {
			err = fmt.Errorf("delete trust root failed, require param [%s], but not found", paramNameRoot)
			r.log.Error(err)
			return nil, err
		}
		roots := trustRoots[index].Root
		found := false
		for ri, rt := range roots {
			if rootPem == rt {
				trustRoots[index].Root = append(trustRoots[index].Root[:ri], trustRoots[index].Root[ri+1:]...)
				if len(trustRoots[index].Root) == 0 {
					err = fmt.Errorf("delete trust root failed, can't delete all TrustRoot")
					r.log.Error(err)
					return nil, err
				}
				found = true
				break
			}
		}
		if !found {
			err = fmt.Errorf("delete trust root failed, root not found, root: %s", rootPem)
			r.log.Error(err)
			return nil, err
		}
	} else {
		trustRoots = append(trustRoots[:index], trustRoots[index+1:]...)
	}

	// verify has consensus nodes
	if useCert(chainConfig.AuthType) {
		for _, node := range chainConfig.Consensus.Nodes {
			if orgId == node.OrgId {
				err = fmt.Errorf("update trust root cert failed, you must delete all consensus nodes under the organization first")
				r.log.Error(err)
				return nil, err
			}
		}
	}

	chainConfig.TrustRoots = trustRoots
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("trust root delete fail, %s, orgId[%s] ", err.Error(), orgId)
	} else {
		r.log.Infof("trust root delete success. orgId[%s]", orgId)
	}
	return result, err
}

// ChainTrustMembersRuntime trust member(third cert) manager
type ChainTrustMembersRuntime struct {
	log protocol.Logger
}

// TrustMemberAdd add third party certificate
// @param org_id
// @param member_info
// @param role
// @param node_id
// @return chainConfig
func (r *ChainTrustMembersRuntime) TrustMemberAdd(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	orgId := string(params[paramNameOrgId])
	memberInfo := string(params[paramNameMemberInfo])
	role := string(params[paramNameRole])
	nodeId := string(params[paramNameNodeId])
	if utils.IsAnyBlank(memberInfo, orgId, role, nodeId) {
		err = fmt.Errorf("%s, add trust member require param [%s, %s,%s,%s] not found",
			common.ErrParams.Error(), paramNameOrgId, paramNameMemberInfo, paramNameRole, paramNameNodeId)
		r.log.Error(err)
		return nil, err
	}
	for _, member := range chainConfig.TrustMembers {
		if member.MemberInfo == memberInfo {
			err = fmt.Errorf("%s, add trsut member failed, the memberinfo[%s] already exist in chainconfig",
				common.ErrParams, memberInfo)
			r.log.Error(err)
			return nil, err
		}
	}
	trustMember := &configPb.TrustMemberConfig{MemberInfo: memberInfo, OrgId: orgId, Role: role, NodeId: nodeId}
	chainConfig.TrustMembers = append(chainConfig.TrustMembers, trustMember)
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("trust member add fail, %s, orgId[%s] memberInfo[%s] role[%s] nodeId[%s]",
			err.Error(), orgId, memberInfo, role, nodeId)
	} else {
		r.log.Infof("trust member add success. orgId[%s] memberInfo[%s] role[%s] nodeId[%s]",
			orgId, memberInfo, role, nodeId)
	}
	return result, err
}

// TrustMemberDelete delete third party certificate
// @param org_id
// @param member_info
// @return chainConfig
func (r *ChainTrustMembersRuntime) TrustMemberDelete(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	memberInfo := string(params[paramNameMemberInfo])
	if utils.IsAnyBlank(memberInfo) {
		err = fmt.Errorf("delete trust member failed, require param [%s], but not found", paramNameNodeId)
		r.log.Error(err)
		return nil, err
	}

	index := -1
	trustMembers := chainConfig.TrustMembers
	for i, trustMember := range trustMembers {
		if memberInfo == trustMember.MemberInfo {
			index = i
			break
		}
	}

	if index == -1 {
		err = fmt.Errorf("delete trust member failed, param [%s] not found from TrustMembers", memberInfo)
		r.log.Error(err)
		return nil, err
	}

	trustMembers = append(trustMembers[:index], trustMembers[index+1:]...)

	chainConfig.TrustMembers = trustMembers
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("trust member delete fail, %s, nodeId[%s] ", err.Error(), memberInfo)
	} else {
		r.log.Infof("trust member delete success. nodeId[%s]", memberInfo)
	}
	return result, err
}

// ChainConsensusRuntime chain consensus config update
type ChainConsensusRuntime struct {
	log protocol.Logger
}

// NodeIdAdd add nodeId for org
// @param org_id
// @param node_ids
// @return chainConfig
func (r *ChainConsensusRuntime) NodeIdAdd(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	orgId := string(params[paramNameOrgId])
	nodeIdsStr := string(params[paramNameNodeIds]) // The addresses are separated by ","

	if utils.IsAnyBlank(orgId, nodeIdsStr) {
		err = fmt.Errorf("add node id failed, require param [%s, %s], but not found", paramNameOrgId, paramNameNodeIds)
		r.log.Error(err)
		return nil, err
	}

	nodeIdStrs := strings.Split(nodeIdsStr, ",")
	nodes := chainConfig.Consensus.Nodes

	index := -1
	var nodeConf *configPb.OrgConfig
	for i, node := range nodes {
		if orgId == node.OrgId {
			index = i
			nodeConf = node
			break
		}
	}

	if index == -1 {
		err = fmt.Errorf("add node id failed, param [%s] not found from nodes", orgId)
		r.log.Error(err)
		return nil, err
	}

	changed := false
	for _, nid := range nodeIdStrs {
		nid = strings.TrimSpace(nid)
		nodeIds := nodeConf.NodeId
		nodeIds = append(nodeIds, nid)
		nodeConf.NodeId = nodeIds
		nodes[index] = nodeConf
		chainConfig.Consensus.Nodes = nodes
		changed = true
	}

	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("node id add fail, %s, orgId[%s] nodeIdsStr[%s]", err.Error(), orgId, nodeIdsStr)
	} else {
		r.log.Infof("node id add success. orgId[%s] nodeIdsStr[%s]", orgId, nodeIdsStr)
	}
	return result, err
}

// NodeIdUpdate update nodeId
// @param org_id
// @param node_id
// @param new_node_id
// @return chainConfig
func (r *ChainConsensusRuntime) NodeIdUpdate(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	if isRaft(chainConfig) {
		err = fmt.Errorf("raft consensus does not support update node id, please use delete and add operation")
		r.log.Error(err)
		return nil, err
	}
	// verify params
	orgId := string(params[paramNameOrgId])
	nodeId := string(params[paramNameNodeId])       // origin node id
	newNodeId := string(params[paramNameNewNodeId]) // new node id

	if utils.IsAnyBlank(orgId, nodeId, newNodeId) {
		err = fmt.Errorf("update node id failed, require param [%s, %s, %s], but not found",
			paramNameOrgId, paramNameNodeId, paramNameNewNodeId)
		r.log.Error(err)
		return nil, err
	}

	nodes := chainConfig.Consensus.Nodes
	nodeId = strings.TrimSpace(nodeId)
	newNodeId = strings.TrimSpace(newNodeId)

	index := -1
	var nodeConf *configPb.OrgConfig
	for i, node := range nodes {
		if orgId == node.OrgId {
			index = i
			nodeConf = node
			break
		}
	}

	if index == -1 {
		err = fmt.Errorf("update node id failed, param orgId[%s] not found from nodes", orgId)
		r.log.Error(err)
		return nil, err
	}

	for j, nid := range nodeConf.NodeId {
		if nodeId == nid {
			nodeConf.NodeId[j] = newNodeId
			nodes[index] = nodeConf
			chainConfig.Consensus.Nodes = nodes
			result, err = SetChainConfig(txSimContext, chainConfig)
			if err != nil {
				r.log.Errorf("node id update fail, %s, orgId[%s] addr[%s] newAddr[%s]",
					err.Error(), orgId, nid, newNodeId)
			} else {
				r.log.Infof("node id update success. orgId[%s] addr[%s] newAddr[%s]", orgId, nid, newNodeId)
			}
			return result, err
		}
	}

	err = fmt.Errorf("update node id failed, param orgId[%s] addr[%s] not found from nodes", orgId, nodeId)
	r.log.Error(err)
	return nil, err
}

// NodeIdDelete delete nodeId
// raft consensus in public key mode, when delete nodes, the remaining consensus nodes counts need >= 2
// @param org_id
// @param node_id
// @return chainConfig
func (r *ChainConsensusRuntime) NodeIdDelete(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	// verify params
	orgId := string(params[paramNameOrgId])
	nodeId := string(params[paramNameNodeId])

	if utils.IsAnyBlank(orgId, nodeId) {
		err = fmt.Errorf("delete node id failed, require param [%s, %s], but not found", paramNameOrgId, paramNameNodeId)
		r.log.Error(err)
		return nil, err
	}

	err = verifyConsensusCount(chainConfig, nodeId, "")
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	nodes := chainConfig.Consensus.Nodes
	index := -1
	var nodeConf *configPb.OrgConfig
	for i, node := range nodes {
		if orgId == node.OrgId {
			index = i
			nodeConf = node
			break
		}
	}

	if index == -1 {
		err = fmt.Errorf("delete node id failed, param orgId[%s] not found from nodes", orgId)
		r.log.Error(err)
		return nil, err
	}

	nodeIds := nodeConf.NodeId
	for j, nid := range nodeIds {
		if nodeId == nid {
			nodeConf.NodeId = append(nodeIds[:j], nodeIds[j+1:]...)
			nodes[index] = nodeConf
			chainConfig.Consensus.Nodes = nodes
			result, err = SetChainConfig(txSimContext, chainConfig)
			if err != nil {
				r.log.Errorf("node id delete fail, %s, orgId[%s] addr[%s]", err.Error(), orgId, nid)
			} else {
				r.log.Infof("node id delete success. orgId[%s] addr[%s]", orgId, nid)
			}
			return result, err
		}
	}

	err = fmt.Errorf("delete node id failed, param orgId[%s] addr[%s] not found from nodes", orgId, nodeId)
	r.log.Error(err)
	return nil, err
}

// NodeOrgAdd add nodeOrg
// @param org_id
// @param node_ids
// @return chainConfig
func (r *ChainConsensusRuntime) NodeOrgAdd(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	orgId := string(params[paramNameOrgId])
	nodeIdsStr := string(params[paramNameNodeIds])

	if utils.IsAnyBlank(orgId, nodeIdsStr) {
		err = fmt.Errorf("add node org failed, require param [%s, %s], but not found", paramNameOrgId, paramNameNodeIds)
		r.log.Error(err)
		return nil, err
	}
	nodes := chainConfig.Consensus.Nodes
	for _, node := range nodes {
		if orgId == node.OrgId {
			return nil, errors.New(paramNameOrgId + " is exist")
		}
	}
	org := &configPb.OrgConfig{
		OrgId:  orgId,
		NodeId: make([]string, 0),
	}

	nodeIds := strings.Split(nodeIdsStr, ",")
	for _, nid := range nodeIds {
		nid = strings.TrimSpace(nid)
		if nid != "" {
			org.NodeId = append(org.NodeId, nid)
		}
	}
	if len(org.NodeId) > 0 {
		chainConfig.Consensus.Nodes = append(chainConfig.Consensus.Nodes, org)

		result, err = SetChainConfig(txSimContext, chainConfig)
		if err != nil {
			r.log.Errorf("node org add fail, %s, orgId[%s] nodeIdsStr[%s]", err.Error(), orgId, nodeIdsStr)
		} else {
			r.log.Infof("node org add success. orgId[%s] nodeIdsStr[%s]", orgId, nodeIdsStr)
		}
		return result, err
	}

	r.log.Error(common.ErrParams)
	return nil, common.ErrParams
}

// NodeOrgUpdate update nodeOrg
// @param org_id
// @param node_ids
// @return chainConfig
func (r *ChainConsensusRuntime) NodeOrgUpdate(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	if isRaft(chainConfig) {
		err = fmt.Errorf("raft consensus does not support update node id, please use delete and add operation")
		r.log.Error(err)
		return nil, err
	}

	// verify params
	changed := false
	orgId := string(params[paramNameOrgId])
	nodeIdsStr := string(params[paramNameNodeIds])

	if utils.IsAnyBlank(orgId, nodeIdsStr) {
		err = fmt.Errorf("update node org failed, require param [%s, %s], but not found",
			paramNameOrgId, paramNameNodeIds)
		r.log.Error(err)
		return nil, err
	}

	nodeIds := strings.Split(nodeIdsStr, ",")
	nodes := chainConfig.Consensus.Nodes
	index := -1
	var nodeConf *configPb.OrgConfig
	for i, node := range nodes {
		if orgId == node.OrgId {
			index = i
			nodeConf = node
			break
		}
	}

	if index == -1 {
		err = fmt.Errorf("update node org failed, param orgId[%s] not found from nodes", orgId)
		r.log.Error(err)
		return nil, err
	}

	nodeConf.NodeId = []string{}
	for _, nid := range nodeIds {
		nid = strings.TrimSpace(nid)
		if nid != "" {
			nodeConf.NodeId = append(nodeConf.NodeId, nid)
			nodes[index] = nodeConf
			chainConfig.Consensus.Nodes = nodes
			changed = true
		}
	}

	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}

	err = verifyConsensusCount(chainConfig, "", "")
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("node org update fail, %s, orgId[%s] nodeIdsStr[%s]", err.Error(), orgId, nodeIdsStr)
	} else {
		r.log.Infof("node org update success. orgId[%s] nodeIdsStr[%s]", orgId, nodeIdsStr)
	}
	return result, err
}

// NodeOrgDelete delete nodeOrg
// raft consensus in public key mode, when delete nodes, the remaining consensus nodes counts need >= 2
// @param org_id
// @return chainConfig
func (r *ChainConsensusRuntime) NodeOrgDelete(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	orgId := string(params[paramNameOrgId])

	if utils.IsAnyBlank(orgId) {
		err = fmt.Errorf("delete node org failed, require param [%s], but not found", paramNameOrgId)
		r.log.Error(err)
		return nil, err
	}

	err = verifyConsensusCount(chainConfig, "", orgId)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	nodes := chainConfig.Consensus.Nodes
	if len(nodes) == 1 {
		err1 := fmt.Errorf("there is at least one org")
		r.log.Error(err1)
		return nil, err1
	}
	for i, node := range nodes {
		if orgId == node.OrgId {
			nodes = append(nodes[:i], nodes[i+1:]...)
			chainConfig.Consensus.Nodes = nodes

			result, err = SetChainConfig(txSimContext, chainConfig)
			if err != nil {
				r.log.Errorf("node org delete fail, %s, orgId[%s]", err.Error(), orgId)
			} else {
				r.log.Infof("node org delete success. orgId[%s]", orgId)
			}
			return result, err
		}
	}

	err = fmt.Errorf("delete node org failed, param orgId[%s] not found from nodes", orgId)
	r.log.Error(err)
	return nil, err
}

// ConsensusExtAdd add consensus extra
func (r *ChainConsensusRuntime) ConsensusExtAdd(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	changed := false
	extConfig := chainConfig.Consensus.ExtConfig
	if extConfig == nil {
		extConfig = make([]*configPb.ConfigKeyValue, 0)
	}

	extConfigMap := make(map[string]string)
	for _, v := range extConfig {
		extConfigMap[v.Key] = string(v.Value)
	}

	// map is out of order, in order to ensure that each execution sequence is consistent, we need to sort
	sortedParams := sortedmap.NewStringKeySortedMapWithBytesData(params)
	var parseParamErr error
	sortedParams.Range(func(key string, val interface{}) (isContinue bool) {
		value, ok := val.([]byte)
		if !ok {
			r.log.Error("value not a string")
		}
		if _, ok = extConfigMap[key]; ok {
			parseParamErr = fmt.Errorf("ext_config key[%s] is exist", key)
			r.log.Error(parseParamErr.Error())
			return false
		}
		extConfig = append(extConfig, &configPb.ConfigKeyValue{
			Key:   key,
			Value: string(value),
		})
		chainConfig.Consensus.ExtConfig = extConfig
		changed = true
		return true
	})
	if parseParamErr != nil {
		return nil, parseParamErr
	}

	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("consensus ext add fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("consensus ext add success. params %+v", params)
	}
	return result, err
}

// ConsensusExtUpdate update consensus extra
func (r *ChainConsensusRuntime) ConsensusExtUpdate(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	changed := false
	extConfig := chainConfig.Consensus.ExtConfig
	if extConfig == nil {
		extConfig = make([]*configPb.ConfigKeyValue, 0)
	}

	extConfigMap := make(map[string]string)
	for _, v := range extConfig {
		extConfigMap[v.Key] = string(v.Value)
	}

	for key, val := range params {
		if _, ok := extConfigMap[key]; !ok {
			continue
		}
		for i, config := range extConfig {
			if key == config.Key {
				extConfig[i] = &configPb.ConfigKeyValue{
					Key:   key,
					Value: string(val),
				}
				chainConfig.Consensus.ExtConfig = extConfig
				changed = true
				break
			}
		}
	}

	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("consensus ext update fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("consensus ext update success. params %+v", params)
	}
	return result, err
}

// ConsensusExtDelete delete consensus extra
func (r *ChainConsensusRuntime) ConsensusExtDelete(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	changed := false
	extConfig := chainConfig.Consensus.ExtConfig
	if extConfig == nil {
		return nil, errors.New("ext_config is empty")
	}
	extConfigMap := make(map[string]string)
	for _, v := range extConfig {
		extConfigMap[v.Key] = string(v.Value)
	}

	for key := range params {
		if _, ok := extConfigMap[key]; !ok {
			continue
		}

		for i, config := range extConfig {
			if key == config.Key {
				extConfig = append(extConfig[:i], extConfig[i+1:]...)
				changed = true
				break
			}
		}
	}
	chainConfig.Consensus.ExtConfig = extConfig
	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("consensus ext delete fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("consensus ext delete success. params %+v", params)
	}
	return result, err
}

// [permissions]
//type ChainPermissionRuntime struct {
//	log protocol.Logger
//}

// ResourcePolicyAdd add permission
func (r *ChainConsensusRuntime) ResourcePolicyAdd(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	changed := false
	resourcePolicies := chainConfig.ResourcePolicies
	if resourcePolicies == nil {
		resourcePolicies = make([]*configPb.ResourcePolicy, 0)
	}

	resourceMap := make(map[string]interface{})
	for _, p := range resourcePolicies {
		resourceMap[p.ResourceName] = struct{}{}
	}

	sortedParams := sortedmap.NewStringKeySortedMapWithBytesData(params)
	var parseParamErr error
	sortedParams.Range(func(key string, val interface{}) (isContinue bool) {
		value, ok := val.([]byte)
		if !ok {
			r.log.Error("value not a string")
		}
		_, ok = resourceMap[key]
		if ok {
			parseParamErr = fmt.Errorf("permission resource_name[%s] is exist", key)
			r.log.Errorf(parseParamErr.Error())
			return false
		}

		policy := &acPb.Policy{}
		err1 := proto.Unmarshal([]byte(value), policy)
		if err1 != nil {
			parseParamErr = fmt.Errorf("policy Unmarshal err:%s", err1)
			r.log.Errorf(parseParamErr.Error())
			return false
		}

		resourcePolicy := &configPb.ResourcePolicy{
			ResourceName: key,
			Policy:       policy,
		}

		ac, err1 := txSimContext.GetAccessControl()
		if err1 != nil {
			parseParamErr = fmt.Errorf("add resource policy GetAccessControl err:%s", err1)
			r.log.Errorf(parseParamErr.Error())
			return false
		}

		b := ac.ValidateResourcePolicy(resourcePolicy)
		if !b {
			parseParamErr = fmt.Errorf(
				"add resource policy failed this resourcePolicy is restricted CheckPrincipleValidity"+
					" err resourcePolicy[%s]", resourcePolicy)
			r.log.Errorf(parseParamErr.Error())
			return false
		}
		resourcePolicies = append(resourcePolicies, resourcePolicy)

		chainConfig.ResourcePolicies = resourcePolicies
		changed = true
		return true
	})

	if parseParamErr != nil {
		return nil, parseParamErr
	}
	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("resource policy add fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("resource policy add success. params %+v", params)
	}
	return result, err
}

// ResourcePolicyUpdate update resource policy
func (r *ChainConsensusRuntime) ResourcePolicyUpdate(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	changed := false
	resourcePolicies := chainConfig.ResourcePolicies
	if resourcePolicies == nil {
		resourcePolicies = make([]*configPb.ResourcePolicy, 0)
	}

	resourceMap := make(map[string]interface{})
	for _, p := range resourcePolicies {
		resourceMap[p.ResourceName] = struct{}{}
	}

	sortedParams := sortedmap.NewStringKeySortedMapWithBytesData(params)
	var parseParamErr error
	sortedParams.Range(func(key string, val interface{}) (isContinue bool) {
		value, ok := val.([]byte)
		if !ok {
			r.log.Error("value not a string")
		}
		_, ok = resourceMap[key]
		if !ok {
			parseParamErr = fmt.Errorf("permission resource name does not exist resource_name[%s]", value)
			r.log.Errorf(parseParamErr.Error())
			return false
		}
		policy := &acPb.Policy{}
		err1 := proto.Unmarshal(value, policy)
		if err1 != nil {
			parseParamErr = fmt.Errorf("policy Unmarshal err:%s", err1)
			r.log.Errorf(parseParamErr.Error())
			return false
		}
		for i, resourcePolicy := range resourcePolicies {
			if resourcePolicy.ResourceName != key {
				continue
			}
			rp := &configPb.ResourcePolicy{
				ResourceName: key,
				Policy:       policy,
			}
			ac, err1 := txSimContext.GetAccessControl()
			if err1 != nil {
				parseParamErr = fmt.Errorf("GetAccessControl, err:%s", err1)
				r.log.Errorf(parseParamErr.Error())
				return false
			}
			b := ac.ValidateResourcePolicy(rp)
			if !b {
				parseParamErr = fmt.Errorf(
					"update resource policy this resourcePolicy is restricted. CheckPrincipleValidity"+
						" err resourcePolicy %+v", rp)
				r.log.Errorf(parseParamErr.Error())
				return false
			}
			resourcePolicies[i] = rp
			chainConfig.ResourcePolicies = resourcePolicies
			changed = true
		}
		return true
	})

	if parseParamErr != nil {
		return nil, parseParamErr
	}

	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("resource policy update fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("resource policy update success. params %+v", params)
	}
	return result, err
}

// ResourcePolicyDelete delete permission
func (r *ChainConsensusRuntime) ResourcePolicyDelete(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := getChainConfig(txSimContext, params)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	changed := false
	resourcePolicies := chainConfig.ResourcePolicies
	if resourcePolicies == nil {
		resourcePolicies = make([]*configPb.ResourcePolicy, 0)
	}

	resourceMap := make(map[string]interface{})
	for _, p := range resourcePolicies {
		resourceMap[p.ResourceName] = struct{}{}
	}

	// map is out of order, in order to ensure that each execution sequence is consistent, we need to sort
	sortedParams := sortedmap.NewStringKeySortedMapWithBytesData(params)
	var parseParamErr error
	sortedParams.Range(func(key string, val interface{}) (isContinue bool) {
		_, ok := resourceMap[key]
		if !ok {
			parseParamErr = fmt.Errorf("permission resource name does not exist resource_name[%s]", key)
			r.log.Error(parseParamErr.Error())
			return false
		}
		resourcePolicy := &configPb.ResourcePolicy{
			ResourceName: key,
			Policy: &acPb.Policy{
				Rule:     string(protocol.RuleDelete),
				OrgList:  nil,
				RoleList: nil,
			},
		}
		ac, err1 := txSimContext.GetAccessControl()
		if err1 != nil {
			parseParamErr = fmt.Errorf("delete resource policy GetAccessControl err:%s", err1)
			r.log.Error(parseParamErr.Error())
			return false
		}
		b := ac.ValidateResourcePolicy(resourcePolicy)
		if !b {
			parseParamErr = fmt.Errorf("delete resource policy this resourcePolicy is restricted,"+
				" CheckPrincipleValidity err resourcePolicy %+v", resourcePolicy)
			r.log.Error(parseParamErr.Error())
			return false
		}

		for i, rp := range resourcePolicies {
			if rp.ResourceName == key {
				resourcePolicies = append(resourcePolicies[:i], resourcePolicies[i+1:]...)
				chainConfig.ResourcePolicies = resourcePolicies
				changed = true
				break
			}
		}
		return true
	})
	if parseParamErr != nil {
		return nil, parseParamErr
	}

	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("resource policy delete fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("resource policy delete success. params %+v", params)
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

func verifyConsensusCount(chainConfig *configPb.ChainConfig, nodeId, orgId string) error {
	// raft && pubKey, verify nodeIds count
	if isRaftPubKey(chainConfig) {
		count := 0
		for _, node := range chainConfig.Consensus.Nodes {
			if orgId == node.OrgId {
				continue
			}
			for _, nodeIdTmp := range node.NodeId {
				if nodeId == nodeIdTmp {
					continue
				}
				count++
			}
		}
		if count < 2 {
			return fmt.Errorf("raft use pubkey, when delete nodes, the remaining consensus nodes counts need >= 2")
		}
	}
	return nil
}

func isRaftPubKey(chainConfig *configPb.ChainConfig) bool {
	return chainConfig.Consensus.Type == consensus.ConsensusType_RAFT && !useCert(chainConfig.AuthType)
}
func isRaft(chainConfig *configPb.ChainConfig) bool {
	return chainConfig.Consensus.Type == consensus.ConsensusType_RAFT
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

	chainConfig, err = GetChainConfigEmptyParams(txSimContext)
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

	chainConfig.Version = fmt.Sprintf("%d", version)
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return result, nil
}
