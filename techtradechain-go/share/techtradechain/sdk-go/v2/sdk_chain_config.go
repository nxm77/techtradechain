/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"fmt"
	"strconv"
	"strings"

	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
	"github.com/gogo/protobuf/proto"
)

const (
	getCCSeqErrStringFormat = "get chain config sequence failed, %s"
)

// GetChainConfig get chain config
func (cc *ChainClient) GetChainConfig() (*config.ChainConfig, error) {
	cc.logger.Debug("[SDK] begin to get chain config")

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_GET_CHAIN_CONFIG.String(), nil, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf("send %s failed, %s", payload.TxType.String(), err.Error())
	}

	if err := utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, err
	}

	chainConfig := &config.ChainConfig{}
	err = proto.Unmarshal(resp.ContractResult.Result, chainConfig)
	if err != nil {
		return nil, fmt.Errorf("unmarshal contract result failed, %s", err.Error())
	}

	return chainConfig, nil
}

// GetChainConfigByBlockHeight get chain config by block height
func (cc *ChainClient) GetChainConfigByBlockHeight(blockHeight uint64) (*config.ChainConfig, error) {
	if cc.IsArchiveCenterQueryFist() {
		// 首先去归档中心查询
		cfg, err := cc.GetArchiveService().GetChainConfigByBlockHeight(blockHeight)
		if err == nil && cfg != nil {
			return cfg, nil
		}
	}
	cc.logger.Debugf("[SDK] begin to get chain config by block height [%d]", blockHeight)

	var pairs = []*common.KeyValuePair{{
		Key:   utils.KeyChainConfigContractBlockHeight,
		Value: []byte(strconv.FormatUint(blockHeight, 10)),
	}}

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_GET_CHAIN_CONFIG_AT.String(), pairs, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err := utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	chainConfig := &config.ChainConfig{}
	err = proto.Unmarshal(resp.ContractResult.Result, chainConfig)
	if err != nil {
		return nil, fmt.Errorf("GetChainConfigByBlockHeight unmarshal contract result failed, %s", err)
	}

	return chainConfig, nil
}

// GetChainConfigSequence get chain config sequence
func (cc *ChainClient) GetChainConfigSequence() (uint64, error) {
	cc.logger.Debug("[SDK] begin to get chain config sequence")

	chainConfig, err := cc.GetChainConfig()
	if err != nil {
		return 0, err
	}

	return chainConfig.Sequence, nil
}

// SignChainConfigPayload sign chain config payload
func (cc *ChainClient) SignChainConfigPayload(payload *common.Payload) (*common.EndorsementEntry, error) {
	return cc.SignPayload(payload)
}

// CreateChainConfigCoreUpdatePayload create chain config core update payload
func (cc *ChainClient) CreateChainConfigCoreUpdatePayload(txSchedulerTimeout,
	txSchedulerValidateTimeout uint64) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [CoreUpdate] to be signed payload")

	if txSchedulerTimeout > 60 {
		return nil, fmt.Errorf("[tx_scheduler_timeout] should be (0,60]")
	}

	if txSchedulerValidateTimeout > 60 {
		return nil, fmt.Errorf("[tx_scheduler_validate_timeout] should be (0,60]")
	}

	var pairs []*common.KeyValuePair
	if txSchedulerTimeout > 0 {
		pairs = append(pairs, &common.KeyValuePair{
			Key:   utils.KeyTxSchedulerTimeout,
			Value: []byte(strconv.FormatUint(txSchedulerTimeout, 10)),
		})
	}

	if txSchedulerValidateTimeout > 0 {
		pairs = append(pairs, &common.KeyValuePair{
			Key:   utils.KeyTxSchedulerValidateTimeout,
			Value: []byte(strconv.FormatUint(txSchedulerValidateTimeout, 10)),
		})
	}

	if len(pairs) == 0 {
		return nil, fmt.Errorf("update nothing")
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_CORE_UPDATE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigBlockUpdatePayload create chain config block update payload
func (cc *ChainClient) CreateChainConfigBlockUpdatePayload(txTimestampVerify, blockTimestampVerify bool, txTimeout,
	blockTimeout, blockTxCapacity, blockSize, blockInterval, txParamterSize uint32) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [BlockUpdate] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   "tx_timestamp_verify",
			Value: []byte(strconv.FormatBool(txTimestampVerify)),
		},
		{
			Key:   "block_timestamp_verify",
			Value: []byte(strconv.FormatBool(blockTimestampVerify)),
		},
	}

	if txTimeout < 600 {
		return nil, fmt.Errorf("[tx_timeout] should be [600, +∞)")
	}

	if blockTxCapacity < 1 {
		return nil, fmt.Errorf("[block_tx_capacity] should be (0, +∞]")
	}

	if blockSize < 1 {
		return nil, fmt.Errorf("[block_size] should be (0, +∞]")
	}

	if blockInterval < 10 {
		return nil, fmt.Errorf("[block_interval] should be [10, +∞]")
	}

	if blockTimeout < 10 {
		return nil, fmt.Errorf("[block_timeout] should be [10, +∞]")
	}

	if txTimeout > 0 {
		pairs = append(pairs, &common.KeyValuePair{
			Key:   utils.KeyTxTimeOut,
			Value: []byte(strconv.FormatUint(uint64(txTimeout), 10)),
		})
	}
	if blockTimeout > 0 {
		pairs = append(pairs, &common.KeyValuePair{
			Key:   utils.KeyBlockTimeOut,
			Value: []byte(strconv.FormatUint(uint64(blockTimeout), 10)),
		})
	}
	if blockTxCapacity > 0 {
		pairs = append(pairs, &common.KeyValuePair{
			Key:   utils.KeyBlockTxCapacity,
			Value: []byte(strconv.FormatUint(uint64(blockTxCapacity), 10)),
		})
	}
	if blockSize > 0 {
		pairs = append(pairs, &common.KeyValuePair{
			Key:   utils.KeyBlockSize,
			Value: []byte(strconv.FormatUint(uint64(blockSize), 10)),
		})
	}
	if blockInterval > 0 {
		pairs = append(pairs, &common.KeyValuePair{
			Key:   utils.KeyBlockInterval,
			Value: []byte(strconv.FormatUint(uint64(blockInterval), 10)),
		})
	}
	if txParamterSize > 0 {
		pairs = append(pairs, &common.KeyValuePair{
			Key:   utils.KeyTxParamterSize,
			Value: []byte(strconv.FormatUint(uint64(txParamterSize), 10)),
		})
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_BLOCK_UPDATE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigTrustRootAddPayload create chain config trust root add payload
func (cc *ChainClient) CreateChainConfigTrustRootAddPayload(trustRootOrgId string,
	trustRootCrt []string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [TrustRootAdd] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigContractOrgId,
			Value: []byte(trustRootOrgId),
		},
		{
			Key:   utils.KeyChainConfigContractRoot,
			Value: []byte(strings.Join(trustRootCrt, ",")),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_TRUST_ROOT_ADD.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigTrustRootUpdatePayload create chain config trust root update payload
func (cc *ChainClient) CreateChainConfigTrustRootUpdatePayload(trustRootOrgId string,
	trustRootCrt []string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [TrustRootUpdate] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigContractOrgId,
			Value: []byte(trustRootOrgId),
		},
		{
			Key:   utils.KeyChainConfigContractRoot,
			Value: []byte(strings.Join(trustRootCrt, ",")),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_TRUST_ROOT_UPDATE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigTrustRootDeletePayload create chain config trust root delete payload
func (cc *ChainClient) CreateChainConfigTrustRootDeletePayload(orgIdOrPKPubkeyPEM string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [TrustRootDelete] to be signed payload")

	var pairs []*common.KeyValuePair
	if cc.GetAuthType() == Public {
		pairs = []*common.KeyValuePair{
			{
				Key:   utils.KeyChainConfigContractOrgId,
				Value: []byte("public"),
			},
			{
				Key:   utils.KeyChainConfigContractRoot,
				Value: []byte(orgIdOrPKPubkeyPEM),
			},
		}
	} else {
		pairs = []*common.KeyValuePair{
			{
				Key:   utils.KeyChainConfigContractOrgId,
				Value: []byte(orgIdOrPKPubkeyPEM),
			},
		}
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_TRUST_ROOT_DELETE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigTrustMemberAddPayload create chain config trust member add payload
func (cc *ChainClient) CreateChainConfigTrustMemberAddPayload(trustMemberOrgId, trustMemberNodeId,
	trustMemberRole, trustMemberInfo string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [TrustRootAdd] to be signed payload")

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigContractTrustMemberOrgId,
			Value: []byte(trustMemberOrgId),
		},
		{
			Key:   utils.KeyChainConfigContractTrustMemberInfo,
			Value: []byte(trustMemberInfo),
		},
		{
			Key:   utils.KeyChainConfigContractNodeId,
			Value: []byte(trustMemberNodeId),
		},
		{
			Key:   utils.KeyChainConfigContractTrustMemberRole,
			Value: []byte(trustMemberRole),
		},
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_TRUST_MEMBER_ADD.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigTrustMemberDeletePayload create chain config trust member delete payload
func (cc *ChainClient) CreateChainConfigTrustMemberDeletePayload(trustMemberInfo string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [TrustRootDelete] to be signed payload")

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigContractTrustMemberInfo,
			Value: []byte(trustMemberInfo),
		},
	}
	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_TRUST_MEMBER_DELETE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigPermissionAddPayload create chain config permission add payload
func (cc *ChainClient) CreateChainConfigPermissionAddPayload(permissionResourceName string,
	policy *accesscontrol.Policy) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [PermissionAdd] to be signed payload")

	policyBytes, err := proto.Marshal(policy)
	if err != nil {
		return nil, fmt.Errorf("marshal policy failed, %s", err)
	}

	pairs := []*common.KeyValuePair{
		{
			Key:   permissionResourceName,
			Value: policyBytes,
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_PERMISSION_ADD.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigPermissionUpdatePayload create chain config permission update payload
func (cc *ChainClient) CreateChainConfigPermissionUpdatePayload(permissionResourceName string,
	policy *accesscontrol.Policy) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [PermissionUpdate] to be signed payload")

	policyBytes, err := proto.Marshal(policy)
	if err != nil {
		return nil, fmt.Errorf("marshal policy failed, %s", err)
	}

	pairs := []*common.KeyValuePair{
		{
			Key:   permissionResourceName,
			Value: policyBytes,
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_PERMISSION_UPDATE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigPermissionDeletePayload create chain config permission delete payload
func (cc *ChainClient) CreateChainConfigPermissionDeletePayload(resourceName string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [PermissionDelete] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key: resourceName,
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_PERMISSION_DELETE.String(), pairs, seq+1, nil)

	return payload, nil
}

// GetChainConfigPermissionList get chain config
func (cc *ChainClient) GetChainConfigPermissionList() ([]*config.ResourcePolicy, error) {
	cc.logger.Debug("[SDK] begin to get chain config permission list")

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_PERMISSION_LIST.String(), nil, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf("send %s failed, %s", payload.TxType.String(), err.Error())
	}

	if err := utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, err
	}

	chainConfig := &config.ChainConfig{}
	err = proto.Unmarshal(resp.ContractResult.Result, chainConfig)
	if err != nil {
		return nil, fmt.Errorf("unmarshal contract result failed, %s", err.Error())
	}

	return chainConfig.ResourcePolicies, nil
}

// CreateChainConfigConsensusNodeIdAddPayload create chain config consensus node id add payload
func (cc *ChainClient) CreateChainConfigConsensusNodeIdAddPayload(nodeOrgId string,
	nodeIds []string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [ConsensusNodeAddrAdd] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigContractOrgId,
			Value: []byte(nodeOrgId),
		},
		{
			Key:   utils.KeyChainConfigContractNodeIds,
			Value: []byte(strings.Join(nodeIds, ",")),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_NODE_ID_ADD.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigConsensusNodeIdUpdatePayload create chain config consensus node id update payload
func (cc *ChainClient) CreateChainConfigConsensusNodeIdUpdatePayload(nodeOrgId, nodeOldIds,
	nodeNewIds string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [ConsensusNodeAddrUpdate] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigContractOrgId,
			Value: []byte(nodeOrgId),
		},
		{
			Key:   utils.KeyChainConfigContractNodeId,
			Value: []byte(nodeOldIds),
		},
		{
			Key:   utils.KeyChainConfigContractNewNodeId,
			Value: []byte(nodeNewIds),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_NODE_ID_UPDATE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigConsensusNodeIdDeletePayload create chain config consensus node id delete payload
func (cc *ChainClient) CreateChainConfigConsensusNodeIdDeletePayload(nodeOrgId,
	nodeId string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [ConsensusNodeAddrDelete] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigContractOrgId,
			Value: []byte(nodeOrgId),
		},
		{
			Key:   utils.KeyChainConfigContractNodeId,
			Value: []byte(nodeId),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_NODE_ID_DELETE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigConsensusNodeOrgAddPayload create chain config consensus node org add payload
func (cc *ChainClient) CreateChainConfigConsensusNodeOrgAddPayload(nodeOrgId string,
	nodeIds []string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [ConsensusNodeOrgAdd] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigContractOrgId,
			Value: []byte(nodeOrgId),
		},
		{
			Key:   utils.KeyChainConfigContractNodeIds,
			Value: []byte(strings.Join(nodeIds, ",")),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_NODE_ORG_ADD.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigConsensusNodeOrgUpdatePayload create chain config consensus node org update payload
func (cc *ChainClient) CreateChainConfigConsensusNodeOrgUpdatePayload(nodeOrgId string,
	nodeIds []string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [ConsensusNodeOrgUpdate] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigContractOrgId,
			Value: []byte(nodeOrgId),
		},
		{
			Key:   utils.KeyChainConfigContractNodeIds,
			Value: []byte(strings.Join(nodeIds, ",")),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_NODE_ORG_UPDATE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigConsensusNodeOrgDeletePayload create chain config consensus node org delete payload
func (cc *ChainClient) CreateChainConfigConsensusNodeOrgDeletePayload(nodeOrgId string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [ConsensusNodeOrgAdd] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigContractOrgId,
			Value: []byte(nodeOrgId),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_NODE_ORG_DELETE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigConsensusExtAddPayload create chain config consensus ext add payload
func (cc *ChainClient) CreateChainConfigConsensusExtAddPayload(kvs []*common.KeyValuePair) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [ConsensusExtAdd] to be signed payload")

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_CONSENSUS_EXT_ADD.String(), kvs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigConsensusExtUpdatePayload create chain config consensus ext update payload
func (cc *ChainClient) CreateChainConfigConsensusExtUpdatePayload(kvs []*common.KeyValuePair) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [ConsensusExtUpdate] to be signed payload")

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_CONSENSUS_EXT_UPDATE.String(), kvs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigConsensusExtDeletePayload create chain config consensus ext delete payload
func (cc *ChainClient) CreateChainConfigConsensusExtDeletePayload(keys []string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [ConsensusExtDelete] to be signed payload")

	var pairs = make([]*common.KeyValuePair, len(keys))
	for i, key := range keys {
		pairs[i] = &common.KeyValuePair{
			Key: key,
		}
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_CONSENSUS_EXT_DELETE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigAlterAddrTypePayload create chain config alter address type payload
func (cc *ChainClient) CreateChainConfigAlterAddrTypePayload(addrType string) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [AlterAddrType] to be signed payload")

	if addrType != "0" && addrType != "1" {
		return nil, fmt.Errorf("unknown addr type [%s], only support: 0-TechTradeChain; 1-ZXL", addrType)
	}

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyChainConfigAddrType,
			Value: []byte(addrType),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_ALTER_ADDR_TYPE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigEnableOrDisableGasPayload create chain config enable or disable gas payload
func (cc *ChainClient) CreateChainConfigEnableOrDisableGasPayload() (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin to create [EnableOrDisable] to be signed payload")

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_ENABLE_OR_DISABLE_GAS.String(), nil, seq+1, nil)

	return payload, nil
}

// SendChainConfigUpdateRequest send chain config update request to node
func (cc *ChainClient) SendChainConfigUpdateRequest(payload *common.Payload, endorsers []*common.EndorsementEntry,
	timeout int64, withSyncResult bool) (*common.TxResponse, error) {
	return cc.proposalRequest(payload, endorsers, nil, timeout, withSyncResult)
}

// CreateChainConfigOptimizeChargeGasPayload create chain config optimize charge gas payload
func (cc *ChainClient) CreateChainConfigOptimizeChargeGasPayload(enable bool) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin CreateChainConfigOptimizeChargeGasPayload")

	var pairs = []*common.KeyValuePair{
		{
			Key:   utils.KeyEnableOptimizeChargeGas,
			Value: []byte(strconv.FormatBool(enable)),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_CORE_UPDATE.String(), pairs, seq+1, nil)

	return payload, nil
}

// CreateChainConfigEnableMultiSignManualRunPayload create chain config
// for multi-sign to `enable_manual_run` flag payload
func (cc *ChainClient) CreateChainConfigEnableMultiSignManualRunPayload(enable bool) (*common.Payload, error) {
	cc.logger.Debug("[SDK] begin CreateChainConfigEnableMultiSignManualRunPayload")

	var pairs = []*common.KeyValuePair{
		{
			Key:   utils.KeyMultiSignEnableManualRun,
			Value: []byte(strconv.FormatBool(enable)),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, fmt.Errorf(getCCSeqErrStringFormat, err)
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_MULTI_SIGN_ENABLE_MANUAL_RUN.String(), pairs, seq+1, nil)

	return payload, nil
}
