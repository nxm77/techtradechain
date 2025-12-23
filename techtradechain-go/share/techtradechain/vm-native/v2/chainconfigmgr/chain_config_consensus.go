/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Package chainconfigmgr is package for chainconf
package chainconfigmgr

import (
	"errors"
	"fmt"
	"strings"

	"techtradechain.com/techtradechain/common/v2/sortedmap"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/consensus"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
)

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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
