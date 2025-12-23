/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package chainconfigmgr2310

import (
	"fmt"
	"strings"

	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
)

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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
	chainConfig, err := common.GetChainConfig(txSimContext)
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

	// auth_type: pwk, raft consensus is not supported, so check is ignored

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
	chainConfig, err := common.GetChainConfig(txSimContext)
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

	// auth_type: pwk, raft consensus is not supported, so check is ignored

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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
	chainConfig, err := common.GetChainConfig(txSimContext)
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
