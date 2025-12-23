/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chainconf

import (
	"crypto/sha256"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"sync"

	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/common/v2/helper"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/consensus"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
)

type consensusVerifier map[consensus.ConsensusType]protocol.Verifier

const (
	minTxTimeout       = 600
	minBlockInterval   = 10
	maxBlockInterval   = 10000
	minBlockTxCapacity = 1
	minBlockSize       = 1
	minNodeIds         = 1
	minTrustRoots      = 1
	maxTxParameterSize = 100
	minBlockTimeout    = 10
	blockVersion233    = 2030300
)

var (
	chainConsensusVerifier = make(map[string]consensusVerifier)
	// for multi chain start
	chainConfigVerifierLock = sync.RWMutex{}
)

// ChainConfig ChainConfig struct
type ChainConfig struct {
	*config.ChainConfig
	NodeOrgIds       map[string][]string // NodeOrgIds is a map mapped org with consensus nodes ids.
	NodeIds          map[string]string   // NodeIds is a map mapped node id with org.
	CaRoots          map[string]struct{} // CaRoots is a map stored org ids.
	ResourcePolicies map[string]struct{} // ResourcePolicies is a map stored resource.
}

func verifyAuthType(cconfig *config.ChainConfig, mConfig *ChainConfig, authType string) error {
	chainLog := log
	switch strings.ToLower(authType) {
	case protocol.Identity, protocol.PermissionedWithCert:
		if err := verifyChainConfigTrustRoots(cconfig, mConfig, chainLog); err != nil {
			return err
		}

		if err := verifyChainConfigTrustMembers(cconfig); err != nil {
			return err
		}

		if err := verifyChainConfigConsensus(cconfig, mConfig, chainLog); err != nil {
			return err
		}

		if err := verifyChainConfigResourcePolicies(cconfig, mConfig); err != nil {
			return err
		}
	case protocol.PermissionedWithKey:
		if err := verifyChainConfigTrustRootsInPermissionedWithKey(cconfig, mConfig, chainLog); err != nil {
			return err
		}

		if err := verifyChainConfigConsensusInPermissionedWithKey(cconfig, mConfig, chainLog); err != nil {
			return err
		}

		if err := verifyChainConfigResourcePolicies(cconfig, mConfig); err != nil {
			return err
		}
	case protocol.Public:
		if err := verifyChainConfigTrustRootsInPublic(cconfig, mConfig, chainLog); err != nil {
			return err
		}
		if len(mConfig.TrustRoots[0].Root) < minTrustRoots {
			msg := fmt.Sprintf("public root at least one, but there is [%d]", len(mConfig.TrustRoots))
			chainLog.Error(msg)
			return errors.New(msg)
		}
		if err := verifyChainConfigConsensusInPublic(cconfig, mConfig, chainLog); err != nil {
			return err
		}
	default:
		err := fmt.Errorf("the authentication type does not exist")
		return err
	}
	return nil
}

// VerifyChainConfig verify the chain config.
func VerifyChainConfig(cconfig *config.ChainConfig) (*ChainConfig, error) {
	chainLog := log
	// validate params
	if err := validateParams(cconfig); err != nil {
		return nil, err
	}

	mConfig := &ChainConfig{
		ChainConfig:      cconfig,
		NodeOrgIds:       make(map[string][]string),
		NodeIds:          make(map[string]string),
		CaRoots:          make(map[string]struct{}),
		ResourcePolicies: make(map[string]struct{}),
	}

	if cconfig.AuthType == "" {
		cconfig.AuthType = protocol.PermissionedWithCert
	}
	if err := verifyAuthType(cconfig, mConfig, cconfig.AuthType); err != nil {
		return nil, err
	}

	if cconfig.Consensus.Type != consensus.ConsensusType_DPOS && len(mConfig.NodeIds) < minNodeIds {
		msg := fmt.Sprintf("nodeIds len less than %d, nodeIds len is %d", minNodeIds, len(mConfig.NodeIds))
		chainLog.Error(msg)
		return nil, errors.New(msg)
	}

	if len(mConfig.TrustRoots) < minTrustRoots {
		msg := fmt.Sprintf("trustRoots len less than %d, trustRoots len is %d", minTrustRoots, len(mConfig.TrustRoots))
		chainLog.Error(msg)
		return nil, errors.New(msg)
	}

	// block
	if err := verifyBlockConfig(cconfig); err != nil {
		return nil, err
	}

	// MAXBFT consensus not support sql
	if cconfig.Contract.EnableSqlSupport {
		if cconfig.Consensus.Type == consensus.ConsensusType_MAXBFT {
			chainLog.Errorf("chain config error: chain config sql is enable, expect consensus tbft/raft/solo,"+
				" but got %s. current config: consensus.type = %s, contract.enable_sql_support = true",
				consensus.ConsensusType_MAXBFT.String(), consensus.ConsensusType_MAXBFT)
			return nil, errors.New("chain config error")
		}
	}

	// support the size of tx parameters,within [0,100]MB.
	if cconfig.Block.TxParameterSize > maxTxParameterSize {
		msg := fmt.Sprintf("txParameterSize should be [0,100] MB, txParameterSize is %d",
			cconfig.Block.TxParameterSize)
		chainLog.Error(msg)
		return nil, errors.New(msg)
	}

	// get verifier to verify config
	verifier := GetVerifier(cconfig.ChainId, cconfig.Consensus.Type)
	if verifier != nil {
		err := verifier.Verify(cconfig.Consensus.Type, cconfig)
		if err != nil {
			chainLog.Errorw("consensus verify is err", "err", err)
			return nil, err
		}
	}

	return mConfig, nil
}

func verifyBlockConfig(cconfig *config.ChainConfig) error {
	chainLog := log
	if cconfig.Block.TxTimeout < minTxTimeout {
		// timeout
		msg := fmt.Sprintf("txTimeout less than %d, txTimeout is %d", minTxTimeout, cconfig.Block.TxTimeout)
		chainLog.Error(msg)
		return errors.New(msg)
	}
	if cconfig.Block.BlockTxCapacity < minBlockTxCapacity {
		// block tx cap
		msg := fmt.Sprintf("blockTxCapacity less than %d, blockTxCapacity is %d",
			minBlockTxCapacity, cconfig.Block.BlockTxCapacity)
		chainLog.Error(msg)
		return errors.New(msg)
	}
	if cconfig.Block.BlockSize < minBlockSize {
		// block size
		msg := fmt.Sprintf("blockSize less than %d, blockSize is %d", minBlockSize, cconfig.Block.BlockSize)
		chainLog.Error(msg)
		return errors.New(msg)
	}
	if cconfig.Block.BlockInterval < minBlockInterval || cconfig.Block.BlockInterval > maxBlockInterval {
		// block interval
		msg := fmt.Sprintf("blockInterval should be within the range of [%d,%d], blockInterval is %d",
			minBlockInterval, maxBlockInterval, cconfig.Block.BlockInterval)
		chainLog.Error(msg)
		return errors.New(msg)
	}

	if cconfig.Contract == nil {
		cconfig.Contract = &config.ContractConfig{EnableSqlSupport: false} //by default disable sql support
	}

	// init Consensus turbo's config
	if cconfig.Core.ConsensusTurboConfig == nil {
		cconfig.Core.ConsensusTurboConfig = &config.ConsensusTurboConfig{
			ConsensusMessageTurbo: false,
			RetryTime:             0,
			RetryInterval:         0,
		}
	}

	if cconfig.Block.BlockTimestampVerify && cconfig.Block.BlockTimeout < minBlockTimeout {
		chainLog.Errorf("blockTimeout should be more than [%d] if BlockTimestampVerify was true",
			minBlockTimeout)
		return errors.New("chain config error")
	}
	return nil
}

func verifyChainConfigTrustRoots(config *config.ChainConfig, mConfig *ChainConfig, log protocol.Logger) error {
	// load all ca root certs
	for _, orgRoots := range config.TrustRoots {
		if _, ok := mConfig.CaRoots[orgRoots.OrgId]; ok {
			err := fmt.Errorf("check root certificate failed, org id [%s] already exists", orgRoots.OrgId)
			log.Error(err)
			return err
		}
		repeatCertCheck := make(map[string]struct{}, len(orgRoots.Root))
		for _, root := range orgRoots.Root {
			id := fmt.Sprintf("%x", sha256.Sum256([]byte(root)))
			if _, ok := repeatCertCheck[id]; ok {
				err := fmt.Errorf("check root certificate failed,trust root already exists in orgId[%s]", orgRoots.OrgId)
				log.Error(err)
				return err
			}
			repeatCertCheck[id] = struct{}{}

			// check root cert
			if ok, err := utils.CheckRootCertificate(root); err != nil && !ok {
				log.Errorf("check root certificate failed, %s", err.Error())
				return err
			}
		}
		mConfig.CaRoots[orgRoots.OrgId] = struct{}{}
	}
	return nil
}

func verifyChainConfigTrustMembers(config *config.ChainConfig) error {
	// load all ca root certs
	repeatMemberCheck := make(map[string]struct{})
	for _, member := range config.TrustMembers {
		id := fmt.Sprintf("%x", sha256.Sum256([]byte(member.MemberInfo)))
		if _, ok := repeatMemberCheck[id]; ok {
			return fmt.Errorf("check trust members failed, trust members already exists in orgId[%s] role[%s]",
				member.OrgId, member.Role)
		}
		repeatMemberCheck[id] = struct{}{}
		block, _ := pem.Decode([]byte(member.MemberInfo))
		if block == nil {
			return errors.New("trust member is empty")
		}
	}
	return nil
}

func verifyChainConfigConsensus(config *config.ChainConfig, mConfig *ChainConfig, log protocol.Logger) error {
	// verify consensus
	if config.Consensus != nil && config.Consensus.Type != consensus.ConsensusType_DPOS {
		if len(config.Consensus.Nodes) == 0 {
			err := fmt.Errorf("there is at least one consensus node")
			log.Error(err.Error())
			return err
		}
		for _, node := range config.Consensus.Nodes {
			// org id can not be repeated
			if _, ok := mConfig.NodeOrgIds[node.OrgId]; ok {
				err := fmt.Errorf("org id(%s) existed", node.OrgId)
				log.Error(err.Error())
				return err
			}
			// when creating genesis, the org id of node must be exist in CaRoots.
			if _, ok := mConfig.CaRoots[node.OrgId]; !ok {
				err := fmt.Errorf("org id(%s) not in trust roots config", node.OrgId)
				log.Error(err.Error())
				return err
			}

			mConfig.NodeOrgIds[node.OrgId] = node.NodeId
			if err := verifyChainConfigConsensusNodesIds(mConfig, node, log); err != nil {
				return err
			}
		}
	}
	return nil
}

// verify chainConfig about consensus nodes' ids
func verifyChainConfigConsensusNodesIds(mConfig *ChainConfig, node *config.OrgConfig, log protocol.Logger) error {
	if len(node.NodeId) > 0 {
		for _, nid := range node.NodeId {
			// check node id
			if !helper.P2pAddressFormatVerify("/p2p/" + nid) {
				log.Errorf("wrong node id set: %s", nid)
				return errors.New("wrong node id set")
			}
			// node id can not be repeated
			if _, ok := mConfig.NodeIds[nid]; ok {
				log.Errorf("node id(%s) existed", nid)
				return errors.New("node id existed")
			}
			mConfig.NodeIds[nid] = node.OrgId
		}
	} else {
		for _, addr := range node.Address {
			nid, err := helper.GetNodeUidFromAddr(addr)
			if err != nil {
				log.Errorf("get node id from addr(%s) failed", addr)
				return err
			}
			// node id can not be repeated
			if _, ok := mConfig.NodeIds[nid]; ok {
				log.Errorf("node id(%s) existed", nid)
				return errors.New("node id existed")
			}
			mConfig.NodeIds[nid] = node.OrgId
		}
	}

	return nil
}

// verify chainConfig resource policies
func verifyChainConfigResourcePolicies(config *config.ChainConfig, mConfig *ChainConfig) error {
	if config.ResourcePolicies != nil {
		resourceLen := len(config.ResourcePolicies)
		for _, resourcePolicy := range config.ResourcePolicies {
			mConfig.ResourcePolicies[resourcePolicy.ResourceName] = struct{}{}
			if err := verifyPolicy(resourcePolicy); err != nil {
				return err
			}
		}
		resLen := len(mConfig.ResourcePolicies)
		if resourceLen != resLen {
			return errors.New("resource name duplicate")
		}
	}
	return nil
}

// verify Policy
func verifyPolicy(resourcePolicy *config.ResourcePolicy) error {
	policy := resourcePolicy.Policy
	resourceName := resourcePolicy.ResourceName
	if policy != nil {
		// to upper
		rule := policy.Rule
		policy.Rule = strings.ToUpper(rule)

		// self only for NODE_ID_UPDATE or TRUST_ROOT_UPDATE
		if policy.Rule == string(protocol.RuleSelf) {
			if resourceName != syscontract.SystemContract_CHAIN_CONFIG.String()+"-"+
				syscontract.ChainConfigFunction_NODE_ID_UPDATE.String() &&
				resourceName != syscontract.SystemContract_CHAIN_CONFIG.String()+"-"+
					syscontract.ChainConfigFunction_TRUST_ROOT_UPDATE.String() {
				err := fmt.Errorf("self rule can only be used by NODE_ID_UPDATE or TRUST_ROOT_UPDATE")
				return err
			}
		}

		roles := policy.RoleList
		if roles != nil {
			// to upper
			for i, role := range roles {
				role = strings.ToUpper(role)
				roles[i] = role
				// MAJORITY role allow admin or null
				if policy.Rule == string(protocol.RuleMajority) {
					if len(role) > 0 && role != string(protocol.RoleAdmin) {
						err := fmt.Errorf("config rule[MAJORITY], role can only be admin or null")
						return err
					}
				}
			}
			policy.RoleList = roles
		}
		// MAJORITY  not allowed org_list
		if policy.Rule == string(protocol.RuleMajority) && len(policy.OrgList) > 0 {
			err := fmt.Errorf("config rule[MAJORITY], org_list param not allowed")
			return err
		}
	}
	return nil
}

// verify chainConfig about trustRoots in PermissionedWithKey way
func verifyChainConfigTrustRootsInPermissionedWithKey(config *config.ChainConfig,
	mConfig *ChainConfig, log protocol.Logger) error {
	// load all ca root certs
	for _, orgRoots := range config.TrustRoots {
		if _, ok := mConfig.CaRoots[orgRoots.OrgId]; ok {
			err := fmt.Errorf("check root certificate failed, org id [%s] already exists", orgRoots.OrgId)
			log.Error(err)
			return err
		}
		repeatCertCheck := make(map[string]struct{}, len(orgRoots.Root))
		for _, root := range orgRoots.Root {
			id := fmt.Sprintf("%x", sha256.Sum256([]byte(root)))
			if _, ok := repeatCertCheck[id]; ok {
				err := fmt.Errorf("check root certificate failed,trust root already exists in orgId[%s]",
					orgRoots.OrgId)
				log.Error(err)
				return err
			}
			repeatCertCheck[id] = struct{}{}

			_, err := asym.PublicKeyFromPEM([]byte(root))
			if err != nil {
				log.Errorf("check the trust root failed: %s", err.Error())
				return err
			}
		}
		mConfig.CaRoots[orgRoots.OrgId] = struct{}{}
	}
	return nil
}

// verify chainConfig about consensus in PermissionedWithKey way
func verifyChainConfigConsensusInPermissionedWithKey(config *config.ChainConfig,
	mConfig *ChainConfig,
	log protocol.Logger) error {
	// verify consensus
	if config.Consensus != nil && config.Consensus.Type != consensus.ConsensusType_DPOS {
		if len(config.Consensus.Nodes) == 0 {
			err := fmt.Errorf("there is at least one consensus node")
			log.Error(err.Error())
			return err
		}
		for _, node := range config.Consensus.Nodes {
			// org id can not be repeated
			if _, ok := mConfig.NodeOrgIds[node.OrgId]; ok {
				err := fmt.Errorf("org id(%s) existed", node.OrgId)
				log.Error(err.Error())
				return err
			}
			if mConfig.ChainConfig.GetBlockVersion() >= blockVersion233 {
				if _, ok := mConfig.CaRoots[node.OrgId]; !ok {
					err := fmt.Errorf("org id(%s) not in trust roots config", node.OrgId)
					log.Error(err.Error())
					return err
				}
			}
			mConfig.NodeOrgIds[node.OrgId] = node.NodeId
			if err := verifyChainConfigConsensusNodesIds(mConfig, node, log); err != nil {
				return err
			}
		}
	}
	return nil
}

// verify chainConfig about consensus in Public way
func verifyChainConfigConsensusInPublic(config *config.ChainConfig, mConfig *ChainConfig, log protocol.Logger) error {
	// verify consensus, dPos consensus has no "nodes" configuration
	if config.Consensus != nil && config.Consensus.Type != consensus.ConsensusType_DPOS {
		if len(config.Consensus.Nodes) == 0 {
			err := fmt.Errorf("there is at least one consensus node")
			log.Error(err.Error())
			return err
		}
		for _, node := range config.Consensus.Nodes {
			// org id can not be repeated
			if _, ok := mConfig.NodeOrgIds[node.OrgId]; ok {
				err := fmt.Errorf("org id(%s) existed", node.OrgId)
				log.Error(err.Error())
				return err
			}
			mConfig.NodeOrgIds[node.OrgId] = node.NodeId
			if err := verifyChainConfigConsensusNodesIds(mConfig, node, log); err != nil {
				return err
			}
		}
	}
	return nil
}

// verify chainConfig about trustRoots in Public way
func verifyChainConfigTrustRootsInPublic(config *config.ChainConfig,
	mConfig *ChainConfig, log protocol.Logger) error {

	// load all ca root certs
	if len(config.TrustRoots) != 1 || config.TrustRoots[0].OrgId != protocol.Public {
		err := fmt.Errorf("check the trust root failed: illegal trust roots configuration in public authType")
		log.Error(err.Error())
		return err
	}

	for _, root := range config.TrustRoots[0].Root {
		if _, ok := mConfig.CaRoots[root]; ok {
			err := fmt.Errorf("check the trust root failed: the root public key already exist")
			log.Error(err.Error())
			return err
		}
		_, err := asym.PublicKeyFromPEM([]byte(root))
		if err != nil {
			log.Errorf("check the trust root failed: %s", err.Error())
			return err
		}
		mConfig.CaRoots[root] = struct{}{}
	}
	return nil
}

// validateParams validate the chainconfig
func validateParams(config *config.ChainConfig) error {
	if config.TrustRoots == nil {
		return errors.New("chainconfig trust_roots is nil")
	}
	if config.Consensus == nil {
		return errors.New("chainconfig consensus is nil")
	}
	if config.Block == nil {
		return errors.New("chainconfig block is nil")
	}

	// bc.yml forward compatibility
	if config.Vm == nil {
		setDefaultVm(config)
	}

	match := utils.CheckChainIdFormat(config.ChainId)
	if !match {
		return fmt.Errorf("chain id[%s] can only consist of numbers,"+
			" letters and underscores and chainId length must less than 30", config.ChainId)
	}
	return nil
}

// setDefaultVm is used to handle bc.yml forward compatibility
func setDefaultVm(chainConf *config.ChainConfig) {
	chainConf.Vm = &config.Vm{
		SupportList: []string{"wasmer", "evm", "gasm", "wxvm", "dockergo"},
		//SupportList: []string{"wasmer", "evm", "gasm", "dockergo"},
	}
}

// RegisterVerifier register a verifier.
func RegisterVerifier(chainId string, consensusType consensus.ConsensusType, verifier protocol.Verifier) error {
	chainConfigVerifierLock.Lock()
	defer chainConfigVerifierLock.Unlock()
	initChainConsensusVerifier(chainId)
	if _, ok := chainConsensusVerifier[chainId][consensusType]; ok {
		return errors.New("consensusType verifier is exist")
	}
	chainConsensusVerifier[chainId][consensusType] = verifier
	return nil
}

// GetVerifier get a verifier if exist.
func GetVerifier(chainId string, consensusType consensus.ConsensusType) protocol.Verifier {
	chainConfigVerifierLock.RLock()
	defer chainConfigVerifierLock.RUnlock()
	initChainConsensusVerifier(chainId)
	verifier, ok := chainConsensusVerifier[chainId][consensusType]
	if !ok {
		return nil
	}
	return verifier
}

// init chain 's consensus Verifier
func initChainConsensusVerifier(chainId string) {
	if _, ok := chainConsensusVerifier[chainId]; !ok {
		chainConsensusVerifier[chainId] = make(consensusVerifier)
	}
}

// IsNativeTx whether the transaction is a native
func isNativeTx(tx *common.Transaction) (contract string, b bool) {
	if tx == nil || tx.Payload == nil {
		return "", false
	}
	txType := tx.Payload.TxType
	switch txType {
	case common.TxType_INVOKE_CONTRACT:
		payload := tx.Payload
		return payload.ContractName, utils.IsNativeContract(payload.ContractName)
	default:
		return "", false
	}
}

// IsNativeTxSucc whether the transaction is a native and run success
func IsNativeTxSucc(tx *common.Transaction) (contract string, b bool) {
	if tx.Result == nil || tx.Result.ContractResult == nil || tx.Result.ContractResult.Result == nil {
		return "", false
	}
	contract, b = isNativeTx(tx)
	if !b {
		return "", false
	}
	if tx.Result.Code != common.TxStatusCode_SUCCESS {
		return "", false
	}
	return contract, true
}
