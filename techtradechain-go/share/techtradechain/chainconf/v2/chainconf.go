/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package chainconf record all the values of the chain config options.
package chainconf

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"

	"techtradechain.com/techtradechain/common/v2/helper"
	"techtradechain.com/techtradechain/common/v2/json"
	"techtradechain.com/techtradechain/logger/v2"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/groupcache/lru"
	"github.com/spf13/viper"
)

var _ protocol.ChainConf = (*ChainConf)(nil)
var log protocol.Logger = logger.GetLogger(logger.MODULE_CHAINCONF)

const (
	allContract = "ALL_CONTRACT"

	blockEmptyErrorTemplate = "block is empty"
)

var errBlockEmpty = errors.New(blockEmptyErrorTemplate)

// ChainConf is the config of a chain.
type ChainConf struct {
	// logger
	log protocol.Logger
	// extends options
	options
	// chain config
	ChainConf *config.ChainConfig
	// lock
	wLock sync.RWMutex
	// set config lock
	cLock sync.RWMutex
	// watchers, all watcher will be invoked when chain config changing.
	watchers []protocol.Watcher
	// contractName ==> module []VmWatcher
	vmWatchers map[string][]protocol.VmWatcher
	//blockHeight->*Block
	lru *lru.Cache
	//configBlockHeight->*BlockHeader
	configLru *lru.Cache
}

// NewChainConf create a new ChainConf instance.
func NewChainConf(opts ...Option) (*ChainConf, error) {
	chainConf := &ChainConf{
		watchers:   make([]protocol.Watcher, 0),
		vmWatchers: make(map[string][]protocol.VmWatcher),
		lru:        lru.New(100),
		configLru:  lru.New(10),
	}
	if err := chainConf.Apply(opts...); err != nil {
		log.Errorw("NewChainConf apply is error", "err", err)
		return nil, err
	}
	chainConf.log = logger.GetLoggerByChain(logger.MODULE_CHAINCONF, chainConf.chainId)

	return chainConf, nil
}

// Genesis will create new genesis config block of chain.
//根据bc1.yml配置文件生成ChainConfig配置对象
//如果配置文件有错误，则直接返回错误
func Genesis(genesisFile string) (*config.ChainConfig, error) {
	chainConfig := &config.ChainConfig{Contract: &config.ContractConfig{EnableSqlSupport: false}}
	fileInfo := map[string]interface{}{}
	// load content from file
	v := viper.New()
	v.SetConfigFile(genesisFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}
	if err := v.Unmarshal(&fileInfo); err != nil {
		return nil, err
	}
	bytes, err := json.Marshal(fileInfo)
	if err != nil {
		return nil, err
	}
	log.Debugf("initial genesis config: %s", string(bytes))
	err = json.Unmarshal(bytes, chainConfig)
	if err != nil {
		return nil, err
	}
	// load the trust root certs than set the bytes as value
	// need verify org and root certs
	for _, root := range chainConfig.TrustRoots {
		for i := 0; i < len(root.Root); i++ {
			filePath := root.Root[i]
			if !filepath.IsAbs(filePath) {
				filePath, err = filepath.Abs(filePath)
				if err != nil {
					return nil, err
				}
			}
			log.Infof("load trust root file path: %s", filePath)
			entry, err1 := ioutil.ReadFile(filePath)
			if err1 != nil {
				return nil, fmt.Errorf("fail to read whiltlist file [%s]: %v", filePath, err1)
			}
			root.Root[i] = string(entry)
		}
	}

	// load the trust member certs than set the bytes as value
	// need verify org
	trustMemberInfoMap := make(map[string]bool, len(chainConfig.TrustMembers))
	for _, member := range chainConfig.TrustMembers {
		filePath := member.MemberInfo
		if !filepath.IsAbs(filePath) {
			filePath, err = filepath.Abs(filePath)
			if err != nil {
				return nil, err
			}
		}
		log.Infof("load trust member file path: %s", filePath)
		entry, err1 := ioutil.ReadFile(filePath)
		if err1 != nil {
			return nil, fmt.Errorf("fail to read trust memberInfo file [%s]: %v", filePath, err1)
		}
		if _, ok := trustMemberInfoMap[string(entry)]; ok {
			return nil, fmt.Errorf("the trust member info is exist, member info: %s", string(entry))
		}
		member.MemberInfo = string(entry)
		trustMemberInfoMap[string(entry)] = true
	}

	// verify
	_, err = VerifyChainConfig(chainConfig)
	if err != nil {
		return nil, err
	}

	return chainConfig, nil
}

// Init chain config.
func (c *ChainConf) Init() error {
	return c.latestChainConfig()
}

// HandleCompatibility will make new version to be compatible with old version
func HandleCompatibility(chainConfig *config.ChainConfig) error {
	// For v1.1 to be compatible with v1.0, check consensus config
	for _, orgConfig := range chainConfig.Consensus.Nodes {
		if orgConfig.NodeId == nil {
			orgConfig.NodeId = make([]string, 0)
		}
		if len(orgConfig.NodeId) == 0 {
			for _, addr := range orgConfig.Address {
				nid, err := helper.GetNodeUidFromAddr(addr)
				if err != nil {
					return err
				}
				orgConfig.NodeId = append(orgConfig.NodeId, nid)
			}
			orgConfig.Address = nil
		}
	}
	/*
		// For v1.1 to be compatible with v1.0, check resource policies
		for _, rp := range ChainConfig.ResourcePolicies {
			switch rp.ResourceName {
			case syscontract.ChainConfigFunction_NODE_ID_ADD.String():
				rp.ResourceName = syscontract.ChainConfigFunction_NODE_ID_ADD.String()
			case syscontract.ChainConfigFunction_NODE_ID_UPDATE.String():
				rp.ResourceName = syscontract.ChainConfigFunction_NODE_ID_UPDATE.String()
			case syscontract.ChainConfigFunction_NODE_ID_DELETE.String():
				rp.ResourceName = syscontract.ChainConfigFunction_NODE_ID_DELETE.String()
			default:
				continue
			}
		}
	*/
	return nil
}

// latestChainConfig load latest chainConfig
func (c *ChainConf) latestChainConfig() error {
	// load chain config from store
	bytes, err := c.blockchainStore.ReadObject(syscontract.SystemContract_CHAIN_CONFIG.String(),
		[]byte(syscontract.SystemContract_CHAIN_CONFIG.String()))
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return errors.New("ChainConfig is empty")
	}
	var chainConfig config.ChainConfig
	err = proto.Unmarshal(bytes, &chainConfig)
	if err != nil {
		return err
	}

	c.ChainConf = &chainConfig

	// compatible with versions before v1.1.1
	if c.ChainConf.Contract == nil {
		c.ChainConf.Contract = &config.ContractConfig{EnableSqlSupport: false} //by default disable sql support
	}

	//
	if c.ChainConf.Core.ConsensusTurboConfig == nil {
		c.ChainConf.Core.ConsensusTurboConfig = &config.ConsensusTurboConfig{
			ConsensusMessageTurbo: false,
			RetryTime:             0,
			RetryInterval:         0,
		}
	}

	return nil
}

// GetChainConfigFromFuture get a future chain config.
func (c *ChainConf) GetChainConfigFromFuture(futureBlockHeight uint64) (*config.ChainConfig, error) {
	c.log.Debugf("GetChainConfig from futureBlockHeiht", "futureBlockHeight", futureBlockHeight)
	if futureBlockHeight > 0 {
		futureBlockHeight--
	}
	return c.GetChainConfigAt(futureBlockHeight)
}

// GetChainConfigAt get the lasted block info of chain config.
// The blockHeight must exist in store.
// If it is a config block , return the current config info.
func (c *ChainConf) GetChainConfigAt(blockHeight uint64) (*config.ChainConfig, error) {
	// get the special blockHeader by height
	header, err := c.getBlockHeaderFromStore(blockHeight)
	if err != nil {
		return nil, err
	}

	var configHeight uint64
	if header.BlockType == common.BlockType_CONFIG_BLOCK {
		var block *common.Block
		block, err = c.getConfigBlockFromStore(blockHeight)
		if err != nil {
			return nil, err
		}

		if utils.IsConfBlock(block) {
			return c.getConfig(block.Txs[0])
		}
	}
	// get latest config block
	configHeight = header.PreConfHeight
	configBlock, err := c.getConfigBlockFromStore(configHeight)
	if err != nil {
		return nil, err
	}

	return c.getConfigFromBlock(configBlock)
}

//getConfigFromBlock parse block, get ChainConfig from block
func (c *ChainConf) getConfigFromBlock(block *common.Block) (*config.ChainConfig, error) {
	txConfig := block.Txs[0]
	if !utils.IsValidConfigTx(txConfig) {
		c.log.Errorf("tx(id: %s) is not config tx", txConfig.Payload.TxId)
		return nil, errors.New("tx is not config tx")
	}

	return c.getConfig(txConfig)
}

//getConfig parse Tx, get ChainConfig struct
func (c *ChainConf) getConfig(tx *common.Transaction) (*config.ChainConfig, error) {
	result := tx.Result.ContractResult.Result
	chainConfig := &config.ChainConfig{}
	err := proto.Unmarshal(result, chainConfig)
	if err != nil {
		return nil, err
	}

	return chainConfig, nil
}

//getBlockHeaderFromStore retrieve block header from cache or store by height
func (c *ChainConf) getBlockHeaderFromStore(blockHeight uint64) (*common.BlockHeader, error) {
	if value, ok := c.lru.Get(blockHeight); ok {
		header, ok := value.(*common.BlockHeader)
		if ok {
			return header, nil
		}
		return nil, fmt.Errorf("not common.BlockHeader type")
	}
	header, err := c.blockchainStore.GetBlockHeaderByHeight(blockHeight)
	if err != nil {
		log.Errorf("get block header(height: %d) from store failed, %s", blockHeight, err)
		return nil, err
	}
	c.lru.Add(blockHeight, header)
	return header, err
}

//getConfigBlockFromStore retrieve block from  cache or block store by height
func (c *ChainConf) getConfigBlockFromStore(blockHeight uint64) (*common.Block, error) {
	var block *common.Block
	var err error
	//load from cache
	if value, ok := c.configLru.Get(blockHeight); ok {
		block, _ = value.(*common.Block)
		return block, err
	}
	//load from store
	block, err = c.blockchainStore.GetBlock(blockHeight)
	if err != nil {
		log.Errorf("get block(height: %d) from store failed, %s", blockHeight, err)
		return nil, err
	}
	//add to cache
	c.configLru.Add(blockHeight, block)
	return block, err
}

// ChainConfig return the chain config.
func (c *ChainConf) ChainConfig() *config.ChainConfig {
	c.cLock.RLock()
	defer c.cLock.RUnlock()
	return c.ChainConf
}

// SetChainConfig set safe chain config
func (c *ChainConf) SetChainConfig(chainConf *config.ChainConfig) error {
	c.cLock.Lock()
	defer c.cLock.Unlock()
	c.ChainConf = chainConf
	return nil
}

// GetConsensusNodeIdList return the node id list of all consensus node.
func (c *ChainConf) GetConsensusNodeIdList() ([]string, error) {
	chainNodeList := make([]string, 0)
	for _, node := range c.ChainConf.Consensus.Nodes {
		chainNodeList = append(chainNodeList, node.NodeId...)
	}
	c.log.Debugf("consensus node id list: %v", chainNodeList)
	return chainNodeList, nil
}

// CompleteBlock complete the block. Invoke all config watchers.
func (c *ChainConf) CompleteBlock(block *common.Block) error {
	if block == nil {
		c.log.Error(blockEmptyErrorTemplate)
		return errBlockEmpty
	}
	if block.Txs == nil || len(block.Txs) == 0 {
		return nil
	}
	tx := block.Txs[0]

	c.wLock.Lock()
	defer c.wLock.Unlock()

	if utils.IsValidConfigTx(tx) || utils.HasDPosTxWritesInHeader(block, c) { // tx is ChainConfig
		// watch ChainConfig
		if err := c.callbackChainConfigWatcher(); err != nil {
			return err
		}
	}

	// watch native contract
	contract, ok := IsNativeTxSucc(tx)
	if ok {
		// is native tx
		// callback the watcher by sync
		payloadData, _ := tx.Payload.Marshal()
		if err := c.callbackContractVmWatcher(contract, payloadData); err != nil {
			return err
		}
	}

	return nil
}

//callbackChainConfigWatcher  when complete the config block, call back the ChainConfig Watcher
func (c *ChainConf) callbackChainConfigWatcher() error {
	err := c.latestChainConfig()
	if err != nil {
		return err
	}
	// callback the watcher by sync
	for _, w := range c.watchers {
		err = w.Watch(c.ChainConf)
		if err != nil {
			c.log.Errorw("chainConf notify err", "module", w.Module(), "err", err)
			return err
		}
	}
	return nil
}

//callbackContractVmWatcher when complete the config block, call back VM Watcher
func (c *ChainConf) callbackContractVmWatcher(contract string, requestPayload []byte) error {
	// watch the all contract
	if vmWatchers, ok := c.vmWatchers[allContract]; ok {
		for m, w := range vmWatchers {
			err := w.Callback(contract, requestPayload)
			if err != nil {
				c.log.Errorf("vm watcher callback failed(contract: %s, module: %s), %s", contract, m, err)
				return err
			}
		}
	}

	// watch some contract
	if vmWatchers, ok := c.vmWatchers[contract]; ok {
		for m, w := range vmWatchers {
			err := w.Callback(contract, requestPayload)
			if err != nil {
				c.log.Errorf("vm watcher callback failed(contract: %s, module: %s), %s", contract, m, err)
				return err
			}
		}
	}
	return nil
}

// AddWatch register a config watcher.
func (c *ChainConf) AddWatch(w protocol.Watcher) {
	c.wLock.Lock()
	defer c.wLock.Unlock()
	for _, watcher := range c.watchers {
		if watcher.Module() == w.Module() {
			c.log.Errorf("chainconfig watcher existed(module: %s)", w.Module())
			return
		}
	}
	c.watchers = append(c.watchers, w)
}

// AddVmWatch add vm watcher
func (c *ChainConf) AddVmWatch(w protocol.VmWatcher) {
	c.wLock.Lock()
	defer c.wLock.Unlock()
	if w != nil {
		contractNames := w.ContractNames()
		if contractNames == nil {
			// watch all contract
			c.addVmWatcherWithAllContract(w)
		} else {
			c.addVmWatcherWithContracts(w)
		}
	}
}

//addVmWatcherWithAllContract add VM Watcher
func (c *ChainConf) addVmWatcherWithAllContract(w protocol.VmWatcher) {
	watchers, ok := c.vmWatchers[allContract]
	if !ok {
		watchers = make([]protocol.VmWatcher, 0)
	}

	for _, watcher := range watchers {
		if watcher.Module() == w.Module() {
			c.log.Errorf("vm watcher existed(contract: %s, module: %s)", allContract, w.Module())
			return
		}
	}

	watchers = append(watchers, w)
	c.vmWatchers[allContract] = watchers
}

//addVmWatcherWithContracts add VM watcher
func (c *ChainConf) addVmWatcherWithContracts(w protocol.VmWatcher) {
	for _, contractName := range w.ContractNames() {
		watchers, ok := c.vmWatchers[contractName]
		if !ok {
			watchers = make([]protocol.VmWatcher, 0)
		}

		for _, watcher := range watchers {
			if watcher.Module() == w.Module() {
				c.log.Errorf("vm watcher existed(contract: %s, module: %s)", contractName, w.Module())
				return
			}
		}
		watchers = append(watchers, w)
		c.vmWatchers[contractName] = watchers
	}
}
