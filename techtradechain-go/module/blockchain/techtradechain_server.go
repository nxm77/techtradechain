/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockchain

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"techtradechain.com/techtradechain-go/module/bridge"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/kms"
	"techtradechain.com/techtradechain/common/v2/kmsutils"

	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/math"

	"techtradechain.com/techtradechain-go/module/net"
	"techtradechain.com/techtradechain-go/module/subscriber"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/common/v2/crypto/engine"
	"techtradechain.com/techtradechain/common/v2/helper"
	"techtradechain.com/techtradechain/common/v2/msgbus"
	localconf "techtradechain.com/techtradechain/localconf/v2"
	logger "techtradechain.com/techtradechain/logger/v2"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/txpool"
	protocol "techtradechain.com/techtradechain/protocol/v2"
)

var log = logger.GetLogger(logger.MODULE_BLOCKCHAIN)

const (
	chainIdNotFoundErrorTemplate = "chain id %s not found"
	notConsensusNode             = "the current node does not belong to the consensus role"
)

// TechTradeChainServer manage all blockchains
type TechTradeChainServer struct {
	// net shared by all chains
	net protocol.Net

	// blockchains known by this node
	blockchains sync.Map // map[string]*Blockchain

	readyC chan struct{}
}

// NewTechTradeChainServer create a new TechTradeChainServer instance.
func NewTechTradeChainServer() *TechTradeChainServer {
	return &TechTradeChainServer{}
}

// Init TechTradeChainServer.
func (server *TechTradeChainServer) Init() error {
	var err error
	log.Debug("begin init chain maker server...")
	server.readyC = make(chan struct{})
	// 1) init net
	if err = server.initNet(); err != nil {
		return err
	}
	// 2) init blockchains
	if err = server.initBlockchains(); err != nil {
		return err
	}
	log.Info("init chain maker server success!")
	return nil
}

// InitForRebuildDbs init TechTradeChainServer.
func (server *TechTradeChainServer) InitForRebuildDbs(chainId string) error {
	var err error
	log.Debug("begin init chain maker rebuild dbs server...")
	server.readyC = make(chan struct{})
	// 1) init net
	//if err = server.initNet(); err != nil {
	//	return err
	//}
	// 2) init blockchains
	if err = server.initBlockchainsForRebuildDbs(chainId); err != nil {
		return err
	}
	log.Info("init chain maker server success!")
	return nil
}
func (server *TechTradeChainServer) initNet() error {
	var netType protocol.NetType
	var err error
	// load net type
	provider := localconf.TechTradeChainConfig.NetConfig.Provider
	log.Infof("load net provider: %s", provider)
	switch strings.ToLower(provider) {
	case "libp2p":
		netType = protocol.Libp2p

	case "liquid":
		netType = protocol.Liquid
	default:
		return errors.New("unsupported net provider")
	}

	authType := localconf.TechTradeChainConfig.AuthType
	emptyAuthType := ""

	// load tls keys and cert path
	keyPath := localconf.TechTradeChainConfig.NetConfig.TLSConfig.PrivKeyFile
	if !filepath.IsAbs(keyPath) {
		keyPath, err = filepath.Abs(keyPath)
		if err != nil {
			return err
		}
	}
	log.Infof("load net tls key file path: %s", keyPath)

	var certPath string
	var pubKeyMode bool
	switch strings.ToLower(authType) {
	case protocol.PermissionedWithKey, protocol.Public:
		pubKeyMode = true
	case protocol.PermissionedWithCert, protocol.Identity, emptyAuthType:
		pubKeyMode = false
		certPath = localconf.TechTradeChainConfig.NetConfig.TLSConfig.CertFile
		if !filepath.IsAbs(certPath) {
			certPath, err = filepath.Abs(certPath)
			if err != nil {
				return err
			}
		}
		log.Infof("load net tls cert file path: %s", certPath)
	default:
		return errors.New("wrong auth type")
	}
	//gmtls enc key/cert
	encKeyPath, _ := filepath.Abs(localconf.TechTradeChainConfig.NetConfig.TLSConfig.PrivEncKeyFile)
	encCertPath, _ := filepath.Abs(localconf.TechTradeChainConfig.NetConfig.TLSConfig.CertEncFile)

	// new net
	var netFactory net.NetFactory
	server.net, err = netFactory.NewNet(
		netType,
		net.WithReadySignalC(server.readyC),
		net.WithListenAddr(localconf.TechTradeChainConfig.NetConfig.ListenAddr),
		net.WithCrypto(pubKeyMode, keyPath, certPath, encKeyPath, encCertPath),
		net.WithPeerStreamPoolSize(localconf.TechTradeChainConfig.NetConfig.PeerStreamPoolSize),
		net.WithMaxPeerCountAllowed(localconf.TechTradeChainConfig.NetConfig.MaxPeerCountAllow),
		net.WithPeerEliminationStrategy(localconf.TechTradeChainConfig.NetConfig.PeerEliminationStrategy),
		net.WithSeeds(localconf.TechTradeChainConfig.NetConfig.Seeds...),
		net.WithBlackAddresses(localconf.TechTradeChainConfig.NetConfig.BlackList.Addresses...),
		net.WithBlackNodeIds(localconf.TechTradeChainConfig.NetConfig.BlackList.NodeIds...),
		net.WithMsgCompression(localconf.TechTradeChainConfig.DebugConfig.UseNetMsgCompression),
		net.WithInsecurity(localconf.TechTradeChainConfig.DebugConfig.IsNetInsecurity),
		net.WithTlsEnabled(localconf.TechTradeChainConfig.NetConfig.TLSConfig.Enabled),
		net.WithPeerHttpTunnelTargetAddressList(localconf.TechTradeChainConfig.NetConfig.SeedTargetAddressList...),
		net.WithStunClient(localconf.TechTradeChainConfig.NetConfig.StunClient.ListenAddr,
			localconf.TechTradeChainConfig.NetConfig.StunClient.StunServerAddr,
			localconf.TechTradeChainConfig.NetConfig.StunClient.NetworkType,
			localconf.TechTradeChainConfig.NetConfig.StunClient.Enabled),
		net.WithStunServer(localconf.TechTradeChainConfig.NetConfig.StunServer.Enabled,
			localconf.TechTradeChainConfig.NetConfig.StunServer.TwoPublicAddress,
			localconf.TechTradeChainConfig.NetConfig.StunServer.OtherStunServerAddr,
			localconf.TechTradeChainConfig.NetConfig.StunServer.LocalNotifyAddr,
			localconf.TechTradeChainConfig.NetConfig.StunServer.OtherNotifyAddr,
			localconf.TechTradeChainConfig.NetConfig.StunServer.ListenAddr1,
			localconf.TechTradeChainConfig.NetConfig.StunServer.ListenAddr2,
			localconf.TechTradeChainConfig.NetConfig.StunServer.ListenAddr3,
			localconf.TechTradeChainConfig.NetConfig.StunServer.ListenAddr4,
			localconf.TechTradeChainConfig.NetConfig.StunServer.NetworkType),
		net.WithHolePunch(localconf.TechTradeChainConfig.NetConfig.EnablePunch),
	)
	if err != nil {
		errMsg := fmt.Sprintf("new net failed, %s", err.Error())
		log.Error(errMsg)
		return errors.New(errMsg)
	}

	// read key file, then set the NodeId of local config
	file, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return err
	}

	var privateKey crypto.PrivateKey
	kmsConfig := localconf.TechTradeChainConfig.NodeConfig.KMSConfig
	if kmsConfig.Enabled && pubKeyMode {
		if err = initKMS(); err != nil {
			return fmt.Errorf("fail to initialize identity management service: [%v]", err)
		}

		privateKey, err = kmsutils.ParseKMSPrivKey(file)
		if err != nil {
			return fmt.Errorf("fail to initialize identity management service: [%v]", err)
		}

	} else {
		privateKey, err = asym.PrivateKeyFromPEM(file, nil)
		if err != nil {
			return err
		}
	}

	nodeId, err := helper.CreateLibp2pPeerIdWithPrivateKey(privateKey)
	if err != nil {
		return err
	}
	localconf.TechTradeChainConfig.SetNodeId(nodeId)

	// load custom chain trust roots
	for _, chainTrustRoots := range localconf.TechTradeChainConfig.NetConfig.CustomChainTrustRoots {
		roots := make([][]byte, 0, len(chainTrustRoots.TrustRoots))
		for _, r := range chainTrustRoots.TrustRoots {
			rootBytes, err2 := ioutil.ReadFile(r.Root)
			if err2 != nil {
				log.Errorf("load custom chain trust roots failed, %s", err2.Error())
				return err2
			}
			roots = append(roots, rootBytes)
		}
		server.net.SetChainCustomTrustRoots(chainTrustRoots.ChainId, roots)
		log.Infof("set custom trust roots for chain[%s] success.", chainTrustRoots.ChainId)
	}
	return nil
}

// initKMS
// @Description: init kms context, only effect when kms enabled
// @return error
func initKMS() error {
	config := localconf.TechTradeChainConfig.NodeConfig.KMSConfig // todo 如果为nil
	if !config.Enabled {
		kmsEnable := os.Getenv("KMS_ENABLE")
		if strings.EqualFold(strings.ToLower(kmsEnable), "true") {
			config.SecretId = os.Getenv("KMS_SECRET_ID")
			config.SecretKey = os.Getenv("KMS_SECRET_KEY")
			config.Address = os.Getenv("KMS_ADDRESS")
			config.Region = os.Getenv("KMS_REGION")
			config.SdkScheme = os.Getenv("SMK_SDK_SCHEME")
			isPublicStr := os.Getenv("KMS_IS_PUBLIC")
			if strings.EqualFold(strings.ToLower(isPublicStr), "true") {
				config.Enabled = true
			}
		}
	}
	if !config.Enabled {
		return nil
	}
	kmsutils.InitKMS(kmsutils.KMSConfig{
		Enable: config.Enabled,
		Config: kms.Config{
			IsPublic:  config.IsPublic,
			SecretId:  config.SecretId,
			SecretKey: config.SecretKey,
			Address:   config.Address,
			Region:    config.Region,
			SDKScheme: config.SdkScheme,
		},
	})
	return nil
}

func (server *TechTradeChainServer) initBlockchains() error {
	server.blockchains = sync.Map{}
	ok := false
	for _, chain := range localconf.TechTradeChainConfig.GetBlockChains() {
		chainId := chain.ChainId
		if err := server.initBlockchain(chainId, chain.Genesis); err != nil {
			log.Error(err.Error())
			continue
		}
		ok = true
	}
	if !ok {
		return fmt.Errorf("init all blockchains fail")
	}
	go server.newBlockchainTaskListener()
	go server.deleteBlockchainTaskListener()
	return nil
}

func (server *TechTradeChainServer) initBlockchainsForRebuildDbs(chainId string) error {
	server.blockchains = sync.Map{}
	ok := false
	for _, chain := range localconf.TechTradeChainConfig.GetBlockChains() {
		if chainId == chain.ChainId {
			if err := server.initBlockchainForRebuildDbs(chainId, chain.Genesis); err != nil {
				return err
			}
			ok = true
		}
		//if err := server.initBlockchainForRebuildDbs(chainId, chain.Genesis); err != nil {
		//	log.Error(err.Error())
		//	continue
		//}
	}
	if !ok {
		return fmt.Errorf("init %s blockchains fail for not exists", chainId)
	}
	go server.newBlockchainTaskListener()
	go server.deleteBlockchainTaskListener()
	return nil
}

func (server *TechTradeChainServer) newBlockchainTaskListener() {
	for newChainId := range localconf.FindNewBlockChainNotifyC {
		_, ok := server.blockchains.Load(newChainId)
		if ok {
			log.Errorf("new block chain found existed(chain-id: %s)", newChainId)
			continue
		}
		log.Infof("new block chain found(chain-id: %s), start to init new block chain.", newChainId)
		for _, chain := range localconf.TechTradeChainConfig.GetBlockChains() {
			if chain.ChainId == newChainId {
				if err := server.initBlockchain(newChainId, chain.Genesis); err != nil {
					log.Error(err.Error())
					continue
				}
				// 通知Dispatcher更新
				bridge.RpcDispatcherChan <- newChainId
				newBlockchain, _ := server.blockchains.Load(newChainId)
				go startBlockchain(newBlockchain.(*Blockchain))
			}
		}
	}
}

func (server *TechTradeChainServer) deleteBlockchainTaskListener() {
	for deleteChainId := range localconf.FindDeleteBlockChainNotifyC {
		oldBlockChain, ok := server.blockchains.Load(deleteChainId)
		if !ok {
			log.Errorf("old block chain not found (chain-id: %s)", deleteChainId)
			continue
		}
		log.Infof("old block chain found(chain-id: %s), start to delete block chain.", deleteChainId)
		oldBlockChainS, _ := oldBlockChain.(*Blockchain)
		blockchainsCount := 0
		server.blockchains.Range(func(key, value any) bool {
			blockchainsCount++
			return true
		})
		// there will be still some blockchains left after delete the current one,
		// so we cannot stop vm module because that different blockchains share
		// grpc connections for some vm runtime types, such as docker-go, docker-java
		if blockchainsCount != 1 {
			oldBlockChainS.StopWithoutVm()
		} else {
			oldBlockChainS.Stop()
		}
		server.blockchains.Delete(deleteChainId)
		// 通知Dispatcher更新
		bridge.RpcDispatcherChan <- deleteChainId
	}
}

func (server *TechTradeChainServer) initBlockchain(chainId, genesis string) error {
	if !filepath.IsAbs(genesis) {
		var err error
		genesis, err = filepath.Abs(genesis)
		if err != nil {
			return err
		}
	}
	log.Infof("load genesis file path of chain[%s]: %s", chainId, genesis)
	blockchain := NewBlockchain(genesis, chainId, msgbus.NewMessageBus(), server.net)

	if err := blockchain.Init(); err != nil {
		errMsg := fmt.Sprintf("init blockchain[%s] failed, %s", chainId, err.Error())
		return errors.New(errMsg)
	}
	server.blockchains.Store(chainId, blockchain)
	log.Infof("init blockchain[%s] success!", chainId)
	return nil
}

func (server *TechTradeChainServer) initBlockchainForRebuildDbs(chainId, genesis string) error {
	if !filepath.IsAbs(genesis) {
		var err error
		genesis, err = filepath.Abs(genesis)
		if err != nil {
			return err
		}
	}
	log.Infof("load genesis file path of chain[%s]: %s", chainId, genesis)
	blockchain := NewBlockchain(genesis, chainId, msgbus.NewMessageBus(), server.net)
	if err := blockchain.InitForRebuildDbs(); err != nil {
		errMsg := fmt.Sprintf("init blockchain[%s] failed, %s", chainId, err.Error())
		return errors.New(errMsg)
	}
	server.blockchains.Store(chainId, blockchain)
	log.Infof("init blockchain[%s] success!", chainId)
	return nil
}
func startBlockchain(chain *Blockchain) {
	if err := chain.Start(); err != nil {
		log.Errorf("[Core] start blockchain[%s] failed, %s", chain.chainId, err.Error())
		os.Exit(-1)
	}
	log.Infof("[Core] start blockchain[%s] success", chain.chainId)
}
func startBlockchainForRebuildDbs(chain *Blockchain, needVerify bool) {
	if err := chain.StartForRebuildDbs(); err != nil {
		log.Errorf("[Core] start blockchain[%s] rebuild-dbs failed, %s", chain.chainId, err.Error())
		os.Exit(-1)
	}
	log.Infof("[Core] start blockchain[%s] rebuild-dbs success", chain.chainId)
	chain.RebuildDbs(needVerify)
}

// Start TechTradeChainServer.
func (server *TechTradeChainServer) Start() error {
	// 1) start Net
	if err := server.net.Start(); err != nil {
		log.Errorf("[Net] start failed, %s", err.Error())
		return err
	}
	log.Infof("[Net] start success!")

	//init crypto engine for ac
	engine.InitCryptoEngine(localconf.TechTradeChainConfig.CryptoEngine, false)

	// 2) start blockchains
	server.blockchains.Range(func(_, value interface{}) bool {
		chain, _ := value.(*Blockchain)
		go startBlockchain(chain)
		return true
	})
	// 3) ready
	close(server.readyC)
	return nil
}

// Start TechTradeChainServer for rebuild dbs.
func (server *TechTradeChainServer) StartForRebuildDbs(needVerify bool) error {
	// 1) start Net
	//if err := server.net.Start(); err != nil {
	//	log.Errorf("[Net] start failed, %s", err.Error())
	//	return err
	//}
	//log.Infof("[Net] start success!")
	// 2) start blockchains
	server.blockchains.Range(func(_, value interface{}) bool {
		chain, _ := value.(*Blockchain)
		go startBlockchainForRebuildDbs(chain, needVerify)
		return true
	})

	// 3) ready
	close(server.readyC)
	return nil
}

// Stop TechTradeChainServer.
func (server *TechTradeChainServer) Stop() {
	// stop all blockchains
	var wg sync.WaitGroup
	server.blockchains.Range(func(_, value interface{}) bool {
		chain, _ := value.(*Blockchain)
		wg.Add(1)
		go func(chain *Blockchain) {
			defer wg.Done()
			chain.Stop()
		}(chain)
		return true
	})
	wg.Wait()
	log.Info("TechTradeChain server is stopped!")

	// stop net
	if err := server.net.Stop(); err != nil {
		log.Errorf("stop net failed, %s", err.Error())
	}
	log.Info("net is stopped!")

}

// AddTx add a transaction.
func (server *TechTradeChainServer) AddTx(chainId string, tx *common.Transaction, source protocol.TxSource) error {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain).txPool.AddTx(tx, source)
	}
	return fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetPoolStatus Returns the max size of config transaction pool and common transaction pool,
// the num of config transaction in queue and pendingCache,
// and the the num of common transaction in queue and pendingCache.
func (server *TechTradeChainServer) GetPoolStatus(chainId string) (*txpool.TxPoolStatus, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain).txPool.GetPoolStatus(), nil
	}
	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetTxIdsByTypeAndStage Returns config or common txIds in different stage.
// txType may be TxType_CONFIG_TX, TxType_COMMON_TX, (TxType_CONFIG_TX|TxType_COMMON_TX)
// txStage may be TxStage_IN_QUEUE, TxStage_IN_PENDING, (TxStage_IN_QUEUE|TxStage_IN_PENDING)
func (server *TechTradeChainServer) GetTxIdsByTypeAndStage(chainId string, txType, txStage int32) ([]string, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain).txPool.GetTxIdsByTypeAndStage(txType, txStage), nil
	}
	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetTxsInPoolByTxIds Retrieve the transactions by the txIds from the txPool,
// return transactions in the txPool and txIds not in txPool.
// default query upper limit is 1w transaction, and error is returned if the limit is exceeded.
func (server *TechTradeChainServer) GetTxsInPoolByTxIds(chainId string,
	txIds []string) ([]*common.Transaction, []string, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain).txPool.GetTxsInPoolByTxIds(txIds)
	}
	return nil, nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetConsensusStateJSON Get the status of the current consensus, including the height and view
// of the block participating in the consensus, timeout, and identity of the consensus node
func (server *TechTradeChainServer) GetConsensusStateJSON(chainId string) ([]byte, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		if blockchain.(*Blockchain).consensus != nil {
			return blockchain.(*Blockchain).consensus.GetConsensusStateJSON()
		}
		return nil, fmt.Errorf(notConsensusNode)
	}
	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetConsensusValidators Get the identity of all consensus nodes
func (server *TechTradeChainServer) GetConsensusValidators(chainId string) ([]string, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		if blockchain.(*Blockchain).consensus != nil {
			return blockchain.(*Blockchain).consensus.GetValidators()
		}
		return nil, fmt.Errorf(notConsensusNode)
	}
	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetConsensusHeight Get the height of the block participating in the consensus
func (server *TechTradeChainServer) GetConsensusHeight(chainId string) (uint64, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		if blockchain.(*Blockchain).consensus != nil {
			height := blockchain.(*Blockchain).consensus.GetLastHeight()
			if height == math.MaxUint64 {
				// Because the interface provided by the consensus module does not provide
				// an error value when it returns. The consensus field in the MAXBFT consensus
				// is always non-empty although the synchronous node, the consensus module
				// internally maintains whether the node is a consensus node. In order to keep
				// consistent with other consensus behaviors (in the case of non-consensus nodes,
				// a specific error is returned), So MAXBFT Mandatory uint64 Maximum value
				// indicates that the current node is a non-consensus node.
				return 0, fmt.Errorf(notConsensusNode)
			}
			return height, nil
		}
		return 0, fmt.Errorf(notConsensusNode)
	}
	return 0, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetStore get the store instance of chain which id is the given.
func (server *TechTradeChainServer) GetStore(chainId string) (protocol.BlockchainStore, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain).store, nil
	}

	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetChainConf get protocol.ChainConf of chain which id is the given.
func (server *TechTradeChainServer) GetChainConf(chainId string) (protocol.ChainConf, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain).chainConf, nil
	}

	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetAllChainConf get all protocol.ChainConf of all the chains.
func (server *TechTradeChainServer) GetAllChainConf() ([]protocol.ChainConf, error) {
	var chainConfs []protocol.ChainConf
	server.blockchains.Range(func(_, value interface{}) bool {
		blockchain, _ := value.(*Blockchain)
		chainConfs = append(chainConfs, blockchain.chainConf)
		return true
	})

	if len(chainConfs) == 0 {
		return nil, fmt.Errorf("all chain not found")
	}

	return chainConfs, nil
}

// GetVmManager get protocol.VmManager of chain which id is the given.
func (server *TechTradeChainServer) GetVmManager(chainId string) (protocol.VmManager, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain).vmMgr, nil
	}

	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetEventSubscribe get subscriber.EventSubscriber of chain which id is the given.
func (server *TechTradeChainServer) GetEventSubscribe(chainId string) (*subscriber.EventSubscriber, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain).eventSubscriber, nil
	}

	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetNetService get protocol.NetService of chain which id is the given.
func (server *TechTradeChainServer) GetNetService(chainId string) (protocol.NetService, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain).netService, nil
	}

	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetBlockchain get Blockchain of chain which id is the given.
func (server *TechTradeChainServer) GetBlockchain(chainId string) (*Blockchain, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain), nil
	}

	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// GetAllAC get all protocol.AccessControlProvider of all the chains.
func (server *TechTradeChainServer) GetAllAC() ([]protocol.AccessControlProvider, error) {
	var accessControls []protocol.AccessControlProvider
	server.blockchains.Range(func(_, value interface{}) bool {
		blockchain, ok := value.(*Blockchain)
		if !ok {
			panic("invalid blockchain obj")
		}
		accessControls = append(accessControls, blockchain.GetAccessControl())
		return true
	})

	if len(accessControls) == 0 {
		return nil, fmt.Errorf("all chain not found")
	}

	return accessControls, nil
}

// GetSync get the sync instance of chain which id is the given.
func (server *TechTradeChainServer) GetSync(chainId string) (protocol.SyncService, error) {
	if blockchain, ok := server.blockchains.Load(chainId); ok {
		return blockchain.(*Blockchain).syncServer, nil
	}

	return nil, fmt.Errorf(chainIdNotFoundErrorTemplate, chainId)
}

// Version of techtradechain.
func (server *TechTradeChainServer) Version() string {
	return CurrentVersion
}
