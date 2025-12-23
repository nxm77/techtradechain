/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package techtradechain_sdk_go define techtradechain sdk go
package techtradechain_sdk_go

import (
	"context"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	bcx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/pb-go/v2/api"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	errStringFormat       = "%s failed, %s"
	sdkErrStringFormat    = "[SDK] %s"
	errGetTxResultTimeout = "get tx results timed out, " +
		"please query this txid later for the tx results, check sdk log file for more details"
)

var _ SDKInterface = (*ChainClient)(nil)

// ChainClient define techtradechain chain client to interact with node
type ChainClient struct {
	// common config
	config                  *ChainClientConfig
	logger                  utils.Logger
	pool                    ConnectionPool
	canonicalTxFetcherPools map[string]ConnectionPool
	txResultDispatcher      *txResultDispatcher
	chainId                 string
	orgId                   string

	userCrtBytes []byte
	userCrt      *bcx509.Certificate
	privateKey   crypto.PrivateKey
	publicKey    crypto.PublicKey
	pkBytes      []byte

	// cert hash config
	enabledCrtHash bool
	userCrtHash    []byte

	// archive config
	archiveConfig *ArchiveConfig

	// grpc client config
	rpcClientConfig *RPCClientConfig

	// pkcs11 config
	pkcs11Config *Pkcs11Config

	// kms config
	kmsConfig *KMSConfig

	hashType crypto.HashType
	authType AuthType
	// 节点是否只支持异步方式，老版本底链不支持同步返回，也就是新版本默认都是false
	nodeOnlyAsync bool
	// retry config
	retryLimit    int
	retryInterval int

	// alias support
	enabledAlias bool
	alias        string

	// default TimestampKey , true NormalKey support
	enableNormalKey bool

	// enable tx result dispatcher
	enableTxResultDispatcher bool
	// enable sync canonical tx result
	enableSyncCanonicalTxResult bool

	ConfigModel *utils.ChainClientConfigModel
	// archiveCenterQueryFirst 查询的时候，先查询归档中心，后查询节点数据库
	archiveCenterQueryFirst bool
	// archiveService 归档服务
	archiveService ArchiveService
}

// NewNodeConfig new node config, returns *NodeConfig
func NewNodeConfig(opts ...NodeOption) *NodeConfig {
	config := &NodeConfig{}
	for _, opt := range opts {
		opt(config)
	}

	return config
}

// NewConnPoolWithOptions new conn pool with optioins, returns *ClientConnectionPool
func NewConnPoolWithOptions(opts ...ChainClientOption) (*ClientConnectionPool, error) {
	config, err := generateConfig(opts...)
	if err != nil {
		return nil, err
	}
	return NewConnPool(config)
}

// NewArchiveConfig new archive config
func NewArchiveConfig(opts ...ArchiveOption) *ArchiveConfig {
	config := &ArchiveConfig{}
	for _, opt := range opts {
		opt(config)
	}

	return config
}

// NewRPCClientConfig new rpc client config
func NewRPCClientConfig(opts ...RPCClientOption) *RPCClientConfig {
	config := &RPCClientConfig{}
	for _, opt := range opts {
		opt(config)
	}
	return config
}

// NewPkcs11Config new pkcs11 config
func NewPkcs11Config(enabled bool, typ, libPath, label, password string,
	sessionCacheSize int, hashAlgo string) *Pkcs11Config {
	return &Pkcs11Config{
		Enabled:          enabled,
		Type:             typ,
		Library:          libPath,
		Label:            label,
		Password:         password,
		SessionCacheSize: sessionCacheSize,
		Hash:             hashAlgo,
	}
}

// NewKMSConfig new kms config
func NewKMSConfig(enabled bool, isPublic bool, secretId, secretKey,
	serverAddress, serverRegion, sdkScheme, extParams string) *KMSConfig {
	return &KMSConfig{
		Enabled:   enabled,
		IsPublic:  isPublic,
		SecretId:  secretId,
		SecretKey: secretKey,
		Address:   serverAddress,
		Region:    serverRegion,
		SdkScheme: sdkScheme,
		ExtParams: extParams,
	}
}

// NewCryptoConfig 根据传入参数创建新的CryptoConfig对象
// @param opts
// @return *CryptoConfig
func NewCryptoConfig(opts ...CryptoOption) *CryptoConfig {
	config := &CryptoConfig{}
	for _, opt := range opts {
		opt(config)
	}
	return config
}

// NewArchiveCenterConfig 根据传入参数创建新的归档中心对象
// @param httpUrl
// @param chainGenesisHash
// @param requestSecondLimit
// @return *ArchiveCenterHttpConfig
func NewArchiveCenterConfig(httpUrl, rpcUrl, chainGenesisHash string,
	requestSecondLimit, maxRpcSend, maxRpcRecv int, tlsEnable bool, tlsCfg utils.TlsConfig) *ArchiveCenterConfig {
	ret := &ArchiveCenterConfig{
		ChainGenesisHash:     chainGenesisHash,
		ArchiveCenterHttpUrl: httpUrl,
		RpcAddress:           rpcUrl,
		ReqeustSecondLimit:   httpRequestDuration,
		MaxSendMsgSize:       archiveCenterRpcMaxMsgSize,
		MaxRecvMsgSize:       archiveCenterRpcMaxMsgSize,
		TlsEnable:            tlsEnable,
		Tls:                  tlsCfg,
	}
	if requestSecondLimit > 0 {
		ret.ReqeustSecondLimit = requestSecondLimit
	}
	if maxRpcRecv > 0 {
		ret.MaxRecvMsgSize = maxRpcRecv
	}
	if maxRpcSend > 0 {
		ret.MaxRecvMsgSize = maxRpcRecv
	}
	return ret
}

// NewChainClient new chain client
func NewChainClient(opts ...ChainClientOption) (*ChainClient, error) {
	config, err := generateConfig(opts...)
	if err != nil {
		return nil, err
	}
	pool, err := NewConnPool(config)
	if err != nil {
		return nil, err
	}

	var hashType crypto.HashType
	// 表示没有配置，则根据情况来获取
	if config.hashType == 0 {
		// 对于public和pwk模式，会有特定的配置
		if config.authType == PermissionedWithKey || config.authType == Public {
			hashType = crypto.HashAlgoMap[config.crypto.hash]
		} else {
			// 对于cert模式，则使用的是sign证书中的配置
			hashType, err = bcx509.GetHashFromSignatureAlgorithm(config.userCrt.SignatureAlgorithm)
			if err != nil {
				return nil, err
			}
		}
	} else {
		// 使用特定的hash配置
		hashType = config.hashType
	}

	var publicKey crypto.PublicKey
	var pkBytes []byte
	var pkPem string
	publicKey = config.userPk
	pkPem, err = publicKey.String()
	if err != nil {
		return nil, err
	}
	pkBytes = []byte(pkPem)

	cc := &ChainClient{
		config:          config,
		pool:            pool,
		logger:          config.logger,
		chainId:         config.chainId,
		orgId:           config.orgId,
		alias:           config.alias,
		userCrtBytes:    config.userSignCrtBytes,
		userCrt:         config.userCrt,
		privateKey:      config.privateKey,
		archiveConfig:   config.archiveConfig,
		rpcClientConfig: config.rpcClientConfig,
		pkcs11Config:    config.pkcs11Config,
		kmsConfig:       config.kmsConfig,

		publicKey: publicKey,
		hashType:  hashType,
		authType:  config.authType,
		pkBytes:   pkBytes,

		nodeOnlyAsync: config.nodeOnlyAsync,
		retryLimit:    config.retryLimit,
		retryInterval: config.retryInterval,

		enableNormalKey:             config.enableNormalKey,
		enableTxResultDispatcher:    config.enableTxResultDispatcher,
		enableSyncCanonicalTxResult: config.enableSyncCanonicalTxResult,

		ConfigModel:             config.ConfigModel,
		archiveCenterQueryFirst: config.archiveCenterQueryFirst,
	}

	// 若设置了别名，便启用
	if config.authType == PermissionedWithCert && len(cc.alias) > 0 {
		if err := cc.EnableAlias(); err != nil {
			return nil, err
		}
	}

	// 启动 异步订阅交易结果
	if cc.enableTxResultDispatcher {
		cc.txResultDispatcher, err = newTxResultDispatcher(cc)
		if err != nil {
			return nil, err
		}
		go cc.txResultDispatcher.start()
	}
	if cc.enableSyncCanonicalTxResult {
		cc.canonicalTxFetcherPools, err = NewCanonicalTxFetcherPools(config)
		if err != nil {
			return nil, err
		}
	}
	if cc.config.optimizeDetection > 0 && len(cc.config.nodeList) > 1 {
		// 启动乐观检查处理
		cc.pool.startOptimizeDetection(context.Background(), cc.config.optimizeDetection, cc)
	}
	return cc, nil
}

// NewChainClientWithoutKey new chain client without sign key
func NewChainClientWithoutKey(opts ...ChainClientOption) (*ChainClient, error) {
	config, err := generateConfigWithoutKey(opts...)
	if err != nil {
		return nil, err
	}
	pool, err := NewConnPool(config)
	if err != nil {
		return nil, err
	}

	var hashType crypto.HashType
	// 表示没有配置，则根据情况来获取
	// 建议在withoutKey中使用WithHashType函数设置
	if config.hashType == 0 {
		// 对于public和pwk模式，会有特定的配置
		if config.authType == PermissionedWithKey || config.authType == Public {
			hashType = crypto.HashAlgoMap[config.crypto.hash]
		} else {
			// cert模式下建议使用WithHashType函数设置
			return nil, fmt.Errorf("hash type is not legal, u can set with option or config file")
		}
	} else {
		// 使用特定的hash配置
		hashType = config.hashType
	}

	cc := &ChainClient{
		config:  config,
		pool:    pool,
		logger:  config.logger,
		chainId: config.chainId,
		orgId:   config.orgId,
		alias:   config.alias,
		//userCrtBytes:    config.userSignCrtBytes,
		//userCrt:         config.userCrt,
		//privateKey:      config.privateKey,
		archiveConfig:   config.archiveConfig,
		rpcClientConfig: config.rpcClientConfig,
		pkcs11Config:    config.pkcs11Config,

		//publicKey: publicKey,
		hashType: hashType,
		authType: config.authType,
		//pkBytes:   pkBytes,

		retryLimit:    config.retryLimit,
		retryInterval: config.retryInterval,

		enableNormalKey:             config.enableNormalKey,
		enableTxResultDispatcher:    config.enableTxResultDispatcher,
		enableSyncCanonicalTxResult: config.enableSyncCanonicalTxResult,

		ConfigModel:             config.ConfigModel,
		archiveCenterQueryFirst: config.archiveCenterQueryFirst,
	}

	// 若设置了别名，便启用
	if config.authType == PermissionedWithCert && len(cc.alias) > 0 {
		if err := cc.EnableAlias(); err != nil {
			return nil, err
		}
	}

	// 启动 异步订阅交易结果
	if cc.enableTxResultDispatcher {
		cc.txResultDispatcher, err = newTxResultDispatcher(cc)
		if err != nil {
			return nil, err
		}
		go cc.txResultDispatcher.start()
	} else if cc.enableSyncCanonicalTxResult {
		cc.canonicalTxFetcherPools, err = NewCanonicalTxFetcherPools(config)
		if err != nil {
			return nil, err
		}
	}
	return cc, nil
}

// NewOptimizeChainClientWithoutKey new chain client without sign key
func NewOptimizeChainClientWithoutKey(trCreator TxRequestCreator, opts ...ChainClientOption) (*ChainClient, error) {
	config, err := generateConfigWithoutKey(opts...)
	if err != nil {
		return nil, err
	}

	pool, err := NewConnPool(config)
	if err != nil {
		return nil, err
	}
	// 使用自定义的配置，即调用
	var hashType crypto.HashType
	// 表示没有配置，则根据情况来获取
	// 建议在withoutKey中使用WithHashType函数设置
	if config.hashType == 0 {
		// 对于public和pwk模式，会有特定的配置
		if config.authType == PermissionedWithKey || config.authType == Public {
			hashType = crypto.HashAlgoMap[config.crypto.hash]
		} else {
			// cert模式下建议使用WithHashType函数设置
			return nil, fmt.Errorf("hash type is not legal, u can set with option or config file")
		}
	} else {
		// 使用特定的hash配置
		hashType = config.hashType
	}

	cc := &ChainClient{
		config:  config,
		pool:    pool,
		logger:  config.logger,
		chainId: config.chainId,
		orgId:   config.orgId,
		alias:   config.alias,
		//userCrtBytes:    config.userSignCrtBytes,
		//userCrt:         config.userCrt,
		//privateKey:      config.privateKey,
		archiveConfig:   config.archiveConfig,
		rpcClientConfig: config.rpcClientConfig,
		pkcs11Config:    config.pkcs11Config,

		//publicKey: publicKey,
		hashType: hashType,
		authType: config.authType,
		//pkBytes:   pkBytes,

		nodeOnlyAsync: config.nodeOnlyAsync,
		retryLimit:    config.retryLimit,
		retryInterval: config.retryInterval,

		enableNormalKey:             config.enableNormalKey,
		enableTxResultDispatcher:    config.enableTxResultDispatcher,
		enableSyncCanonicalTxResult: config.enableSyncCanonicalTxResult,

		ConfigModel:             config.ConfigModel,
		archiveCenterQueryFirst: config.archiveCenterQueryFirst,
	}

	// 若设置了别名，便启用
	if config.authType == PermissionedWithCert && len(cc.alias) > 0 {
		if err := cc.EnableAlias(); err != nil {
			return nil, err
		}
	}

	// 启动 异步订阅交易结果
	if cc.enableTxResultDispatcher {
		cc.txResultDispatcher, err = newTxResultDispatcher(cc)
		if err != nil {
			return nil, err
		}
		go cc.txResultDispatcher.start()
	} else if cc.enableSyncCanonicalTxResult {
		cc.canonicalTxFetcherPools, err = NewCanonicalTxFetcherPools(config)
		if err != nil {
			return nil, err
		}
	}
	if cc.config.optimizeDetection > 0 && len(cc.config.nodeList) > 1 {
		// 启动乐观检查处理
		cc.pool.startOptimizeDetection(context.Background(), cc.config.optimizeDetection, trCreator)
	}

	return cc, nil
}

// GetArchiveService 获取归档中心服务 lazy load
// @return ArchiveService
func (cc *ChainClient) GetArchiveService() ArchiveService {
	if cc.archiveService == nil {
		config := cc.config
		// 若设置了归档中心配置,则启用
		if config.archiveConfig == nil {
			panic("archive config is not set")
		}
		if config.archiveConfig.archiveType == "mysql" {
			cc.archiveService = NewArchiveMySqlClient(config.chainId, config.archiveConfig, cc, cc.logger)
		} else if config.archiveCenterConfig != nil {
			if len(config.archiveCenterConfig.RpcAddress) > 0 {
				cc.archiveService = NewArchiveCenterGrpcClient(config.archiveCenterConfig, cc.logger)
			} else if len(config.archiveCenterConfig.ArchiveCenterHttpUrl) > 0 {
				cc.archiveService = NewArchiveCenterHttpClient(config.archiveCenterConfig, cc.logger)
			} else {
				panic("archive center config is not set")
			}
		} else {
			panic("archive config is not set")
		}
	}
	return cc.archiveService
}

// IsEnableNormalKey whether to use normal key
func (cc *ChainClient) IsEnableNormalKey() bool {
	return cc.enableNormalKey
}

// IsArchiveCenterQueryFist check whether to query archive data from archivecenter first
func (cc *ChainClient) IsArchiveCenterQueryFist() bool {
	return cc.archiveCenterQueryFirst
}

// Stop stop chain client
func (cc *ChainClient) Stop() error {
	if cc.archiveConfig != nil && cc.archiveService != nil {
		cc.GetArchiveService().Close()
	}
	if cc.txResultDispatcher != nil {
		cc.txResultDispatcher.stop()
	}
	for _, pool := range cc.canonicalTxFetcherPools {
		pool.Close()
	}
	return cc.pool.Close()
}

func (cc *ChainClient) newAccessMember() *accesscontrol.Member {
	var member *accesscontrol.Member
	if cc.authType == PermissionedWithCert {
		if cc.enabledAlias && len(cc.alias) > 0 {
			member = &accesscontrol.Member{
				OrgId:      cc.orgId,
				MemberInfo: []byte(cc.alias),
				MemberType: accesscontrol.MemberType_ALIAS,
			}
		} else if cc.enabledCrtHash && len(cc.userCrtHash) > 0 {
			member = &accesscontrol.Member{
				OrgId:      cc.orgId,
				MemberInfo: cc.userCrtHash,
				MemberType: accesscontrol.MemberType_CERT_HASH,
			}
		} else {
			member = &accesscontrol.Member{
				OrgId:      cc.orgId,
				MemberInfo: cc.userCrtBytes,
				MemberType: accesscontrol.MemberType_CERT,
			}
		}
	} else {
		member = &accesscontrol.Member{
			OrgId:      cc.orgId,
			MemberInfo: cc.pkBytes,
			MemberType: accesscontrol.MemberType_PUBLIC_KEY,
		}
	}
	return member
}

// GenerateTxRequest sign payload and generate *common.TxRequest
func (cc *ChainClient) GenerateTxRequest(payload *common.Payload,
	endorsers []*common.EndorsementEntry) (*common.TxRequest, error) {
	return cc.GenerateTxRequestWithPayer(payload, endorsers, nil)
}

// GenerateTxRequestWithPayer sign payload and generate *common.TxRequest with payer
func (cc *ChainClient) GenerateTxRequestWithPayer(payload *common.Payload,
	endorsers []*common.EndorsementEntry, payer *common.EndorsementEntry) (*common.TxRequest, error) {
	req := &common.TxRequest{
		Payload: payload,
		Sender: &common.EndorsementEntry{
			Signer: cc.newAccessMember(),
		},
		Endorsers: endorsers,
		Payer:     payer,
	}
	var err error
	req.Sender.Signature, err = utils.SignPayloadWithHashType(cc.privateKey, cc.hashType, payload)
	if err != nil {
		return nil, err
	}
	return req, nil
}

// GenerateTxRequestBySigner sign payload and generate *common.TxRequest
// use signer to sign payload if it is not nil.
// use cc.privateKey to sign payload if signer is nil.
func (cc *ChainClient) GenerateTxRequestBySigner(payload *common.Payload, endorsers []*common.EndorsementEntry,
	signer Signer) (*common.TxRequest, error) {
	if signer == nil {
		return cc.GenerateTxRequest(payload, endorsers)
	}

	sig, err := signer.Sign(payload)
	if err != nil {
		return nil, err
	}

	member, err := signer.NewMember()
	if err != nil {
		return nil, err
	}

	return &common.TxRequest{
		Payload: payload,
		Sender: &common.EndorsementEntry{
			Signer:    member,
			Signature: sig,
		},
		Endorsers: endorsers,
	}, nil
}

func (cc *ChainClient) proposalRequest(payload *common.Payload, endorsers []*common.EndorsementEntry,
	payer *common.EndorsementEntry, timeout int64, syncResult bool) (*common.TxResponse, error) {

	req, err := cc.GenerateTxRequestWithPayer(payload, endorsers, payer)
	if err != nil {
		return nil, err
	}
	return cc.sendRequest(req, timeout, syncResult)
}

// sendRequest send tx request for universal use
func (cc *ChainClient) sendRequest(req *common.TxRequest, timeout int64, syncResult bool) (
	*common.TxResponse, error) {
	if timeout <= 0 {
		if req.Payload.TxType == common.TxType_QUERY_CONTRACT {
			timeout = cc.rpcClientConfig.rpcClientGetTxTimeout
		} else {
			timeout = cc.rpcClientConfig.rpcClientSendTxTimeout
		}
	}

	// canonical query
	if cc.enableSyncCanonicalTxResult && req.Payload.TxType == common.TxType_QUERY_CONTRACT {
		return cc.queryCanonical(req, timeout)
	}
	// invoke tx and sync result use tx result dispatcher
	if cc.enableTxResultDispatcher && req.Payload.TxType == common.TxType_INVOKE_CONTRACT {
		resp, err := cc.sendTxRequestWithSyncResult(req, timeout, false)
		if err != nil {
			return resp, err
		}
		if resp.Code == common.TxStatusCode_SUCCESS {
			if syncResult {
				r, err := cc.asyncTxResult(req.Payload.TxId, timeout)
				if err != nil {
					return nil, fmt.Errorf("asyncTxResult failed, %s, txid: %s, %s", err.Error(),
						req.Payload.TxId, errGetTxResultTimeout)
				}
				resp.Code = r.Result.Code
				resp.Message = r.Result.Message
				resp.ContractResult = r.Result.ContractResult
				resp.TxId = req.Payload.TxId
				resp.TxTimestamp = r.TxTimestamp
				resp.TxBlockHeight = r.TxBlockHeight
			}
		}
		return resp, nil
	}
	// normal tx request, query/invoke etc.
	// result will be included in response if tx is TxType_INVOKE_CONTRACT and syncResult is true, otherwise not
	return cc.sendTxRequestWithSyncResult(req, timeout, syncResult)
}

// sendTxRequestWithSyncResult normal tx request, query/invoke etc.
// result will be included in response if tx is invoke and syncResult is true, otherwise not
func (cc *ChainClient) sendTxRequestWithSyncResult(req *common.TxRequest, timeout int64, syncResult bool) (
	*common.TxResponse, error) {

	var (
		resp *common.TxResponse
		// polling result compatible.
		// if syncResult is true, and polling config is set, and tx is TxType_INVOKE_CONTRACT then polling tx result
		// add nodeOnlyAsync for async get response, will get resp by txid when nodeOnlyAsync is true
		pollingResultCompatible = syncResult && cc.retryLimit > 0 && cc.retryInterval > 0 && cc.nodeOnlyAsync &&
			req.Payload.TxType == common.TxType_INVOKE_CONTRACT
		ignoreAddrs = make(map[string]struct{})
	)

	for {
		netCli, err := cc.pool.getClientWithIgnoreAddrs(ignoreAddrs)
		if err != nil {
			return nil, err
		}

		if len(ignoreAddrs) > 0 {
			cc.logger.Debugf("[SDK] begin try to connect node [%s]", netCli.ID)
		}
		if !syncResult {
			// send request async
			resp, err = netCli.sendRequest(req, timeout)
		} else if pollingResultCompatible {
			// polling result compatible
			resp, err = netCli.sendRequest(req, timeout)
		} else {
			// send request sync
			resp, err = netCli.sendRequestSync(req, timeout)
		}
		if err != nil {
			statusErr, ok := status.FromError(err)
			if ok {
				if statusErr.Code() == codes.Unavailable {
					cc.logger.Errorf("[SDK] request to [%s] failed %s, try to connect another node",
						netCli.ID, err.Error())
					ignoreAddrs[netCli.ID] = struct{}{}
					continue
				} else if statusErr.Code() == codes.Unknown &&
					strings.Contains(err.Error(), "malformed header: missing HTTP content-type") {
					netCli.conn.Close()
					conn, err := cc.pool.initGRPCConnect(netCli.nodeAddr, netCli.useTLS, netCli.caPaths,
						netCli.caCerts, netCli.tlsHostName)
					if err != nil {
						cc.pool.getLogger().Errorf("init grpc connection [%s] failed, %s",
							netCli.ID, err.Error())
						ignoreAddrs[netCli.ID] = struct{}{}
						continue
					}

					netCli.conn = conn
					netCli.rpcNode = api.NewRpcNodeClient(conn)
					continue
				}
			}

			cc.logger.Errorf("[SDK] sendRequest failed, %s", err)
			return &common.TxResponse{
				Code:    common.TxStatusCode_INTERNAL_ERROR,
				Message: err.Error(),
				TxId:    req.Payload.TxId,
			}, err
		}

		// polling result compatible. do polling tx result in another requests.
		if pollingResultCompatible && resp.Code == common.TxStatusCode_SUCCESS {
			r, err := cc.pollingTxResult(req.Payload.TxId)
			if err != nil {
				return resp, fmt.Errorf("pollingTxResult failed, %s, txid: %s, %s", err.Error(),
					req.Payload.TxId, errGetTxResultTimeout)
			}
			resp.Code = r.Result.Code
			resp.Message = r.Result.Message
			resp.ContractResult = r.Result.ContractResult
			resp.TxId = req.Payload.TxId
			resp.TxTimestamp = r.TxTimestamp
			resp.TxBlockHeight = r.TxBlockHeight
		}
		return resp, nil
	}
}

// EnableCertHash Cert Hash logic
func (cc *ChainClient) EnableCertHash() error {
	var (
		err error
	)

	// 优先使用别名，如果开启了别名，直接忽略压缩证书
	if cc.enabledAlias {
		return nil
	}

	if cc.GetAuthType() != PermissionedWithCert {
		return errors.New("cert hash is not supported")
	}

	// 0.已经启用压缩证书
	if cc.enabledCrtHash {
		return nil
	}

	// 1.如尚未获取证书Hash，便进行获取
	if len(cc.userCrtHash) == 0 {
		// 获取证书Hash
		cc.userCrtHash, err = cc.GetCertHash()
		if err != nil {
			errMsg := fmt.Sprintf("get cert hash failed, %s", err.Error())
			cc.logger.Errorf(sdkErrStringFormat, errMsg)
			return errors.New(errMsg)
		}
	}

	// 2.链上查询证书是否存在
	ok, err := cc.getCheckCertHash()
	if err != nil {
		errMsg := fmt.Sprintf("enable cert hash, get and check cert hash failed, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return errors.New(errMsg)
	}

	// 3.1 若证书已经上链，直接返回
	if ok {
		cc.enabledCrtHash = true
		return nil
	}

	// 3.2 若证书未上链，添加证书
	resp, err := cc.AddCert()
	if err != nil {
		errMsg := fmt.Sprintf("enable cert hash AddCert failed, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return errors.New(errMsg)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		errMsg := fmt.Sprintf("enable cert hash AddCert got invalid resp, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return errors.New(errMsg)
	}

	// 循环检查证书是否成功上链
	err = cc.checkUserCertOnChain()
	if err != nil {
		errMsg := fmt.Sprintf("check user cert on chain failed, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return errors.New(errMsg)
	}

	cc.enabledCrtHash = true

	return nil
}

// DisableCertHash disable cert hash logic
func (cc *ChainClient) DisableCertHash() error {
	cc.enabledCrtHash = false
	return nil
}

// GetEnabledCrtHash check whether the cert hash logic is enabled
func (cc *ChainClient) GetEnabledCrtHash() bool {
	return cc.enabledCrtHash
}

// GetUserCrtHash get user cert hash of cc
func (cc *ChainClient) GetUserCrtHash() []byte {
	return cc.userCrtHash
}

// GetHashType get hash type of cc
func (cc *ChainClient) GetHashType() crypto.HashType {
	return cc.hashType
}

// GetAuthType get auth type of cc
func (cc *ChainClient) GetAuthType() AuthType {
	return cc.authType
}

// GetPublicKey get public key of cc
func (cc *ChainClient) GetPublicKey() crypto.PublicKey {
	return cc.publicKey
}

// GetPrivateKey get private key of cc
func (cc *ChainClient) GetPrivateKey() crypto.PrivateKey {
	return cc.privateKey
}

// GetCertPEM get cert pem of cc
func (cc *ChainClient) GetCertPEM() []byte {
	return cc.userCrtBytes
}

// GetLocalCertAlias get local cert alias of cc
func (cc *ChainClient) GetLocalCertAlias() string {
	return cc.alias
}

// ChangeSigner change ChainClient siger. signerCrt passes nil in Public or PermissionedWithKey mode
// publicModeHashType must be set in Public mode else set to zero value.
func (cc *ChainClient) ChangeSigner(signerPrivKey crypto.PrivateKey, signerCrt *bcx509.Certificate,
	publicModeHashType crypto.HashType) error {
	signerPubKey := signerPrivKey.PublicKey()
	pkPem, err := signerPubKey.String()
	if err != nil {
		return err
	}

	cc.pkBytes = []byte(pkPem)
	cc.publicKey = signerPubKey
	cc.privateKey = signerPrivKey

	if signerCrt != nil {
		crtPem := pem.EncodeToMemory(&pem.Block{Bytes: signerCrt.Raw, Type: "CERTIFICATE"})
		cc.userCrtBytes = crtPem
		cc.userCrt = signerCrt
	} else {
		cc.hashType = publicModeHashType
	}
	return nil
}

// 检查证书是否成功上链
func (cc *ChainClient) checkUserCertOnChain() error {
	err := retry.Retry(func(uint) error {
		ok, err := cc.getCheckCertHash()
		if err != nil {
			errMsg := fmt.Sprintf("check user cert on chain, get and check cert hash failed, %s", err.Error())
			cc.logger.Errorf(sdkErrStringFormat, errMsg)
			return errors.New(errMsg)
		}

		if !ok {
			errMsg := "user cert havenot on chain yet, and try again"
			cc.logger.Debugf(sdkErrStringFormat, errMsg)
			return errors.New(errMsg)
		}

		return nil
	}, strategy.Limit(10), strategy.Wait(time.Second))

	if err != nil {
		errMsg := fmt.Sprintf("check user upload cert on chain failed, try again later, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return errors.New(errMsg)
	}

	return nil
}

func (cc *ChainClient) getCheckCertHash() (bool, error) {
	// 根据已缓存证书Hash，查链上是否存在
	certInfo, err := cc.QueryCert([]string{hex.EncodeToString(cc.userCrtHash)})
	if err != nil {
		errMsg := fmt.Sprintf("QueryCert failed, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return false, errors.New(errMsg)
	}

	if len(certInfo.CertInfos) == 0 {
		return false, nil
	}

	// 返回链上证书列表长度不为1，即报错
	if len(certInfo.CertInfos) > 1 {
		errMsg := "CertInfos != 1"
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return false, errors.New(errMsg)
	}

	// 如果链上证书Hash不为空
	if len(certInfo.CertInfos[0].Cert) > 0 {
		// 如果和缓存的证书Hash不一致则报错
		if hex.EncodeToString(cc.userCrtHash) != certInfo.CertInfos[0].Hash {
			errMsg := fmt.Sprintf("not equal certHash, [expected:%s]/[actual:%s]",
				cc.userCrtHash, certInfo.CertInfos[0].Hash)
			cc.logger.Errorf(sdkErrStringFormat, errMsg)
			return false, errors.New(errMsg)
		}

		// 如果和缓存的证书Hash一致，则说明已经上传好了证书，具备提交压缩证书交易的能力
		return true, nil
	}

	return false, nil
}

// Pkcs11Config get pkcs11 config of cc
// returns:
func (cc *ChainClient) Pkcs11Config() *Pkcs11Config {
	return cc.pkcs11Config
}

// KMSConfig get kms config of cc
// returns:
func (cc *ChainClient) KMSConfig() *KMSConfig {
	return cc.kmsConfig
}

// ArchiveCenterConfig 获取归档中心的配置信息
func (cc *ChainClient) ArchiveCenterConfig() *ArchiveCenterConfig {
	if cc.GetArchiveService() == nil {
		return nil
	}
	if archiveService, ok := cc.GetArchiveService().(*ArchiveCenterGrpcClient); ok {
		return archiveService.ArchiveCenterConfig()
	}
	if archiveService, ok := cc.GetArchiveService().(*ArchiveCenterHttpClient); ok {
		return archiveService.ArchiveCenterConfig()
	}
	return nil
}

// CreateChainClient create chain client and init chain client, returns *ChainClient
func CreateChainClient(pool ConnectionPool, userCrtBytes, privKey, userCrtHash []byte, orgId, chainId string,
	enabledCrtHash int) (*ChainClient, error) {
	cert, err := utils.ParseCert(userCrtBytes)
	if err != nil {
		return nil, err
	}

	priv, err := asym.PrivateKeyFromPEM(privKey, nil)
	if err != nil {
		return nil, err
	}

	chain := &ChainClient{
		pool:         pool,
		logger:       pool.getLogger(),
		chainId:      chainId,
		orgId:        orgId,
		userCrtBytes: userCrtBytes,
		userCrt:      cert,
		privateKey:   priv,
	}

	return chain, nil
}

// EnableAlias enable cert alias logic
func (cc *ChainClient) EnableAlias() error {
	var (
		err error
	)

	// 已经启用别名，直接返回
	if cc.enabledAlias {
		return nil
	}

	// 查询别名是否上链
	ok, err := cc.getCheckAlias()
	if err != nil {
		errMsg := fmt.Sprintf("enable alias, get and check alias failed, %s", err.Error())
		cc.logger.Debugf(sdkErrStringFormat, errMsg)
		//return errors.New(errMsg)
	}

	// 别名已上链
	if ok {
		cc.enabledAlias = true
		return nil
	}

	// 添加别名
	resp, err := cc.AddAlias()
	if err != nil {
		errMsg := fmt.Sprintf("enable alias AddAlias failed, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return errors.New(errMsg)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		errMsg := fmt.Sprintf("enable alias AddAlias got invalid resp, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return errors.New(errMsg)
	}

	// 循环检查别名是否成功上链
	err = cc.checkAliasOnChain()
	if err != nil {
		errMsg := fmt.Sprintf("check alias on chain failed, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return errors.New(errMsg)
	}

	cc.enabledAlias = true

	return nil
}

func (cc *ChainClient) getCheckAlias() (bool, error) {
	aliasInfos, err := cc.QueryCertsAlias([]string{cc.alias})
	if err != nil {
		errMsg := fmt.Sprintf("QueryCertsAlias failed, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return false, errors.New(errMsg)
	}

	if len(aliasInfos.AliasInfos) != 1 {
		return false, errors.New("alias not found")
	}

	if aliasInfos.AliasInfos[0].Alias != cc.alias {
		return false, errors.New("alias not equal")
	}

	if aliasInfos.AliasInfos[0].NowCert.Cert == nil {
		return false, errors.New("alias has been deleted")
	}

	return true, nil
}

func (cc *ChainClient) checkAliasOnChain() error {
	err := retry.Retry(func(uint) error {
		ok, err := cc.getCheckAlias()
		if err != nil {
			errMsg := fmt.Sprintf("check alias on chain, get and check alias failed, %s", err.Error())
			cc.logger.Errorf(sdkErrStringFormat, errMsg)
			return errors.New(errMsg)
		}

		if !ok {
			errMsg := "alias havenot on chain yet, and try again"
			cc.logger.Debugf(sdkErrStringFormat, errMsg)
			return errors.New(errMsg)
		}

		return nil
	}, strategy.Limit(10), strategy.Wait(time.Second))

	if err != nil {
		errMsg := fmt.Sprintf("check upload alias on chain failed, try again later, %s", err.Error())
		cc.logger.Errorf(sdkErrStringFormat, errMsg)
		return errors.New(errMsg)
	}

	return nil
}
