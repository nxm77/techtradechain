/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"techtradechain.com/techtradechain/common/v2/kmsutils"

	"techtradechain.com/techtradechain/common/v2/crypto/kms"

	"techtradechain.com/techtradechain/common/v2/cert"
	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/common/v2/crypto/pkcs11"
	"techtradechain.com/techtradechain/common/v2/crypto/sdf"
	bcx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/common/v2/log"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
	"go.uber.org/zap"
)

const (
	// MaxConnCnt 单TechTradeChain节点最大连接数
	MaxConnCnt = 1024
	// DefaultGetTxTimeout 查询交易超时时间
	DefaultGetTxTimeout = 10
	// DefaultSendTxTimeout 发送交易超时时间
	DefaultSendTxTimeout = 10
	// DefaultRpcClientMaxReceiveMessageSize 默认grpc客户端接收message最大值 4M
	DefaultRpcClientMaxReceiveMessageSize = 4
	// DefaultRpcClientMaxSendMessageSize 默认grpc客户端发送message最大值 4M
	DefaultRpcClientMaxSendMessageSize = 4
	// OptimizeDetectionTxReqTimeout 乐观检查时，交易请求超时间
	OptimizeDetectionTxReqTimeout = 5
	// DefaultOptimizeDetectionSeconds 默认的乐观检查配置
	DefaultOptimizeDetectionSeconds = 60
	// MaxOptimizeDetectionSeconds 最大的乐观检查配置
	MaxOptimizeDetectionSeconds = 86400
)

var (
	// global thread-safe pkcs11 handler
	hsmHandle interface{}

	// global flag for kms
	kmsEnabled bool
)

// GetP11Handle get global thread-safe pkcs11 handler
func GetP11Handle() interface{} {
	return hsmHandle
}

// KMSEnabled return if kms is enabled
// returns:
func KMSEnabled() bool {
	return kmsEnabled
}

// NodeConfig 节点配置
type NodeConfig struct {
	// 必填项
	// 节点地址
	addr string
	// 节点连接数
	connCnt int
	// 选填项
	// 是否启用TLS认证
	useTLS bool
	// CA ROOT证书路径
	caPaths []string
	// CA ROOT证书内容（同时配置caPaths和caCerts以caCerts为准）
	caCerts []string
	// TLS hostname
	tlsHostName string
	// TLS chain hostname(如果是网关，tlsHostName是网关，当前为链的配置nginx的寻址地址)
	chainTlsHostName string
}

// NodeOption define node option func
type NodeOption func(config *NodeConfig)

// WithNodeAddr 设置节点地址
func WithNodeAddr(addr string) NodeOption {
	return func(config *NodeConfig) {
		config.addr = addr
	}
}

// WithNodeConnCnt 设置节点连接数
func WithNodeConnCnt(connCnt int) NodeOption {
	return func(config *NodeConfig) {
		config.connCnt = connCnt
	}
}

// WithNodeUseTLS 设置是否启动TLS开关
func WithNodeUseTLS(useTLS bool) NodeOption {
	return func(config *NodeConfig) {
		config.useTLS = useTLS
	}
}

// WithNodeCAPaths 添加CA证书路径
func WithNodeCAPaths(caPaths []string) NodeOption {
	return func(config *NodeConfig) {
		config.caPaths = caPaths
	}
}

// WithNodeCACerts 添加CA证书内容
func WithNodeCACerts(caCerts []string) NodeOption {
	return func(config *NodeConfig) {
		config.caCerts = caCerts
	}
}

// WithNodeTLSHostName use tls host name
func WithNodeTLSHostName(tlsHostName string) NodeOption {
	return func(config *NodeConfig) {
		config.tlsHostName = tlsHostName
	}
}

// WithNodeChainTLSHostName use chain tls host name
func WithNodeChainTLSHostName(chainTlsHostName string) NodeOption {
	return func(config *NodeConfig) {
		config.chainTlsHostName = chainTlsHostName
	}
}

// ArchiveConfig Archive配置
type ArchiveConfig struct {
	// 非必填
	// secret key
	secretKey   string
	archiveType string
	Dest        string
}

// ArchiveOption define archive option func
type ArchiveOption func(config *ArchiveConfig)

// WithSecretKey 设置Archive的secret key
func WithSecretKey(key string) ArchiveOption {
	return func(config *ArchiveConfig) {
		config.secretKey = key
	}
}

// WithType 设置Archive的类型
// @param archiveType
// @return ArchiveOption
func WithType(archiveType string) ArchiveOption {
	return func(config *ArchiveConfig) {
		config.archiveType = archiveType
	}
}

// WithDest 设置Archive的目标路径
// @param dest
// @return ArchiveOption
func WithDest(dest string) ArchiveOption {
	return func(config *ArchiveConfig) {
		config.Dest = dest
	}
}

// RPCClientConfig RPC Client 链接配置
type RPCClientConfig struct {
	// grpc客户端接收和发送消息时，允许单条message大小的最大值(MB)
	rpcClientMaxReceiveMessageSize, rpcClientMaxSendMessageSize int
	// grpc客户端发送交易和查询交易超时时间
	rpcClientSendTxTimeout, rpcClientGetTxTimeout int64
}

// RPCClientOption define rpc client option func
type RPCClientOption func(config *RPCClientConfig)

// WithRPCClientMaxReceiveMessageSize 设置RPC Client的Max Receive Message Size
func WithRPCClientMaxReceiveMessageSize(size int) RPCClientOption {
	return func(config *RPCClientConfig) {
		config.rpcClientMaxReceiveMessageSize = size
	}
}

// WithRPCClientMaxSendMessageSize 设置RPC Client的Max Send Message Size
func WithRPCClientMaxSendMessageSize(size int) RPCClientOption {
	return func(config *RPCClientConfig) {
		config.rpcClientMaxSendMessageSize = size
	}
}

// WithRPCClientSendTxTimeout 设置RPC Client的发送交易超时时间
func WithRPCClientSendTxTimeout(timeout int64) RPCClientOption {
	return func(config *RPCClientConfig) {
		config.rpcClientSendTxTimeout = timeout
	}
}

// WithRPCClientGetTxTimeout 设置RPC Client的查询交易超时时间
func WithRPCClientGetTxTimeout(timeout int64) RPCClientOption {
	return func(config *RPCClientConfig) {
		config.rpcClientGetTxTimeout = timeout
	}
}

// Pkcs11Config pkcs11配置
type Pkcs11Config struct {
	// 是否开启pkcs11, 如果为 ture 则下面所有的字段都是必填
	Enabled bool
	// interface type of lib, support pkcs11 and sdf
	Type string
	// path to the .so file of pkcs11 interface
	Library string
	// label for the slot to be used
	Label string
	// password to logon the HSM(Hardware security module)
	Password string
	// size of HSM session cache
	SessionCacheSize int
	// hash algorithm used to compute SKI, eg, SHA256
	Hash string
}

// KMSConfig kms配置
type KMSConfig struct {
	// kms enable flag
	Enabled bool
	//Public cloud or private cloud
	IsPublic bool
	//KMS SecretId
	SecretId string
	//KMS SecretKey
	SecretKey string
	//KMS server address, ip or dns
	Address string
	//KMS server region
	Region string
	//KMS sdk scheme, http or https
	SdkScheme string
	//Optional settings，style "{k1:v1, k2:v2}".
	ExtParams string
}

// AuthType define auth type of chain client
type AuthType uint32

const (
	// PermissionedWithCert permissioned with certificate
	PermissionedWithCert AuthType = iota + 1

	// PermissionedWithKey permissioned with public key
	PermissionedWithKey

	// Public public key
	Public
)

const (
	// DefaultAuthType is default cert auth type
	DefaultAuthType = ""
)

// AuthTypeToStringMap define auth type to string map
var AuthTypeToStringMap = map[AuthType]string{
	PermissionedWithCert: "permissionedwithcert",
	PermissionedWithKey:  "permissionedwithkey",
	Public:               "public",
}

// StringToAuthTypeMap define string to auth type map
var StringToAuthTypeMap = map[string]AuthType{
	"permissionedwithcert": PermissionedWithCert,
	"permissionedwithkey":  PermissionedWithKey,
	"public":               Public,
}

// ChainClientConfig define chain client configuration
type ChainClientConfig struct {
	// logger若不设置，将采用默认日志文件输出日志，建议设置，以便采用集成系统的统一日志输出
	logger utils.Logger

	// 链客户端相关配置
	// 方式1：配置文件指定（方式1与方式2可以同时使用，参数指定的值会覆盖配置文件中的配置）
	confPath string

	// 方式2：参数指定（方式1与方式2可以同时使用，参数指定的值会覆盖配置文件中的配置）
	orgId    string
	chainId  string
	nodeList []*NodeConfig

	// 是否使用tls方式，用于对参数判断，只要存在一个node开启了tls，则该值为true
	useTls bool

	// 以下xxxPath和xxxBytes同时指定的话，优先使用Bytes
	userKeyFilePath     string
	userKeyPwd          string
	userCrtFilePath     string
	userEncKeyFilePath  string //only for gmtls1.1
	userEncKeyPwd       string
	userEncCrtFilePath  string
	userSignKeyFilePath string // 公钥模式下使用该字段
	userSignKeyPwd      string
	userSignCrtFilePath string

	userKeyBytes     []byte
	userCrtBytes     []byte
	userEncKeyBytes  []byte //only for gmtls1.1
	userEncCrtBytes  []byte
	userSignKeyBytes []byte // 公钥模式下使用该字段
	userSignCrtBytes []byte

	// 以下字段为经过处理后的参数
	privateKey crypto.PrivateKey // 证书和公钥身份模式都使用该字段存储私钥

	// 证书模式下
	userCrt *bcx509.Certificate

	userPk crypto.PublicKey
	crypto *CryptoConfig

	// 归档特性的配置
	archiveConfig *ArchiveConfig

	// rpc客户端设置
	rpcClientConfig *RPCClientConfig

	// pkcs11的配置
	pkcs11Config *Pkcs11Config

	// kms配置
	kmsConfig *KMSConfig

	// Hash类型
	hashType crypto.HashType

	// AuthType
	authType AuthType

	// 节点是否只支持异步方式，老版本底链不支持同步返回，也就是新版本默认都是false
	nodeOnlyAsync bool
	// retry config
	retryLimit    int
	retryInterval int

	// alias
	alias string

	enableNormalKey bool

	// enable tx result dispatcher
	enableTxResultDispatcher bool
	// enable sync canonical tx result
	enableSyncCanonicalTxResult bool

	ConfigModel *utils.ChainClientConfigModel

	archiveCenterQueryFirst bool
	archiveCenterConfig     *ArchiveCenterConfig
	proxyUrl                string
	// 优化检测的时间周期，单位s，最大化保证多节点下使用最优化的节点
	optimizeDetection int
	// 关闭订阅时的乐观处理，默认不关闭，即开启
	disableSubscribeOptimize bool
}

// CryptoConfig define crypto config
type CryptoConfig struct {
	hash string
}

// CryptoOption define crypto option func
type CryptoOption func(config *CryptoConfig)

// WithHashAlgo 公钥模式下：添加用户哈希算法配置
func WithHashAlgo(hashType string) CryptoOption {
	return func(config *CryptoConfig) {
		config.hash = hashType
	}
}

// ChainClientOption define chain client option func
type ChainClientOption func(*ChainClientConfig)

// WithHashType specified hash type
// CRYPTO_ALGO_SHA256   = "SHA256"
// CRYPTO_ALGO_SHA3_256 = "SHA3_256"
// CRYPTO_ALGO_SM3      = "SM3"
func WithHashType(hashType string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.hashType = crypto.HashAlgoMap[hashType]
	}
}

// WithAuthType specified auth type
func WithAuthType(authType string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.authType = StringToAuthTypeMap[authType]
	}
}

// WithEnableNormalKey specified use normal key or not
func WithEnableNormalKey(enableNormalKey bool) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.enableNormalKey = enableNormalKey
	}
}

// WithConfPath 设置配置文件路径
func WithConfPath(confPath string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.confPath = confPath
	}
}

// AddChainClientNodeConfig 添加TechTradeChain节点地址及连接数配置
func AddChainClientNodeConfig(nodeConfig *NodeConfig) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.nodeList = append(config.nodeList, nodeConfig)
	}
}

// WithUserKeyFilePath 添加用户私钥文件路径配置
func WithUserKeyFilePath(userKeyFilePath string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userKeyFilePath = userKeyFilePath
	}
}

// WithUserCrtFilePath 添加用户证书文件路径配置
func WithUserCrtFilePath(userCrtFilePath string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userCrtFilePath = userCrtFilePath
	}
}

// WithUserSignKeyFilePath 添加用户签名私钥文件路径配置
func WithUserSignKeyFilePath(userSignKeyFilePath string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userSignKeyFilePath = userSignKeyFilePath
	}
}

// WithUserSignCrtFilePath 添加用户签名证书文件路径配置
func WithUserSignCrtFilePath(userSignCrtFilePath string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userSignCrtFilePath = userSignCrtFilePath
	}
}

// WithUserKeyBytes 添加用户私钥文件内容配置
func WithUserKeyBytes(userKeyBytes []byte) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userKeyBytes = userKeyBytes
	}
}

// WithUserCrtBytes 添加用户证书文件内容配置
func WithUserCrtBytes(userCrtBytes []byte) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userCrtBytes = userCrtBytes
	}
}

// WithUserEncKeyFilePath 添加支持国密双证书的加密证书私钥文件
func WithUserEncKeyFilePath(userEncKeyFilePath string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userEncKeyFilePath = userEncKeyFilePath
	}
}

// WithUserEncKeyBytes 添加支持国密双证书的加密证书私钥文件内容配置
func WithUserEncKeyBytes(userEncKeyBytes []byte) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userEncKeyBytes = userEncKeyBytes
	}
}

// WithUserEncCrtFilePath 添加支持国密双证书的加密证书文件
func WithUserEncCrtFilePath(userEncCrtFilePath string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userEncCrtFilePath = userEncCrtFilePath
	}
}

// WithUserEncCrtBytes 添加支持国密双证书的加密证书文件内容配置
func WithUserEncCrtBytes(userEncCrtBytes []byte) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userEncCrtBytes = userEncCrtBytes
	}
}

// WithUserSignKeyBytes 添加用户签名私钥文件内容配置
func WithUserSignKeyBytes(userSignKeyBytes []byte) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userSignKeyBytes = userSignKeyBytes
	}
}

// WithUserSignCrtBytes 添加用户签名证书文件内容配置
func WithUserSignCrtBytes(userSignCrtBytes []byte) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userSignCrtBytes = userSignCrtBytes
	}
}

// WithChainClientOrgId 添加OrgId
func WithChainClientOrgId(orgId string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.orgId = orgId
	}
}

// WithChainClientChainId 添加ChainId
func WithChainClientChainId(chainId string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.chainId = chainId
	}
}

// WithNodeOnlyAsync 当前值默认为false，会影响是否启动异步查询，即retryLimit和retryInterval是否生效
func WithNodeOnlyAsync(nodeOnlyAsync bool) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.nodeOnlyAsync = nodeOnlyAsync
	}
}

// WithRetryLimit 设置 chain client 同步模式下，轮询获取交易结果时的最大轮询次数
func WithRetryLimit(limit int) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.retryLimit = limit
	}
}

// WithRetryInterval 设置 chain client 同步模式下，每次轮询交易结果时的等待时间，单位：ms
func WithRetryInterval(interval int) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.retryInterval = interval
	}
}

// WithChainClientAlias specified cert alias
func WithChainClientAlias(alias string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.alias = alias
	}
}

// WithChainClientLogger 设置Logger对象，便于日志打印
func WithChainClientLogger(logger utils.Logger) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.logger = logger
	}
}

// WithArchiveConfig 设置Archive配置
func WithArchiveConfig(conf *ArchiveConfig) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.archiveConfig = conf
	}
}

// WithRPCClientConfig 设置grpc客户端配置
func WithRPCClientConfig(conf *RPCClientConfig) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.rpcClientConfig = conf
	}
}

// WithPkcs11Config 设置pkcs11配置
func WithPkcs11Config(conf *Pkcs11Config) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.pkcs11Config = conf
	}
}

// WithKMSConfig 设置kms配置
// args:
// returns:
func WithKMSConfig(conf *KMSConfig) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.kmsConfig = conf
	}
}

// WithCryptoConfig 设置crypto配置
func WithCryptoConfig(conf *CryptoConfig) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.crypto = conf
	}
}

// WithEnableTxResultDispatcher 设置是否启用 异步订阅机制获取交易结果。
// 默认不启用，如不启用将继续使用轮询机制获取交易结果。
func WithEnableTxResultDispatcher(enable bool) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.enableTxResultDispatcher = enable
	}
}

// WithEnableSyncCanonicalTxResult 设置是否启用 同步获取权威的公认的交易结果，即超过半数共识的交易。默认不启用。
func WithEnableSyncCanonicalTxResult(enable bool) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.enableSyncCanonicalTxResult = enable
	}
}

// WithUserKeyPwd 配置用户私钥密码
func WithUserKeyPwd(pwd string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userKeyPwd = pwd
	}
}

// WithUserSignKeyPwd 配置用户签名私钥密码
func WithUserSignKeyPwd(pwd string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userSignKeyPwd = pwd
	}
}

// WithUserEncKeyPwd 配置国密双证书模式下用户私钥密码
func WithUserEncKeyPwd(pwd string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.userEncKeyPwd = pwd
	}
}

// WithArchiveCenterHttpConfig 设置归档中心http接口
func WithArchiveCenterHttpConfig(conf *ArchiveCenterConfig) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.archiveCenterConfig = conf
	}
}

// WithArchiveCenterQueryFirst 设置优先从归档中心查询
func WithArchiveCenterQueryFirst(first bool) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.archiveCenterQueryFirst = first
	}
}

// WithProxy 配置http代理地址
func WithProxy(url string) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.proxyUrl = strings.TrimSpace(url)
	}
}

// WithOptimizeDetection 配置乐观优化的时间检测周期
func WithOptimizeDetection(seconds int) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.optimizeDetection = seconds
	}
}

// WithDisableSubscribeOptimize 关闭乐观检查
func WithDisableSubscribeOptimize(dso bool) ChainClientOption {
	return func(config *ChainClientConfig) {
		config.disableSubscribeOptimize = dso
	}
}

// 生成SDK配置并校验合法性
func generateConfig(opts ...ChainClientOption) (*ChainClientConfig, error) {
	config := &ChainClientConfig{}
	for _, opt := range opts {
		opt(config)
	}

	// 校验config参数合法性
	if err := checkConfig(config); err != nil {
		return nil, err
	}

	// 进一步处理config参数
	if err := dealConfig(config); err != nil {
		return nil, err
	}

	return config, nil
}

func generateConfigWithoutKey(opts ...ChainClientOption) (*ChainClientConfig, error) {
	config := &ChainClientConfig{}
	for _, opt := range opts {
		opt(config)
	}

	// 校验config参数合法性
	if err := checkConfigWithoutKey(config); err != nil {
		return nil, err
	}

	// 进一步处理config参数
	if err := dealConfigWithoutKey(config); err != nil {
		return nil, err
	}

	return config, nil
}

func setAuthType(config *ChainClientConfig) {
	if config.authType == 0 {
		if config.ConfigModel.ChainClientConfig.AuthType == "" {
			config.authType = PermissionedWithCert
		} else {
			config.authType = StringToAuthTypeMap[config.ConfigModel.ChainClientConfig.AuthType]
		}
	}
}

func setCrypto(config *ChainClientConfig) {
	if config.authType == PermissionedWithCert {
		config.crypto = &CryptoConfig{}
		return
	}

	if config.ConfigModel.ChainClientConfig.Crypto != nil && config.crypto == nil {
		config.crypto = &CryptoConfig{
			hash: config.ConfigModel.ChainClientConfig.Crypto.Hash,
		}
	}
}

func setChainConfig(config *ChainClientConfig) {
	if config.ConfigModel.ChainClientConfig.ChainId != "" && config.chainId == "" {
		config.chainId = config.ConfigModel.ChainClientConfig.ChainId
	}

	if config.ConfigModel.ChainClientConfig.OrgId != "" && config.orgId == "" {
		config.orgId = config.ConfigModel.ChainClientConfig.OrgId
	}

	if config.ConfigModel.ChainClientConfig.Alias != "" && config.alias == "" {
		config.alias = config.ConfigModel.ChainClientConfig.Alias
	}

	config.enableNormalKey = config.ConfigModel.ChainClientConfig.EnableNormalKey
}

// nolint
// 如果参数没有设置，便使用配置文件的配置
func setUserConfig(config *ChainClientConfig) {
	// pwk模式和Public、Cert模式均支持国密双证书无需单独处理
	if config.ConfigModel.ChainClientConfig.UserKeyFilePath != "" && config.userKeyFilePath == "" &&
		config.userKeyBytes == nil {
		config.userKeyFilePath = config.ConfigModel.ChainClientConfig.UserKeyFilePath
	}

	if config.ConfigModel.ChainClientConfig.UserCrtFilePath != "" && config.userCrtFilePath == "" &&
		config.userCrtBytes == nil {
		config.userCrtFilePath = config.ConfigModel.ChainClientConfig.UserCrtFilePath
	}

	if config.ConfigModel.ChainClientConfig.UserEncKeyFilePath != "" && config.userEncKeyFilePath == "" &&
		config.userEncKeyBytes == nil {
		config.userEncKeyFilePath = config.ConfigModel.ChainClientConfig.UserEncKeyFilePath
	}

	if config.ConfigModel.ChainClientConfig.UserEncCrtFilePath != "" && config.userEncCrtFilePath == "" &&
		config.userEncCrtBytes == nil {
		config.userEncCrtFilePath = config.ConfigModel.ChainClientConfig.UserEncCrtFilePath
	}

	if config.ConfigModel.ChainClientConfig.UserSignKeyFilePath != "" && config.userSignKeyFilePath == "" &&
		config.userSignKeyBytes == nil {
		config.userSignKeyFilePath = config.ConfigModel.ChainClientConfig.UserSignKeyFilePath
	}

	// 证书模型下需要处理该部分内容，pk模式下不需要加载
	if config.authType == PermissionedWithCert {
		if config.ConfigModel.ChainClientConfig.UserSignCrtFilePath != "" && config.userSignCrtFilePath == "" &&
			config.userSignCrtBytes == nil {
			config.userSignCrtFilePath = config.ConfigModel.ChainClientConfig.UserSignCrtFilePath
		}
	}

	if config.ConfigModel.ChainClientConfig.UserKeyPwd != "" && config.userKeyPwd == "" {
		config.userKeyPwd = config.ConfigModel.ChainClientConfig.UserKeyPwd
	}
	if config.ConfigModel.ChainClientConfig.UserSignKeyPwd != "" && config.userSignKeyPwd == "" {
		config.userSignKeyPwd = config.ConfigModel.ChainClientConfig.UserSignKeyPwd
	}
	if config.ConfigModel.ChainClientConfig.UserEncKeyPwd != "" && config.userEncKeyPwd == "" {
		config.userEncKeyPwd = config.ConfigModel.ChainClientConfig.UserEncKeyPwd
	}

	// 支持正向代理设置
	if config.ConfigModel.ChainClientConfig.Proxy != "" {
		config.proxyUrl = strings.TrimSpace(config.ConfigModel.ChainClientConfig.Proxy)
	}
}

func setNodeList(config *ChainClientConfig) {
	if len(config.ConfigModel.ChainClientConfig.NodesConfig) > 0 && len(config.nodeList) == 0 {
		for _, conf := range config.ConfigModel.ChainClientConfig.NodesConfig {
			node := NewNodeConfig(
				// 节点地址，格式：127.0.0.1:12301
				WithNodeAddr(conf.NodeAddr),
				// 节点连接数
				WithNodeConnCnt(conf.ConnCnt),
				// 节点是否启用TLS认证
				WithNodeUseTLS(conf.EnableTLS),
				// 根证书路径，支持多个
				WithNodeCAPaths(conf.TrustRootPaths),
				// TLS Hostname
				WithNodeTLSHostName(conf.TLSHostName),
				// TLS Chain Hostname
				WithNodeChainTLSHostName(conf.ChainTLSHostName),
			)

			config.nodeList = append(config.nodeList, node)
		}
	}
}

func setArchiveConfig(config *ChainClientConfig) {
	if config.ConfigModel.ChainClientConfig.ArchiveConfig != nil && config.archiveConfig == nil {
		archive := NewArchiveConfig(
			// secret key
			WithSecretKey(config.ConfigModel.ChainClientConfig.ArchiveConfig.SecretKey),
			// archive server address
			WithDest(config.ConfigModel.ChainClientConfig.ArchiveConfig.Dest),
			//archive type
			WithType(config.ConfigModel.ChainClientConfig.ArchiveConfig.Type),
		)

		config.archiveConfig = archive
	}
}

func setRPCClientConfig(config *ChainClientConfig) {
	if config.ConfigModel.ChainClientConfig.RPCClientConfig != nil && config.rpcClientConfig == nil {
		rpcClient := NewRPCClientConfig(
			WithRPCClientMaxReceiveMessageSize(config.ConfigModel.ChainClientConfig.RPCClientConfig.MaxRecvMsgSize),
			WithRPCClientMaxSendMessageSize(config.ConfigModel.ChainClientConfig.RPCClientConfig.MaxSendMsgSize),
			WithRPCClientSendTxTimeout(config.ConfigModel.ChainClientConfig.RPCClientConfig.SendTxTimeout),
			WithRPCClientGetTxTimeout(config.ConfigModel.ChainClientConfig.RPCClientConfig.GetTxTimeout),
		)
		config.rpcClientConfig = rpcClient
	}
}

func setPkcs11Config(config *ChainClientConfig) {
	if config.authType == PermissionedWithCert {
		if config.ConfigModel.ChainClientConfig.Pkcs11Config != nil && config.pkcs11Config == nil {
			config.pkcs11Config = NewPkcs11Config(
				config.ConfigModel.ChainClientConfig.Pkcs11Config.Enabled,
				config.ConfigModel.ChainClientConfig.Pkcs11Config.Type,
				config.ConfigModel.ChainClientConfig.Pkcs11Config.Library,
				config.ConfigModel.ChainClientConfig.Pkcs11Config.Label,
				config.ConfigModel.ChainClientConfig.Pkcs11Config.Password,
				config.ConfigModel.ChainClientConfig.Pkcs11Config.SessionCacheSize,
				config.ConfigModel.ChainClientConfig.Pkcs11Config.Hash,
			)
		}
	} else {
		config.pkcs11Config = &Pkcs11Config{
			Enabled: false,
		}
		//if config.ConfigModel.ChainClientConfig.Pkcs11Config != nil && config.pkcs11Config == nil {
		//	config.pkcs11Config = &Pkcs11Config{
		//	}
		//}
	}
}

// setKMSConfig set KMS config.
// args:
func setKMSConfig(config *ChainClientConfig) {
	if config.authType == PermissionedWithCert || config.authType == Public {
		if config.ConfigModel.ChainClientConfig.KMSConfig != nil && config.kmsConfig == nil {
			config.kmsConfig = NewKMSConfig(
				config.ConfigModel.ChainClientConfig.KMSConfig.Enabled,
				config.ConfigModel.ChainClientConfig.KMSConfig.IsPublic,
				config.ConfigModel.ChainClientConfig.KMSConfig.SecretId,
				config.ConfigModel.ChainClientConfig.KMSConfig.SecretKey,
				config.ConfigModel.ChainClientConfig.KMSConfig.Address,
				config.ConfigModel.ChainClientConfig.KMSConfig.Region,
				config.ConfigModel.ChainClientConfig.KMSConfig.SdkScheme,
				config.ConfigModel.ChainClientConfig.KMSConfig.ExtParams,
			)
		}
	} else {
		config.kmsConfig = &KMSConfig{
			Enabled: false,
		}
	}
}

// setArchiveCenterConfig 设置归档中心相关配置
func setArchiveCenterConfig(config *ChainClientConfig) {

	if config.ConfigModel.ChainClientConfig.ArchiveCenterConfig != nil &&
		len(strings.TrimSpace(
			config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.ChainGenesisHash)) > 0 {
		config.archiveCenterConfig = &ArchiveCenterConfig{}
		config.archiveCenterConfig.ChainGenesisHash = strings.TrimSpace(
			config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.ChainGenesisHash)
		config.archiveCenterConfig.ArchiveCenterHttpUrl = strings.TrimSpace(
			config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.ArchiveCenterHttpUrl)

		config.archiveCenterConfig.RpcAddress = strings.TrimSpace(
			config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.RpcAddress)
		config.archiveCenterConfig.TlsEnable =
			config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.TlsEnable
		config.archiveCenterConfig.Tls.ServerName = strings.TrimSpace(
			config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.Tls.ServerName)
		config.archiveCenterConfig.Tls.PrivKeyFile = strings.TrimSpace(
			config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.Tls.PrivKeyFile)
		config.archiveCenterConfig.Tls.CertFile = strings.TrimSpace(
			config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.Tls.CertFile)
		for i := 0; i < len(
			config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.Tls.TrustCaList); i++ {
			config.archiveCenterConfig.Tls.TrustCaList =
				append(config.archiveCenterConfig.Tls.TrustCaList,
					config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.Tls.TrustCaList[i])
		}
		if config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.RequestSecondLimit > 0 {
			config.archiveCenterConfig.ReqeustSecondLimit =
				config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.RequestSecondLimit
		} else {
			config.archiveCenterConfig.ReqeustSecondLimit = httpRequestDuration
		}
		if config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.MaxSendMsgSize > 0 {
			config.archiveCenterConfig.MaxSendMsgSize =
				config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.MaxSendMsgSize
		} else {
			config.archiveCenterConfig.MaxSendMsgSize = archiveCenterRpcMaxMsgSize
		}
		if config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.MaxRecvMsgSize > 0 {
			config.archiveCenterConfig.MaxRecvMsgSize =
				config.ConfigModel.ChainClientConfig.ArchiveCenterConfig.MaxRecvMsgSize
		} else {
			config.archiveCenterConfig.MaxRecvMsgSize = archiveCenterRpcMaxMsgSize
		}
		// set archiveCenterQueryFirst
		config.archiveCenterQueryFirst = config.ConfigModel.ChainClientConfig.ArchiveCenterQueryFirst
	}
}

func setRetryConfig(config *ChainClientConfig) {
	// 增加NodeOnlyAsync配置
	if !config.nodeOnlyAsync && config.ConfigModel.ChainClientConfig.NodeOnlyAsync {
		config.nodeOnlyAsync = config.ConfigModel.ChainClientConfig.NodeOnlyAsync
	}
	if config.ConfigModel.ChainClientConfig.RetryLimit != 0 && config.retryLimit == 0 {
		config.retryLimit = config.ConfigModel.ChainClientConfig.RetryLimit
	}
	if config.ConfigModel.ChainClientConfig.RetryInterval != 0 && config.retryInterval == 0 {
		config.retryInterval = config.ConfigModel.ChainClientConfig.RetryInterval
	}
}

func setOptimizeConfig(config *ChainClientConfig) {
	if config.ConfigModel.ChainClientConfig.OptimizeDetection != 0 && config.optimizeDetection == 0 {
		config.optimizeDetection = config.ConfigModel.ChainClientConfig.OptimizeDetection
	}
	if config.optimizeDetection == 0 {
		// 默认开启乐观检查
		config.optimizeDetection = DefaultOptimizeDetectionSeconds
	}
	// 设置最大值，不允许该值太大
	if config.optimizeDetection > MaxOptimizeDetectionSeconds {
		config.optimizeDetection = MaxOptimizeDetectionSeconds
	}
	// 设置开启或关闭乐观订阅的处理
	config.disableSubscribeOptimize = config.ConfigModel.ChainClientConfig.DisableSubscribeOptimize
}

func readConfigFile(config *ChainClientConfig) error {
	// 若没有配置配置文件
	if config.confPath == "" {
		return nil
	}
	var (
		configModel *utils.ChainClientConfigModel
		err         error
	)
	if configModel, err = utils.InitConfig(config.confPath); err != nil {
		return fmt.Errorf("init config failed, %s", err.Error())
	}

	config.ConfigModel = configModel

	setAuthType(config)

	setCrypto(config)

	setChainConfig(config)

	setUserConfig(config)

	setNodeList(config)

	setArchiveConfig(config)

	setRPCClientConfig(config)

	setPkcs11Config(config)

	setKMSConfig(config)

	setRetryConfig(config)

	setArchiveCenterConfig(config) // 归档中心设置

	setOptimizeConfig(config) // 优化配置

	return nil
}

// checkConfigWithoutKey SDK配置校验在不需要用户签名的情况下
func checkConfigWithoutKey(config *ChainClientConfig) error {

	var (
		err error
	)

	if err = readConfigFile(config); err != nil {
		return fmt.Errorf("read sdk config file failed, %s", err.Error())
	}

	// 如果logger未指定，使用默认zap logger
	if config.logger == nil {
		config.logger = getDefaultLogger()
	}

	// 如果authType未指定，使用默认PermissionedWithCert
	if config.authType == 0 {
		config.authType = PermissionedWithCert
	}

	if err = checkNodeListConfig(config); err != nil {
		return err
	}

	if err = checkUserConfigWithoutKey(config); err != nil {
		return err
	}

	if err = checkChainConfig(config); err != nil {
		return err
	}

	if err = checkArchiveConfig(config); err != nil {
		return err
	}

	if err = checkPkcs11Config(config); err != nil {
		return err
	}

	if err = checkKMSConfig(config); err != nil {
		return err
	}

	if err = checkArchiveCenterConfig(config); err != nil {
		return err
	}

	return checkRPCClientConfig(config)
}

// SDK配置校验
func checkConfig(config *ChainClientConfig) error {

	var (
		err error
	)

	if err = readConfigFile(config); err != nil {
		return fmt.Errorf("read sdk config file failed, %s", err.Error())
	}

	// 如果logger未指定，使用默认zap logger
	if config.logger == nil {
		config.logger = getDefaultLogger()
	}

	// 如果authType未指定，使用默认PermissionedWithCert
	if config.authType == 0 {
		config.authType = PermissionedWithCert
	}

	if err = checkNodeListConfig(config); err != nil {
		return err
	}

	if err = checkUserConfig(config); err != nil {
		return err
	}

	if err = checkChainConfig(config); err != nil {
		return err
	}

	if err = checkArchiveConfig(config); err != nil {
		return err
	}

	if err = checkPkcs11Config(config); err != nil {
		return err
	}

	if err = checkKMSConfig(config); err != nil {
		return err
	}

	if err = checkArchiveCenterConfig(config); err != nil {
		return err
	}

	return checkRPCClientConfig(config)
}

func checkNodeListConfig(config *ChainClientConfig) error {
	// 连接的节点地址不可为空
	if len(config.nodeList) == 0 {
		return fmt.Errorf("connect techtradechain node address is empty")
	}

	// 已配置的节点地址连接数，需要在合理区间
	for _, node := range config.nodeList {
		if node.connCnt <= 0 || node.connCnt > MaxConnCnt {
			return fmt.Errorf("node connection count should >0 && <=%d",
				MaxConnCnt)
		}

		if node.useTLS {
			// 如果开启了TLS认证，CA路径必填
			if len(node.caPaths) == 0 && len(node.caCerts) == 0 {
				return fmt.Errorf("if node useTLS is open, should set caPaths or caCerts")
			}

			// 如果开启了TLS认证，需配置TLS HostName
			if node.tlsHostName == "" {
				return fmt.Errorf("if node useTLS is open, should set tls hostname")
			}

			// 只要任意一个node启动了tls，则设置useTls为true
			config.useTls = true
		}
	}

	return nil
}

// checkUserConfigWithoutKey 在withoutKey模式下，不需要验证用户签名的信息
func checkUserConfigWithoutKey(config *ChainClientConfig) error {
	return checkTlsConfig(config)
}

func checkUserConfig(config *ChainClientConfig) error {
	if config.authType == PermissionedWithCert {
		// 证书模式下user_sign_key_file_path和user_sign_crt_file_path对应信息必须配置
		if config.userSignKeyFilePath == "" && config.userSignKeyBytes == nil {
			return fmt.Errorf("user sign key cannot be empty in cert mode")
		}
		if config.userSignCrtFilePath == "" && config.userSignCrtBytes == nil {
			return fmt.Errorf("user sign crt cannot be empty in cert mode")
		}
		return checkTlsConfig(config)
	} else if config.authType == Public {
		// 在Public模式下，必须保证user_sign_key_file_path对应信息配置
		if config.userSignKeyFilePath == "" && config.userSignKeyBytes == nil {
			return fmt.Errorf("user sign key cannot be empty in public mode")
		}
		return checkTlsConfig(config)
	}
	// 在PWK模式下，必须保证user_sign_key_file_path对应信息配置
	if config.userSignKeyFilePath == "" && config.userSignKeyBytes == nil {
		return fmt.Errorf("user sign key cannot be empty in pwk mode")
	}
	return checkTlsConfig(config)
}

func checkTlsConfig(config *ChainClientConfig) error {
	modeStr := "cert"
	if config.authType == Public {
		modeStr = "public"
	} else if config.authType == PermissionedWithKey {
		modeStr = "pwk"
	}
	// 如果使用了tls则必须配置user_key_file_path和user_crt_file_path对应的信息
	// 因没有办法判断是否开启了国密双证书，固不对其进行验证
	if config.useTls {
		if config.userKeyFilePath == "" && config.userKeyBytes == nil {
			return fmt.Errorf("user tls key cannot be empty in %s mode", modeStr)
		}
		if config.userCrtFilePath == "" && config.userCrtBytes == nil {
			return fmt.Errorf("user tls crt cannot be empty in %s mode", modeStr)
		}
	}
	return nil
}

func checkChainConfig(config *ChainClientConfig) error {
	if config.authType == PermissionedWithCert || config.authType == PermissionedWithKey {
		// OrgId不可为空
		if config.orgId == "" {
			return fmt.Errorf("orgId cannot be empty")
		}
	}

	// ChainId不可为空
	if config.chainId == "" {
		return fmt.Errorf("chainId cannot be empty")
	}

	return nil
}

func checkArchiveConfig(config *ChainClientConfig) error {
	return nil
}

func checkPkcs11Config(config *ChainClientConfig) error {
	if config.pkcs11Config == nil {
		config.pkcs11Config = &Pkcs11Config{}
		return nil
	}
	if !config.pkcs11Config.Enabled {
		return nil
	}
	// 如果config.pkcs11Config.Enabled == true 则其他参数不能为空
	if config.pkcs11Config.Library == "" {
		return errors.New("config.pkcs11Config.Library must not empty")
	}
	if config.pkcs11Config.Type == "" {
		return errors.New("config.pkcs11Config.Type must not empty")
	}
	if config.pkcs11Config.Label == "" {
		return errors.New("config.pkcs11Config.Label must not empty")
	}
	if config.pkcs11Config.Password == "" {
		return errors.New("config.pkcs11Config.Password must not empty")
	}
	if config.pkcs11Config.SessionCacheSize == 0 {
		return errors.New("config.pkcs11Config.SessionCacheSize must > 0")
	}
	if config.pkcs11Config.Hash == "" {
		return errors.New("config.pkcs11Config.Hash must not empty")
	}
	return nil
}

// checkKMSConfig check KMS config
func checkKMSConfig(cfg *ChainClientConfig) error {
	config := cfg.kmsConfig
	if config == nil || !config.Enabled {
		envConfig := &KMSConfig{}
		kmsEnable := os.Getenv("KMS_ENABLE")
		if strings.EqualFold(strings.ToLower(kmsEnable), "true") {
			envConfig.SecretId = os.Getenv("KMS_SECRET_ID")
			envConfig.SecretKey = os.Getenv("KMS_SECRET_KEY")
			envConfig.Address = os.Getenv("KMS_ADDRESS")
			envConfig.Region = os.Getenv("KMS_REGION")
			envConfig.SdkScheme = os.Getenv("SMK_SDK_SCHEME")
			isPublicStr := os.Getenv("KMS_IS_PUBLIC")
			if strings.EqualFold(strings.ToLower(isPublicStr), "true") {
				envConfig.Enabled = true
			}
		}
		config = envConfig
		cfg.kmsConfig = envConfig
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

// checkRPCClientConfig check grpc client config
func checkRPCClientConfig(config *ChainClientConfig) error {
	if config.rpcClientConfig == nil {
		rpcClient := NewRPCClientConfig(
			WithRPCClientMaxReceiveMessageSize(DefaultRpcClientMaxReceiveMessageSize),
			WithRPCClientMaxSendMessageSize(DefaultRpcClientMaxSendMessageSize),
			WithRPCClientSendTxTimeout(DefaultSendTxTimeout),
			WithRPCClientGetTxTimeout(DefaultGetTxTimeout),
		)
		config.rpcClientConfig = rpcClient
	} else {
		if config.rpcClientConfig.rpcClientMaxReceiveMessageSize <= 0 {
			config.rpcClientConfig.rpcClientMaxReceiveMessageSize = DefaultRpcClientMaxReceiveMessageSize
		}
		if config.rpcClientConfig.rpcClientMaxSendMessageSize <= 0 {
			config.rpcClientConfig.rpcClientMaxSendMessageSize = DefaultRpcClientMaxSendMessageSize
		}
		if config.rpcClientConfig.rpcClientSendTxTimeout <= 0 {
			config.rpcClientConfig.rpcClientSendTxTimeout = DefaultSendTxTimeout
		}
		if config.rpcClientConfig.rpcClientGetTxTimeout <= 0 {
			config.rpcClientConfig.rpcClientGetTxTimeout = DefaultGetTxTimeout
		}
	}
	return nil
}

func checkArchiveCenterConfig(config *ChainClientConfig) error {
	return nil
}

func dealConfig(config *ChainClientConfig) error {
	var err error
	if err = dealArchiveCenterConfig(config); err != nil {
		return err
	}

	// PermissionedWithCert
	if err = dealUserCrtConfig(config); err != nil {
		return err
	}
	if err = dealUserKeyConfig(config); err != nil {
		return err
	}

	// Public模式和PWK模式均支持国密双证书配置，但配置内容与cert模式有区别，因此单独摘出来
	if config.authType == Public || config.authType == PermissionedWithKey {
		// gmtls enc key/cert set
		err = dealUserEncConfig(config)
		if err != nil {
			return err
		}
		return dealUserSignKeyConfig(config)
	}

	// cert模式处理
	if err = dealUserEncConfig(config); err != nil {
		return err
	}
	if err = dealUserSignCrtConfig(config); err != nil {
		return err
	}
	return dealUserSignKeyConfig(config)
}

func dealConfigWithoutKey(config *ChainClientConfig) error {
	var err error
	if err = dealArchiveCenterConfig(config); err != nil {
		return err
	}

	// ida 业务场景下，sdk 使用不配置签名私钥和证书
	// PermissionedWithKey & Public
	//if config.authType == PermissionedWithKey || config.authType == Public {
	//	return dealUserSignKeyConfig(config)
	//}

	// PWK模式暂不支持国密双证书
	if config.authType == PermissionedWithKey {
		return nil
	}

	// pk、cert 模式支持国密双证书配置
	// gmtls enc key/cert set
	err = dealUserEncConfig(config)
	if err != nil {
		return err
	}

	// cert 模式还需要处理下 tls 证书
	// PermissionedWithCert
	if config.authType == PermissionedWithCert {
		if err = dealUserCrtConfig(config); err != nil {
			return err
		}

		if err = dealUserKeyConfig(config); err != nil {
			return err
		}
	}

	// ida 业务场景下，sdk 使用不配置签名私钥和证书
	//if err = dealUserSignCrtConfig(config); err != nil {
	//	return err
	//}

	return nil
}

func dealUserCrtConfig(config *ChainClientConfig) (err error) {
	// userCrtBytes优先级更高，仅在userCrtBytes未配置，但userCrtFilePath配置的情况下需要解析
	if len(config.userCrtBytes) == 0 && len(config.userCrtFilePath) > 0 {
		// 读取用户证书
		config.userCrtBytes, err = ioutil.ReadFile(config.userCrtFilePath)
		if err != nil {
			return fmt.Errorf("read user crt file failed, %s", err.Error())
		}
	}
	return nil
}

func dealUserKeyConfig(config *ChainClientConfig) (err error) {
	// userKeyBytes优先级更高
	if len(config.userKeyBytes) == 0 && len(config.userKeyFilePath) > 0 {
		// 从私钥文件读取用户私钥，转换为privateKey对象
		userKeyBytes, err := ioutil.ReadFile(config.userKeyFilePath)
		if err != nil {
			return fmt.Errorf("read user key file failed, %s", err)
		}
		if config.userKeyPwd != "" {
			config.userKeyBytes, err = decryptPrivKeyPem(userKeyBytes,
				[]byte(config.userKeyPwd))
			if err != nil {
				return fmt.Errorf("decrypt user key pem failed, %s", err)
			}
		} else {
			config.userKeyBytes = userKeyBytes
		}
	}
	if len(config.userKeyBytes) > 0 {
		config.privateKey, err = asym.PrivateKeyFromPEM(config.userKeyBytes, nil)
		if err != nil {
			return fmt.Errorf("parse user key file to privateKey obj failed, %s", err)
		}
	}

	return nil
}

// dealUserEncConfig is used to load tls enc key/crt
// if the files from config are not valid, use default tls, no error is returned!
func dealUserEncConfig(config *ChainClientConfig) (err error) {
	// userEncCrtBytes 优先级更高
	if len(config.userEncCrtBytes) == 0 && len(config.userEncCrtFilePath) > 0 {
		crtBytes, err := ioutil.ReadFile(config.userEncCrtFilePath)
		if err != nil {
			return fmt.Errorf("read user enc crt file %s failed, %s", config.userEncCrtFilePath, err.Error())
		}
		config.userEncCrtBytes = crtBytes
	}
	// userEncKeyBytes 优先级更高
	if len(config.userEncKeyBytes) == 0 && len(config.userEncKeyFilePath) > 0 {
		keyBytes, err := ioutil.ReadFile(config.userEncKeyFilePath)
		if err != nil {
			return fmt.Errorf("read user enc key file %s failed, %s", config.userEncKeyFilePath, err.Error())
		}
		if config.userEncKeyPwd != "" {
			config.userEncKeyBytes, err = decryptPrivKeyPem(keyBytes, []byte(config.userEncKeyPwd))
			if err != nil {
				return fmt.Errorf("decrypt user enc key pem failed, %s", err)
			}
		} else {
			config.userEncKeyBytes = keyBytes
		}
	}
	return nil
}

func dealUserSignCrtConfig(config *ChainClientConfig) (err error) {
	if len(config.userSignCrtBytes) == 0 && len(config.userSignCrtFilePath) > 0 {
		config.userSignCrtBytes, err = ioutil.ReadFile(config.userSignCrtFilePath)
		if err != nil {
			return fmt.Errorf("read user sign crt file %s failed, %s", config.userSignCrtFilePath, err.Error())
		}
	}
	if len(config.userSignCrtBytes) > 0 {
		// userCrt该值中的Hash算法代表的是链的Hash算法，使用userSign证书中的数据
		if config.userCrt, err = utils.ParseCert(config.userSignCrtBytes); err != nil {
			return fmt.Errorf("parse user sign crt obj failed, %s", err.Error())
		}
	}
	return
}

func decryptPrivKeyPem(encryptedPrivKeyPem, pwd []byte) ([]byte, error) {
	block, _ := pem.Decode(encryptedPrivKeyPem)
	privateKey, err := asym.PrivateKeyFromPEM(encryptedPrivKeyPem, pwd)
	if err != nil {
		return nil, fmt.Errorf("PrivateKeyFromPEM failed, %s", err.Error())
	}
	privDER, err := privateKey.Bytes()
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Bytes: privDER, Type: block.Type}), nil
}

func dealUserSignKeyConfig(config *ChainClientConfig) (err error) {
	// PermissionedWithKey, Public
	if config.authType == PermissionedWithKey || config.authType == Public {
		if config.userSignKeyBytes == nil && len(config.userSignKeyFilePath) > 0 {
			userSignKeyBytes, err := ioutil.ReadFile(config.userSignKeyFilePath)
			if err != nil {
				return fmt.Errorf("read user sign key file %s failed, %s",
					config.userSignKeyFilePath, err.Error())
			}
			if config.userSignKeyPwd != "" {
				config.userSignKeyBytes, err = decryptPrivKeyPem(userSignKeyBytes,
					[]byte(config.userSignKeyPwd))
				if err != nil {
					return fmt.Errorf("decrypt user sign key failed, %s", err.Error())
				}
			} else {
				config.userSignKeyBytes = userSignKeyBytes
			}
		}

		if config.kmsConfig.Enabled {
			config.privateKey, err = kmsutils.ParseKMSPrivKey(config.userSignKeyBytes)
			if err != nil {
				return fmt.Errorf("cert.ParseKMSPrivKey failed, %s", err)
			}
			kmsEnabled = config.kmsConfig.Enabled
		} else {
			config.privateKey, err = asym.PrivateKeyFromPEM(config.userSignKeyBytes, nil)
			if err != nil {
				return fmt.Errorf("PrivateKeyFromPEM failed, %s", err.Error())
			}
		}

		config.userPk = config.privateKey.PublicKey()
		return nil
	}

	return dealUserSignKeyConfigWithCert(config)
}

func dealUserSignKeyConfigWithCert(config *ChainClientConfig) (err error) {
	// PermissionedWithCert
	if config.userSignKeyBytes == nil && len(config.userSignKeyFilePath) > 0 {
		userSignKeyBytes, err := ioutil.ReadFile(config.userSignKeyFilePath)
		if err != nil {
			return fmt.Errorf("read user sign key file %s failed, %s", config.userSignKeyFilePath, err.Error())
		}
		if config.userSignKeyPwd != "" {
			config.userSignKeyBytes, err = decryptPrivKeyPem(userSignKeyBytes,
				[]byte(config.userSignKeyPwd))
			if err != nil {
				return fmt.Errorf("decrypt user sign key failed, %s", err.Error())
			}
		} else {
			config.userSignKeyBytes = userSignKeyBytes
		}
	}

	if config.pkcs11Config.Enabled {
		if strings.EqualFold(config.pkcs11Config.Type, "pkcs11") {
			hsmHandle, err = pkcs11.New(config.pkcs11Config.Library, config.pkcs11Config.Label,
				config.pkcs11Config.Password, config.pkcs11Config.SessionCacheSize, config.pkcs11Config.Hash)
		} else if strings.EqualFold(config.pkcs11Config.Type, "sdf") {
			hsmHandle, err = sdf.New(config.pkcs11Config.Library, config.pkcs11Config.SessionCacheSize)
		} else {
			err = fmt.Errorf("type is not valid, want pkcs11 or sdf, got %s", config.pkcs11Config.Type)
		}
		if err != nil {
			return fmt.Errorf("failed to initialize pkcs11 handle, %s", err)
		}
		config.privateKey, err = cert.ParseP11PrivKey(hsmHandle, config.userSignKeyBytes)
		if err != nil {
			return fmt.Errorf("cert.ParseP11PrivKey failed, %s", err)
		}
	} else if config.kmsConfig.Enabled {
		config.privateKey, err = kmsutils.ParseKMSPrivKey(config.userSignKeyBytes)
		if err != nil {
			return fmt.Errorf("cert.ParseKMSPrivKey failed, %s", err)
		}
		kmsEnabled = config.kmsConfig.Enabled
	} else {
		config.privateKey, err = asym.PrivateKeyFromPEM(config.userSignKeyBytes, nil)
		if err != nil {
			return fmt.Errorf("parse user key file to privateKey obj failed, %s", err)
		}
	}
	if config.privateKey.Type() == crypto.DILITHIUM2 {
		config.userPk = config.userCrt.PublicKey
	} else {
		config.userPk = config.privateKey.PublicKey()
	}

	return nil
}

func dealArchiveCenterConfig(config *ChainClientConfig) (err error) {
	return nil
}

func getDefaultLogger() *zap.SugaredLogger {
	config := log.LogConfig{
		Module:       "[SDK]",
		LogPath:      "./sdk.log",
		LogLevel:     log.LEVEL_DEBUG,
		MaxAge:       30,
		JsonFormat:   false,
		ShowLine:     true,
		LogInConsole: false,
	}

	logger, _ := log.InitSugarLogger(&config)
	return logger
}
