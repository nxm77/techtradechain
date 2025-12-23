/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import "time"

// VMConfig is the vm config
var VMConfig *DockerVMConfig

// DockerVMConfig match vm settings in chain maker yml
type DockerVMConfig struct {
	EnableDockerVM    bool   `mapstructure:"enable"`          // enable docker go virtual machine
	DockerVMMountPath string `mapstructure:"data_mount_path"` // mount point in techtradechain
	DockerVMLogPath   string `mapstructure:"log_mount_path"`  // log point in techtradechain

	// unix domain socket open, used for techtradechain and docker manager communication
	ConnectionProtocol string `mapstructure:"protocol"`

	// uds_open true
	// 可自定义交易执行超时时间，但是tx_scheduler_timeout是否有冲突，保留配置，但不启用
	TxTimeout uint32 `mapstructure:"tx_timeout"`

	MaxConcurrency uint32 `mapstructure:"max_concurrency"`   // max process num
	MaxSendMsgSize uint32 `mapstructure:"max_send_msg_size"` // grpc max send message size, Unit: MB
	MaxRecvMsgSize uint32 `mapstructure:"max_recv_msg_size"` // grpc max recv message size, Unit: MB
	// uds_open false， tcp
	RuntimeServer  RuntimeServerConfig  `mapstructure:"runtime_server"`  // runtime server
	ContractEngine ContractEngineConfig `mapstructure:"contract_engine"` // contract engine
	Slow           SlowConfig           `mapstructure:"slow_log"`        // slow tx config
}

// RuntimeServerConfig is the runtime server config
type RuntimeServerConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

// ContractEngineConfig is the contract engine config
type ContractEngineConfig struct {
	Host          string `mapstructure:"host"`
	Port          int    `mapstructure:"port"`
	MaxConnection uint64 `mapstructure:"max_connection"`
}

// SlowConfig is the slow tx log
type SlowConfig struct {
	Disable  bool `mapstructure:"disable"`
	StepTime int  `mapstructure:"step_time"`
	TxTime   int  `mapstructure:"tx_time"`
}

// DockerContainerConfig docker container settings
type DockerContainerConfig struct {
	HostMountDir string
	HostLogDir   string
}

// Bool is the int32 type of bool
type Bool int32

const (
	//ENV_ENABLE_UDS             = "ENV_ENABLE_UDS"
	//ENV_USER_NUM               = "ENV_USER_NUM"
	//ENV_TX_TIME_LIMIT          = "ENV_TX_TIME_LIMIT"
	//ENV_LOG_LEVEL              = "ENV_LOG_LEVEL"
	//ENV_LOG_IN_CONSOLE         = "ENV_LOG_IN_CONSOLE"
	//ENV_MAX_CONCURRENCY        = "ENV_MAX_CONCURRENCY"
	//ENV_ENABLE_PPROF           = "ENV_ENABLE_PPROF"
	//ENV_PPROF_PORT             = "ENV_PPROF_PORT"
	//ENV_MAX_SEND_MSG_SIZE      = "ENV_MAX_SEND_MSG_SIZE"
	//ENV_MAX_RECV_MSG_SIZE      = "ENV_MAX_RECV_MSG_SIZE"
	//ENV_MAX_LOCAL_CONTRACT_NUM = "ENV_MAX_LOCAL_CONTRACT_NUM"

	// DefaultMaxSendSize is the default max send size
	DefaultMaxSendSize = 100
	// DefaultMaxRecvSize is the default max recv size
	DefaultMaxRecvSize = 100
	//DefaultTxTimeLimit         = 5
	//DefaultMaxConcurrency      = 50
	//DefaultMaxLocalContractNum = 1024

	// ContractsDir dir save executable contract
	ContractsDir = "contract-bins"
	// SockDir dir save domain socket file
	SockDir = "contract-engine-sock"
	// EngineSockName domain socket file name
	EngineSockName = "chain.sock"

	//DockerConfigDir = "config"

	// RuntimeSockName is the runtime sock name
	RuntimeSockName = "runtime.sock"
	// RuntimeSockDir is the runtime sock dir
	RuntimeSockDir = "runtime-sock"

	// TestPort for contract engine
	TestPort = "22356"

	// FuncKvIteratorCreate create kv iter
	FuncKvIteratorCreate = "createKvIterator"
	// FuncKvPreIteratorCreate create pre kv iter
	FuncKvPreIteratorCreate = "createKvPreIterator"
	// FuncKvIteratorHasNext judge kv iter has next
	FuncKvIteratorHasNext = "kvIteratorHasNext"
	// FuncKvIteratorNext get kv iter next
	FuncKvIteratorNext = "kvIteratorNext"
	// FuncKvIteratorClose close kv iter
	FuncKvIteratorClose = "kvIteratorClose"

	// FuncKeyHistoryIterHasNext judge history kv iter has next
	FuncKeyHistoryIterHasNext = "keyHistoryIterHasNext"
	// FuncKeyHistoryIterNext get history kv iter next
	FuncKeyHistoryIterNext = "keyHistoryIterNext"
	// FuncKeyHistoryIterClose close kv iter
	FuncKeyHistoryIterClose = "keyHistoryIterClose"

	// BoolTrue is the int32 representation of true
	BoolTrue Bool = 1
	// BoolFalse is the int32 representation of false
	BoolFalse Bool = 0

	// ServerMinInterval server min interval
	ServerMinInterval = time.Duration(1) * time.Minute
	// ConnectionTimeout connection timeout time
	ConnectionTimeout = 5 * time.Second

	// TCPProtocol is tcp connection protocol
	TCPProtocol = "tcp"
	// UDSProtocol is uds connection protocol
	UDSProtocol = "uds"

	// DefaultTxTimeout is default tx timeout
	DefaultTxTimeout = 9

	// DefaultSlowStepLogTime is default slow step log time
	DefaultSlowStepLogTime = 3000

	// DefaultSlowTxLogTime is default slow tx log time
	DefaultSlowTxLogTime = 6000
)

const (
	// KeyContractFullName is the key contract full name
	KeyContractFullName = "KEY_CONTRACT_FULL_NAME"
	// KeySenderAddr is the key sender addr
	KeySenderAddr = "KEY_SENDER_ADDR"

	// KeyCallContractResp is the key call contract resp
	KeyCallContractResp = "KEY_CALL_CONTRACT_RESPONSE"
	// KeyCallContractReq is the key call contract req
	KeyCallContractReq = "KEY_CALL_CONTRACT_REQUEST"

	// KeyStateKey is the key state key
	KeyStateKey = "KEY_STATE_KEY"
	// KeyUserKey is the key user key
	KeyUserKey = "KEY_USER_KEY"
	// KeyUserField is the key user field
	KeyUserField = "KEY_USER_FIELD"
	// KeyStateValue is the key state value
	KeyStateValue = "KEY_STATE_VALUE"

	// KeyKVIterKey is the KV iter key
	KeyKVIterKey = "KEY_KV_ITERATOR_KEY"
	// KeyIterIndex is the key iter index
	KeyIterIndex = "KEY_KV_ITERATOR_INDEX"

	// KeyHistoryIterKey is the key history iter key
	KeyHistoryIterKey = "KEY_HISTORY_ITERATOR_KEY"
	// KeyHistoryIterField is the key history iter field
	KeyHistoryIterField = "KEY_HISTORY_ITERATOR_FIELD"
	//KeyHistoryIterIndex = "KEY_HISTORY_ITERATOR_INDEX"

	// KeyContractName is the key contract name
	KeyContractName = "KEY_CONTRACT_NAME"
	// KeyIteratorFuncName is the key iter func name
	KeyIteratorFuncName = "KEY_ITERATOR_FUNC_NAME"
	// KeyIterStartKey is the key iter start key
	KeyIterStartKey = "KEY_ITERATOR_START_KEY"
	// KeyIterStartField is the key iter start field
	KeyIterStartField = "KEY_ITERATOR_START_FIELD"
	// KeyIterLimitKey is the key iter limit key
	KeyIterLimitKey = "KEY_ITERATOR_LIMIT_KEY"
	// KeyIterLimitField is the key limit field
	KeyIterLimitField = "KEY_ITERATOR_LIMIT_FIELD"
	// KeyWriteMap is the key write map
	KeyWriteMap = "KEY_WRITE_MAP"
	// KeyIteratorHasNext is the key iter has next
	KeyIteratorHasNext = "KEY_ITERATOR_HAS_NEXT"

	// KeyTxId is key tx id
	KeyTxId = "KEY_TX_ID"
	// KeyBlockHeight is key block height
	KeyBlockHeight = "KEY_BLOCK_HEIGHT"
	// KeyIsDelete judge key deleted
	KeyIsDelete = "KEY_IS_DELETE"
	// KeyTimestamp is key timestamp
	KeyTimestamp = "KEY_TIMESTAMP"
)

// BufferSize set grpc buffer size to 1M, only between sandbox and engine, sandbox and runtime server
const BufferSize = 1024 * 1024
