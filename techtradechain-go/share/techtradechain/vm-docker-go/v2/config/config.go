/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

// DockerVMConfig match vm settings in chain maker yml
type DockerVMConfig struct {
	EnableDockerVM        bool   `mapstructure:"enable_dockervm"`
	DockerVMContainerName string `mapstructure:"dockervm_container_name"`
	DockerVMMountPath     string `mapstructure:"dockervm_mount_path"`
	DockerVMLogPath       string `mapstructure:"dockervm_log_path"`
	LogInConsole          bool   `mapstructure:"log_in_console"`
	LogLevel              string `mapstructure:"log_level"`
	DockerVMUDSOpen       bool   `mapstructure:"uds_open"`
	MaxConnection         uint32 `mapstructure:"max_connection"`
	DockerVMHost          string `mapstructure:"docker_vm_host"`
	DockerVMPort          uint32 `mapstructure:"docker_vm_port"`
	MaxSendMsgSize        uint32 `mapstructure:"max_send_msg_size"`
	MaxRecvMsgSize        uint32 `mapstructure:"max_recv_msg_size"`
	DisableInstall        bool   `mapstructure:"disable_install"`
	DisableUpgrade        bool   `mapstructure:"disable_upgrade"`
}

// DockerContainerConfig docker container settings
type DockerContainerConfig struct {
	HostMountDir string
	HostLogDir   string
}

type Bool int32

const (

	// ContractsDir dir save executable contract
	ContractsDir = "contracts"
	// SockDir dir save domain socket file
	SockDir = "sock"
	// SockName domain socket file name
	SockName = "cdm.sock"

	// stateKvIterator method
	FuncKvIteratorCreate    = "createKvIterator"
	FuncKvPreIteratorCreate = "createKvPreIterator"
	FuncKvIteratorHasNext   = "kvIteratorHasNext"
	FuncKvIteratorNext      = "kvIteratorNext"
	FuncKvIteratorClose     = "kvIteratorClose"

	// keyHistoryKvIterator method
	FuncKeyHistoryIterHasNext = "keyHistoryIterHasNext"
	FuncKeyHistoryIterNext    = "keyHistoryIterNext"
	FuncKeyHistoryIterClose   = "keyHistoryIterClose"

	// int32 representation of bool
	BoolTrue  Bool = 1
	BoolFalse Bool = 0
)
