/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package utils

import (
	"fmt"
	"strings"

	"techtradechain.com/techtradechain/vm-docker-go/v2/config"
)

const (
	DefaultMaxSendSize   = 20
	DefaultMaxRecvSize   = 20
	DefaultMaxConnection = 1
)

func SplitContractName(contractNameAndVersion string) string {
	contractName := strings.Split(contractNameAndVersion, "#")[1]
	return contractName
}

func GetMaxSendMsgSizeFromConfig(config *config.DockerVMConfig) uint32 {
	if config.MaxSendMsgSize < DefaultMaxSendSize {
		return DefaultMaxSendSize
	}
	return config.MaxSendMsgSize
}

func GetMaxRecvMsgSizeFromConfig(config *config.DockerVMConfig) uint32 {
	if config.MaxRecvMsgSize < DefaultMaxRecvSize {
		return DefaultMaxRecvSize
	}
	return config.MaxRecvMsgSize
}

func GetURLFromConfig(config *config.DockerVMConfig) string {
	ip := config.DockerVMHost
	port := config.DockerVMPort
	if ip == "" {
		ip = "127.0.0.1"
	}
	if port == 0 {
		port = 22359
	}
	url := fmt.Sprintf("%s:%d", ip, port)
	return url
}

func GetMaxConnectionFromConfig(config *config.DockerVMConfig) uint32 {
	if config.MaxConnection == 0 {
		return DefaultMaxConnection
	}
	return config.MaxConnection
}

// ConstructReceiveMapKey contractName#txId
func ConstructReceiveMapKey(names ...string) string {
	return strings.Join(names, "#")
}
