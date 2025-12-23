/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package vm

import (
	"strings"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	techtradechainVM "techtradechain.com/techtradechain/vm/v2"
)

var vmProviders = make(map[string]techtradechainVM.Provider)

func RegisterVmProvider(t string, f techtradechainVM.Provider) {
	vmProviders[strings.ToUpper(t)] = f
	techtradechainVM.RegisterVmProvider(t, f)
}

func GetVmProvider(t string) techtradechainVM.Provider {
	provider, ok := vmProviders[strings.ToUpper(t)]
	if !ok {
		return nil
	}
	return provider
}

const (
	VmTypeGasm   = "GASM"
	VmTypeWasmer = "WASMER"
	VmTypeEvm    = "EVM"
	VmTypeWxvm   = "WXVM"
)

var VmTypeToRunTimeType = map[string]common.RuntimeType{
	"GASM":     common.RuntimeType_GASM,
	"WASMER":   common.RuntimeType_WASMER,
	"WXVM":     common.RuntimeType_WXVM,
	"EVM":      common.RuntimeType_EVM,
	"DOCKERGO": common.RuntimeType_DOCKER_GO,
	"JAVA":     common.RuntimeType_JAVA,
	"GO":       common.RuntimeType_GO,
}

var RunTimeTypeToVmType = map[common.RuntimeType]string{
	common.RuntimeType_GASM:      "GASM",
	common.RuntimeType_WASMER:    "WASMER",
	common.RuntimeType_WXVM:      "WXVM",
	common.RuntimeType_EVM:       "EVM",
	common.RuntimeType_DOCKER_GO: "DOCKERGO",
	common.RuntimeType_JAVA:      "JAVA",
	common.RuntimeType_GO:        "GO",
}
