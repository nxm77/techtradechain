/*
 * Copyright 2020 The SealEVM Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package precompiledContracts

import (
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/storage"
)

// log level
const (
	lDebug   = 0
	lInfo    = 1
	lWarning = 2
	lError   = 3
)

// contractLog enables users to log in the contract
type contractLog struct{}

//func (c *contractLog)SetValue(v string){}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *contractLog) GasCost(input []byte) uint64 {
	return params.IdentityBaseGas
}

func (c *contractLog) Execute(input []byte, version uint32, ctx *environment.Context) ([]byte, error) {
	return printLog(input, version, ctx)
}

// printLog implements the contractLog precompile
func printLog(input []byte, version uint32, ctx *environment.Context) ([]byte, error) {
	level := input[0] - '0'
	name := storage.IntAddr2HexStr(ctx.Message.Caller, version)

	switch level {
	case lDebug:
		ctx.EvmLog.Debugf("contract[%s] version[%s] %s", name, ctx.Contract.Version, string(input[1:]))
	case lInfo:
		ctx.EvmLog.Infof("contract[%s] version[%s] %s", name, ctx.Contract.Version, string(input[1:]))
	case lWarning:
		ctx.EvmLog.Warnf("contract[%s] version[%s] %s", name, ctx.Contract.Version, string(input[1:]))
	case lError:
		ctx.EvmLog.Errorf("contract[%s] version[%s] %s", name, ctx.Contract.Version, string(input[1:]))
	default:
		ctx.EvmLog.Errorf("contract[%s] version[%s] %v", name, ctx.Contract.Version, level)
	}

	return nil, nil
}
