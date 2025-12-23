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
	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"
)

type PrecompiledContract interface {
	//SetValue(v string)
	GasCost(input []byte) uint64
	Execute(input []byte, version uint32, ctx *environment.Context) ([]byte, error)
}

// const ContractsMaxAddress = 16
const EthPrecompiledMinAddr = 1
const EthPrecompiledMaxAddr = 9

// const ExtPrecompiled2030100MinAddr = 10
const ExtPrecompiled2030100MaxAddr = 15
const ExtPrecompiled2030200MinAddr = 1000
const ExtPrecompiled2030200MaxAddr = 1007

// var Contracts = [ContractsMaxAddress]PrecompiledContract{
var Contracts = map[uint64]PrecompiledContract{
	1:    &ecRecover{},
	2:    &sha256hash{},
	3:    &ripemd160hash{},
	4:    &dataCopy{},
	5:    &bigModExp{},
	6:    &bn256AddIstanbul{},
	7:    &bn256ScalarMulIstanbul{},
	8:    &bn256PairingIstanbul{},
	9:    &blake2F{},
	10:   &senderOrgId{},
	11:   &senderRole{},
	12:   &senderPk{},
	13:   &creatorOrgId{},
	14:   &creatorRole{},
	15:   &creatorPk{},
	1007: &signVerify{},
}

var ContractsNew = map[uint64]PrecompiledContract{
	1:    &ecRecover{},
	2:    &sha256hash{},
	3:    &ripemd160hash{},
	4:    &dataCopy{},
	5:    &bigModExp{},
	6:    &bn256AddIstanbul{},
	7:    &bn256ScalarMulIstanbul{},
	8:    &bn256PairingIstanbul{},
	9:    &blake2F{},
	1000: &senderOrgId{},
	1001: &senderRole{},
	1002: &senderPk{},
	1003: &creatorOrgId{},
	1004: &creatorRole{},
	1005: &creatorPk{},
	1006: &contractLog{},
	1007: &signVerify{},
}

func IsPrecompiledContract(address *evmutils.Int, version uint32) bool {
	if address.IsUint64() {
		addr := address.Uint64()
		if version <= params.V2030100 {
			return addr <= ExtPrecompiled2030100MaxAddr
		} else {
			return (EthPrecompiledMinAddr <= addr && addr <= EthPrecompiledMaxAddr) ||
				(ExtPrecompiled2030200MinAddr <= addr && addr <= ExtPrecompiled2030200MaxAddr)
		}
	}

	return false
}
