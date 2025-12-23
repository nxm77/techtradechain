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

package storage

import (
	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
)

// Teh External Storage,provding a Storage for touching out of current evm
type IExternalStorage interface {
	GetBalance(address *evmutils.Int) (*evmutils.Int, error)
	GetCode(address *evmutils.Int) ([]byte, error)
	GetCodeSize(address *evmutils.Int) (*evmutils.Int, error)
	GetCodeHash(address *evmutils.Int) (*evmutils.Int, error)
	GetBlockHash(block *evmutils.Int) (*evmutils.Int, error)
	GetCurrentBlockVersion() uint32

	//CreateAddress(name *evmutils.Int, addrType int32) *evmutils.Int
	CreateFixedAddress(caller *evmutils.Int, salt *evmutils.Int, tx environment.Transaction, addrType int32) *evmutils.Int

	CanTransfer(from *evmutils.Int, to *evmutils.Int, amount *evmutils.Int) bool

	Load(n string, k string) (*evmutils.Int, error)
	Store(address string, key string, val []byte)
	SetCrossVmOutParams(index *evmutils.Int, param *evmutils.Int)
	IsCrossVmMode() bool
	//GetCrossVmInParam(key string) []byte
	CallContract(name string, rtType int32, method string, byteCode []byte, parameters map[string][]byte,
		gasUsed uint64, isCreate bool) (res *common.ContractResult, stat common.TxStatusCode)
}
