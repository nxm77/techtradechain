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
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
)

const CrossVmOutParamsBeginKey = "CrossVMCall"

//const CrossVmOutParamsEndKey = "CrossVMOutParamsEnd"
//const CrossVmInParamsBeginKey = "CrossVMInParamsBegin"
//const CrossVmInParamsEndKey = "CrossVMInParamsEnd"
const CrossVmCallMethodKey = "CrossVmCallMethod"

//const CrossVmCallDispatchKey = "crossVmCallDispatch()"

type Params map[string][]byte

//type AddrParams map[string]Params

type CrossVmParams struct {
	ParamsCache  Params
	IsCrossVm    bool
	ParamsBegin  int64
	LongStrLen   int64
	LastParamKey string
}

func TruncateNullTail(val []byte) []byte {
	b := val[:]
	for i, e := range val {
		if e == 0 {
			b = val[:i]
			break
		}
	}
	return b
}

func NewParamsCache() *CrossVmParams {
	return &CrossVmParams{
		ParamsCache:  Params{},
		IsCrossVm:    false,
		ParamsBegin:  0,
		LongStrLen:   0,
		LastParamKey: "",
	}
}

func (p *CrossVmParams) GetParam(key string) []byte {
	return p.ParamsCache[key]
}

func (p *CrossVmParams) SetParam(key string, value []byte) {
	p.ParamsCache[key] = value
}

func (p *CrossVmParams) ResetLongStrParamStatus() {
	p.LongStrLen = 0
	p.LastParamKey = ""
}

func (p *CrossVmParams) Reset() {
	p.ParamsCache = Params{}
	p.IsCrossVm = false
	p.ParamsBegin = 0
	p.ResetLongStrParamStatus()
}

func (p *CrossVmParams) GetParamsCnt() int {
	return len(p.ParamsCache)
}

type Cache map[string]*evmutils.Int
type CacheUnderAddress map[string]Cache

func (c CacheUnderAddress) Get(address string, key string) *evmutils.Int {
	if c[address] == nil {
		return nil
	} else {
		return c[address][key]
	}
}

func (c CacheUnderAddress) Set(address string, key string, v *evmutils.Int) {
	if c[address] == nil {
		c[address] = Cache{}
	}

	c[address][key] = v
}

type balance struct {
	Address *evmutils.Int
	Balance *evmutils.Int
}

type BalanceCache map[string]*balance

type Log struct {
	Topics  [][]byte
	Data    []byte
	Context environment.Context
}

type LogCache map[string][]Log

const HashLen = 32

type Hash [HashLen]byte
type TEntry map[Hash]Hash

//transient storage
type TStorage map[*evmutils.Int]TEntry

// NewTStorage creates a new instance of a TStorage.
func NewTStorage() TStorage {
	return make(TStorage)
}

// Set sets the transient storage `value` for `key` at the given `addr`.
func (t TStorage) Set(addr *evmutils.Int, key, value Hash) {
	if _, ok := t[addr]; !ok {
		t[addr] = make(TEntry)
	}
	t[addr][key] = value
}

// Get gets the transient storage for `key` at the given `addr`.
func (t TStorage) Get(addr *evmutils.Int, key Hash) Hash {
	val, ok := t[addr]
	if !ok {
		return Hash{}
	}
	return val[key]
}

type ResultCache struct {
	//OriginalData CacheUnderAddress
	ReadCache     CacheUnderAddress
	WriteCache    CacheUnderAddress //when version<=232, include read cache; when version>232, only include write
	Balance       BalanceCache
	Logs          LogCache
	Destructs     Cache
	ContractEvent []*commonPb.ContractEvent
}

type CodeCache map[string][]byte

type readOnlyCache struct {
	Code      CodeCache
	CodeSize  Cache
	CodeHash  Cache
	BlockHash Cache
}

func MergeResultCache2211(src *ResultCache, to *ResultCache) {
	// fix bug for multi cross call of contract
	for k, v := range src.WriteCache {
		vTo, exist := to.WriteCache[k]
		if !exist {
			to.WriteCache[k] = v
		} else {
			vSrc := (map[string]*evmutils.Int)(v)
			for vKSrc, vVSrc := range vSrc {
				vTo[vKSrc] = vVSrc
			}
		}
	}

	for k, v := range src.Balance {
		if to.Balance[k] != nil {
			to.Balance[k].Balance.Add(v.Balance)
		} else {
			to.Balance[k] = v
		}
	}

	for k, v := range src.Logs {
		to.Logs[k] = append(to.Logs[k], v...)
	}

	for k, v := range src.Destructs {
		to.Destructs[k] = v
	}
}

func MergeResultCache(src *ResultCache, to *ResultCache) {
	for k, v := range src.WriteCache {
		to.WriteCache[k] = v
	}

	for k, v := range src.Balance {
		if to.Balance[k] != nil {
			to.Balance[k].Balance.Add(v.Balance)
		} else {
			to.Balance[k] = v
		}
	}

	for k, v := range src.Logs {
		to.Logs[k] = append(to.Logs[k], v...)
	}

	for k, v := range src.Destructs {
		to.Destructs[k] = v
	}
}
