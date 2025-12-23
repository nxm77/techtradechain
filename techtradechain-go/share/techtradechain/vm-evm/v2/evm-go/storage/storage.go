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
	"encoding/hex"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/utils"
)

type Storage struct {
	ResultCache     ResultCache
	UpperStorage    *Storage
	ExternalStorage IExternalStorage
	readOnlyCache   readOnlyCache
	TransientCache  TStorage
}

func New(upperStorage *Storage, extStorage IExternalStorage) *Storage {
	s := &Storage{
		ResultCache: ResultCache{
			//OriginalData: CacheUnderAddress{},
			WriteCache: CacheUnderAddress{},
			ReadCache:  CacheUnderAddress{},
			Balance:    BalanceCache{},
			Logs:       LogCache{},
			Destructs:  Cache{},
		},
		ExternalStorage: extStorage,
		UpperStorage:    upperStorage,
		readOnlyCache: readOnlyCache{
			Code:      CodeCache{},
			CodeSize:  Cache{},
			CodeHash:  Cache{},
			BlockHash: Cache{},
		},
	}

	return s
}

func IntAddr2HexStr(addr *evmutils.Int, version uint32) string {
	if len(addr.Bytes()) < 20 {
		if version > params.V2030200 {
			fillUp := evmutils.LeftPaddingSlice(addr.Bytes(), 20)
			return hex.EncodeToString(fillUp)
		}
	}

	return hex.EncodeToString(addr.Bytes())
}

func (s *Storage) IntAddr2HexStr(addr *evmutils.Int) string {
	return IntAddr2HexStr(addr, s.GetCurrentBlockVersion())

}

func (s *Storage) SLoad2217(n *evmutils.Int, k *evmutils.Int) (*evmutils.Int, error) {
	//if s.ResultCache.OriginalData == nil || s.ResultCache.WriteCache == nil || s.ExternalStorage == nil {
	if s.ResultCache.WriteCache == nil || s.ExternalStorage == nil {
		return nil, utils.ErrStorageNotInitialized
	}

	//nsStr := hex.EncodeToString(n.Bytes())
	nsStr := s.IntAddr2HexStr(n)
	keyStr := hex.EncodeToString(k.Bytes())
	if s.GetCurrentBlockVersion() >= params.V2300 {
		nsStr = s.ExternalStorage.(*ContractStorage).Contract.Name
	}

	var err error
	i := s.ResultCache.WriteCache.Get(nsStr, keyStr)
	if i == nil {
		i, err = s.ExternalStorage.Load(nsStr, keyStr)
		if err != nil {
			return nil, utils.NoSuchDataInTheStorage(err)
		}

		//s.ResultCache.OriginalData.Set(nsStr, keyStr, i)
		if s.GetCurrentBlockVersion() != params.V220 {
			s.ResultCache.WriteCache.Set(nsStr, keyStr, i)
		}

	}

	return i, nil
}

// SLoad load data from storage, sequence: ResultCache, UpperStorage and ExternalStorage(just for the original evm)
func (s *Storage) SLoad(contractAddr *evmutils.Int, k *evmutils.Int) (*evmutils.Int, error) {
	if s.ResultCache.WriteCache == nil || s.ResultCache.ReadCache == nil ||
		(s.UpperStorage == nil && s.ExternalStorage == nil) {
		return nil, utils.ErrStorageNotInitialized
	}

	//addrStr := hex.EncodeToString(contractAddr.Bytes())
	addrStr := s.IntAddr2HexStr(contractAddr)
	keyStr := hex.EncodeToString(k.Bytes())
	version := s.GetCurrentBlockVersion()
	if version >= params.V2300 {
		addrStr = s.ExternalStorage.(*ContractStorage).Contract.Name
	}

	i := s.ResultCache.WriteCache.Get(addrStr, keyStr)
	if i != nil {
		return i, nil
	}

	if version > params.V2030200 {
		i = s.ResultCache.ReadCache.Get(addrStr, keyStr)
		if i != nil {
			return i, nil
		}
	}

	var err error
	if s.UpperStorage != nil {
		i, err = s.UpperStorage.SLoad(contractAddr, k)
		if err != nil {
			return nil, err
		}
		if i != nil {
			return i, nil
		}
	}
	if s.ExternalStorage != nil {
		i, err = s.ExternalStorage.Load(addrStr, keyStr)
		if err != nil {
			return nil, utils.NoSuchDataInTheStorage(err)
		}
	}

	if version < params.V2030201 {
		s.ResultCache.WriteCache.Set(addrStr, keyStr, i)
	} else {
		s.ResultCache.ReadCache.Set(addrStr, keyStr, i)
	}

	cs := s.ExternalStorage.(*ContractStorage)
	txId := cs.Ctx.GetTx().Payload.GetTxId()
	valStr := hex.EncodeToString(i.Bytes())
	cs.SystemLog.DebugDynamic(func() string {
		return fmt.Sprintf("evm sload, tx[%s], address[%s], key[%s], value[%v]", txId, addrStr, keyStr, valStr)
	})

	return i, nil
}

func (s *Storage) SStore(n *evmutils.Int, k *evmutils.Int, v *evmutils.Int) {
	if !v.IsInt64() && !v.IsUint64() {
		val := TruncateNullTail(v.Bytes())
		if string(val) == CrossVmOutParamsBeginKey {
			s.ExternalStorage.SetCrossVmOutParams(k, v)
			return
		}
	}

	if !s.ExternalStorage.IsCrossVmMode() {
		//nsStr := hex.EncodeToString(n.Bytes())
		nsStr := s.IntAddr2HexStr(n)
		keyStr := hex.EncodeToString(k.Bytes())
		if s.GetCurrentBlockVersion() < 2300 {
			//version < 2300, cross call occurs inside the vm, so there will be multiple contrats ant it's address
			s.ResultCache.WriteCache.Set(nsStr, keyStr, v)
		} else {
			//version >= 2300, cross call will be through the chain, so each vm has only one contract name
			estore := s.ExternalStorage.(*ContractStorage)
			s.ResultCache.WriteCache.Set(estore.Contract.Name, keyStr, v)
		}

		cs := s.ExternalStorage.(*ContractStorage)
		txId := cs.Ctx.GetTx().Payload.GetTxId()
		valStr := hex.EncodeToString(v.Bytes())
		cs.SystemLog.DebugDynamic(func() string {
			return fmt.Sprintf("evm sstore, tx[%s], address[%s], key[%s], value[%v]", txId, nsStr, keyStr, valStr)
		})
		return
	}

	s.ExternalStorage.SetCrossVmOutParams(k, v)
}

func (s *Storage) BalanceModify(address *evmutils.Int, value *evmutils.Int, neg bool) {
	//kString := hex.EncodeToString(address.Bytes())
	kString := s.IntAddr2HexStr(address)

	b, exist := s.ResultCache.Balance[kString]
	if !exist {
		b = &balance{
			Address: evmutils.FromBigInt(address.Int),
			Balance: evmutils.New(0),
		}

		s.ResultCache.Balance[kString] = b
	}

	if neg {
		b.Balance.Int.Sub(b.Balance.Int, value.Int)
	} else {
		b.Balance.Int.Add(b.Balance.Int, value.Int)
	}
}

func (s *Storage) Log(address *evmutils.Int, topics [][]byte, data []byte, context environment.Context) {
	//kString := hex.EncodeToString(address.Bytes())
	kString := s.IntAddr2HexStr(address)

	var theLog = Log{
		Topics:  topics,
		Data:    data,
		Context: context,
	}
	l := s.ResultCache.Logs[kString]
	s.ResultCache.Logs[kString] = append(l, theLog)
}

func (s *Storage) Destruct(address *evmutils.Int) {
	//s.ResultCache.Destructs[address.String()] = address
	addr := s.IntAddr2HexStr(address)
	s.ResultCache.Destructs[addr] = address
}

//type commonGetterFunc func(*evmutils.Int) (*evmutils.Int, error)
//
//func (s *Storage) commonGetter(key *evmutils.Int, cache Cache, getterFunc commonGetterFunc) (*evmutils.Int, error) {
//	//keyStr := key.String()
//	keyStr := hex.EncodeToString(key.Bytes())
//	if b, exists := cache[keyStr]; exists {
//		return evmutils.FromBigInt(b.Int), nil
//	}
//
//	b, err := getterFunc(key)
//	if err == nil {
//		cache[keyStr] = b
//	}
//
//	return b, err
//}

func (s *Storage) Balance(address *evmutils.Int) (*evmutils.Int, error) {
	return s.ExternalStorage.GetBalance(address)
}
func (s *Storage) SetCode(address *evmutils.Int, code []byte) {
	//keyStr := hex.EncodeToString(address.Bytes())
	keyStr := s.IntAddr2HexStr(address)
	s.readOnlyCache.Code[keyStr] = code
}
func (s *Storage) GetCode(address *evmutils.Int) ([]byte, error) {
	//keyStr := hex.EncodeToString(address.Bytes())
	keyStr := s.IntAddr2HexStr(address)
	if b, exists := s.readOnlyCache.Code[keyStr]; exists {
		return b, nil
	}

	//Read the contract code from the chain through the external interface
	b, err := s.ExternalStorage.GetCode(address)
	if err == nil {
		s.readOnlyCache.Code[keyStr] = b
	}

	return b, err
}
func (s *Storage) SetCodeSize(address *evmutils.Int, size *evmutils.Int) {
	//keyStr := address.String()
	//keyStr := hex.EncodeToString(address.Bytes())
	keyStr := s.IntAddr2HexStr(address)
	s.readOnlyCache.CodeSize[keyStr] = size
}
func (s *Storage) GetCodeSize(address *evmutils.Int) (*evmutils.Int, error) {
	//keyStr := hex.EncodeToString(address.Bytes())
	keyStr := s.IntAddr2HexStr(address)
	if size, exists := s.readOnlyCache.CodeSize[keyStr]; exists {
		return size, nil
	}

	size, err := s.ExternalStorage.GetCodeSize(address)
	if err == nil {
		s.readOnlyCache.CodeSize[keyStr] = size
	}

	return size, err
}
func (s *Storage) SetCodeHash(address *evmutils.Int, codeHash *evmutils.Int) {
	//keyStr := hex.EncodeToString(address.Bytes())
	keyStr := s.IntAddr2HexStr(address)
	s.readOnlyCache.CodeHash[keyStr] = codeHash
}
func (s *Storage) GetCodeHash(address *evmutils.Int) (*evmutils.Int, error) {
	//keyStr := hex.EncodeToString(address.Bytes())
	keyStr := s.IntAddr2HexStr(address)
	if hash, exists := s.readOnlyCache.CodeHash[keyStr]; exists {
		return hash, nil
	}

	hash, err := s.ExternalStorage.GetCodeHash(address)
	if err == nil {
		s.readOnlyCache.CodeHash[keyStr] = hash
	}

	return hash, err
}

func (s *Storage) GetBlockHash(block *evmutils.Int) (*evmutils.Int, error) {
	//keyStr := block.String()
	keyStr := hex.EncodeToString(block.Bytes())
	if hash, exists := s.readOnlyCache.BlockHash[keyStr]; exists {
		return hash, nil
	}

	hash, err := s.ExternalStorage.GetBlockHash(block)
	if err == nil {
		s.readOnlyCache.BlockHash[keyStr] = hash
	}

	return hash, err
}

//func (s *Storage) CreateAddress(name *evmutils.Int, addrType int32) *evmutils.Int {
//	return s.ExternalStorage.CreateAddress(name, addrType)
//}

func (s *Storage) GetCurrentBlockVersion() uint32 {
	return s.ExternalStorage.GetCurrentBlockVersion()
}

func (s *Storage) CreateFixedAddress(caller *evmutils.Int, salt *evmutils.Int, tx environment.Transaction, addrType int32) *evmutils.Int {
	return s.ExternalStorage.CreateFixedAddress(caller, salt, tx, addrType)
}
