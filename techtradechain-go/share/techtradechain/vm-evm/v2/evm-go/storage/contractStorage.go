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
	"strconv"

	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"

	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
)

type ContractStorage struct {
	OutParams *CrossVmParams
	//InParams        *CrossVmParams
	ResultCache ResultCache
	//ExternalStorage IExternalStorage
	readOnlyCache readOnlyCache
	Ctx           protocol.TxSimContext
	BlockHash     *evmutils.Int
	Contract      *common.Contract // contract info
	SystemLog     protocol.Logger
}

func NewStorage(extStorage IExternalStorage) *ContractStorage {
	s := &ContractStorage{
		ResultCache: ResultCache{
			//OriginalData: CacheUnderAddress{},
			WriteCache: CacheUnderAddress{},
			ReadCache:  CacheUnderAddress{},
			Balance:    BalanceCache{},
			Logs:       LogCache{},
			Destructs:  Cache{},
		},
		//ExternalStorage: extStorage,
		readOnlyCache: readOnlyCache{
			Code:      CodeCache{},
			CodeSize:  Cache{},
			CodeHash:  Cache{},
			BlockHash: Cache{},
		},
	}
	return s
}

//func (c *ContractStorage) GetBalance(address *evmutils.Int) (*evmutils.Int, error) {
//	return evmutils.New(0), nil
//}

func (c *ContractStorage) GetBalance(address *evmutils.Int) (*evmutils.Int, error) {
	v := c.GetCurrentBlockVersion()
	if v <= params.V2217 || v == params.V2300 || v == params.V2030100 {
		//2300 and 2310 have been released, but 2217 has found bugs, so versions before 2218, as well as 2300 and 2310
		//that have been released, use the old logic, and other versions use the new logic
		return evmutils.New(0), nil
	}

	var manager common.Contract
	method := syscontract.GasAccountFunction_GET_BALANCE.String()
	manager.Name = syscontract.SystemContract_ACCOUNT_MANAGER.String()
	manager.Status = common.ContractStatus_NORMAL

	parameters := make(map[string][]byte)
	//parameters["address_key"] = []byte(hex.EncodeToString(address.Bytes()))
	parameters["address_key"] = []byte(IntAddr2HexStr(address, v))

	b := evmutils.New(0)
	res, _, stat := c.Ctx.CallContract(c.Contract, &manager, method, nil, parameters, 0,
		common.TxType_INVOKE_CONTRACT)
	if stat != common.TxStatusCode_SUCCESS {
		return b, fmt.Errorf("failed to get balance of %s", hex.EncodeToString(address.Bytes()))
	}

	b.SetBytes(res.Result)
	return b, nil
}

func (c *ContractStorage) CanTransfer(from, to, val *evmutils.Int) bool {
	return false
}

func (c *ContractStorage) GetCode(address *evmutils.Int) (code []byte, err error) {
	//return utils.GetContractBytecode(c.Ctx.Get, address.String())
	key := IntAddr2HexStr(address, c.GetCurrentBlockVersion())
	if c.GetCurrentBlockVersion() >= params.V2220 {
		////version >= v2.2.0 code stored by name, so get contract first
		//contract, err := c.Ctx.GetContractByName(hex.EncodeToString(address.Bytes()))
		contract, err := c.Ctx.GetContractByName(key)
		if err != nil {
			return nil, err
		}
		key = contract.Name
	}

	return c.Ctx.GetContractBytecode(key)
}

func (c *ContractStorage) GetCodeSize(address *evmutils.Int) (size *evmutils.Int, err error) {
	code, err := c.GetCode(address)
	if err != nil {
		c.SystemLog.Error("failed to get other contract code size :", err.Error())
		return nil, err
	}
	return evmutils.New(int64(len(code))), err
}

func (c *ContractStorage) GetCodeHash(address *evmutils.Int) (codeHase *evmutils.Int, err error) {
	code, err := c.GetCode(address)
	if err != nil {
		c.SystemLog.Error("failed to get other contract code hash :", err.Error())
		return nil, err
	}
	hash := evmutils.Keccak256(code)
	i := evmutils.New(0)
	i.SetBytes(hash)
	return i, err
	//return evmutils.New(int64(len(code))), err
}

func (c *ContractStorage) GetBlockHash(block *evmutils.Int) (*evmutils.Int, error) {
	currentHight := c.Ctx.GetBlockHeight() - 1
	high := evmutils.MinI(int64(currentHight), block.Int64())
	Block, err := c.Ctx.GetBlockchainStore().GetBlock(uint64(high))
	if err != nil {
		return evmutils.New(0), err
	}
	hash, err := evmutils.HashBytesToEVMInt(Block.GetHeader().GetBlockHash())
	if err != nil {
		return evmutils.New(0), err
	}
	return hash, nil
}

func (c *ContractStorage) GetCurrentBlockVersion() uint32 {
	return c.Ctx.GetBlockVersion()
}

////Create a unified address generation method within EVM to avoid duplicate wheels
//func generateAddress(data []byte, addrType int32) *evmutils.Int {
//	if addrType == int32(config.AddrType_ZXL) {
//		addr, _ := evmutils.ZXAddress(data)
//		return evmutils.FromHexString(addr[2:])
//	} else {
//		return evmutils.MakeAddress(data)
//	}
//}

//This is mostly called after 2300
//func (c *ContractStorage) CreateAddress(name *evmutils.Int, addrType int32) *evmutils.Int {
//	//in seal abc smart assets application, we always create fixed contract address.
//	data := name.Bytes()
//	//return generateAddress(data, addrType)
//	addr, _ := utils.GenerateAddrInt(data, config.AddrType(addrType))
//	return addr
//}

// Only versions < 2300 are called
func (c *ContractStorage) CreateFixedAddress(caller *evmutils.Int, salt *evmutils.Int, tx environment.Transaction, addrType int32) *evmutils.Int {
	data := append(caller.Bytes(), tx.TxHash...)
	if salt != nil {
		data = append(data, salt.Bytes()...)
	}

	//return generateAddress(data, addrType)
	addr, _ := utils.NameToAddrInt(string(data), config.AddrType(addrType), 2299)
	return addr
}

func (c *ContractStorage) Load(n string, k string) (*evmutils.Int, error) {
	var val []byte
	var err error
	if c.Ctx.GetBlockVersion() < 2300 {
		//version < 2300, cross call occurs inside the vm, so there will be multiple contrats ant it's address
		val, err = c.Ctx.Get(n, []byte(k))
	} else {
		//version >= 2300, cross call will be through the chain, so each vm has only one contract name
		val, err = c.Ctx.Get(c.Contract.Name, []byte(k))
	}

	if err != nil {
		return nil, err
	}

	r := evmutils.New(0)
	r.SetBytes(val)

	return r, err
}

func (c ContractStorage) Store(address string, key string, val []byte) {
	if c.Ctx.GetBlockVersion() < 2300 {
		//version < 2300, cross call occurs inside the vm, so there will be multiple contrats ant it's address
		_ = c.Ctx.Put(address, []byte(key), val)
	} else {
		//version >= 2300, cross call will be through the chain, so each vm has only one contract name
		err := c.Ctx.Put(c.Contract.Name, []byte(key), val)
		if err != nil && c.Ctx.GetBlockVersion() >= params.V2030500 {
			panic(fmt.Sprintf("store failed, contract[%s], key[%s], value[%s]", c.Contract.Name, key, string(val)))
		}
	}
}

func (c ContractStorage) IsCrossVmMode() bool {
	//Query whether the parameter transfer mode of cross-vm contract invocation is enabled, which is used by
	//sload directives and sstore directives to distinguish regular state read/write from read/write parameters
	return c.OutParams.IsCrossVm
}

//func (c ContractStorage) GetCrossVmInParam(key string) []byte {
//	return c.InParams.ParamsCache[key]
//}

func (c *ContractStorage) SetCrossVmOutParams(index *evmutils.Int, element *evmutils.Int) {
	var val []byte
	if !element.IsInt64() && !element.IsUint64() {
		//If the element is not a number, the element value is obtained after whitespace is removed
		val = TruncateNullTail(element.Bytes())
	}

	if string(val) == CrossVmOutParamsBeginKey {
		//Turn on the pass parameter flag if the element is invoked by an external cross-vm contract
		c.OutParams.IsCrossVm = true
		//Marks the slot at which the parameter is started
		c.OutParams.ParamsBegin = index.Int64()
		//OutParams starts writing
		c.OutParams.SetParam(CrossVmOutParamsBeginKey, []byte("start"))
		return
	}

	if (index.Int64()-c.OutParams.ParamsBegin == 1) && !element.IsInt64() {
		//The 0th element is the cross-vm contract invocation token, and the first element is the invoked method
		c.OutParams.SetParam(CrossVmCallMethodKey, val)
		return
	}

	if index.IsInt64() {
		if (index.Int64()-c.OutParams.ParamsBegin)%2 == 0 { //element is param's key
			c.OutParams.LastParamKey = string(val)
		} else { //elementis param's value
			if element.IsInt64() {
				//If element is an integer, it could be the value of a param or the length of a long string of more than 32 bytes
				value := strconv.FormatInt(element.Int64(), 10)
				c.OutParams.SetParam(c.OutParams.LastParamKey, []byte(value))
			} else {
				c.OutParams.SetParam(c.OutParams.LastParamKey, val)
			}
		}
	} else { //When index is not a numeric subscript, element is a fragment of a long string
		value := val
		if c.OutParams.LongStrLen == 0 {
			//If the string is marked 0, then the last stored element is the length of the string,
			//and the current element is the first segment of the string
			num := c.OutParams.GetParam(c.OutParams.LastParamKey)
			n, err := strconv.ParseInt(string(num), 10, 64)
			if err != nil && c.GetCurrentBlockVersion() >= params.V2030500 {
				panic("parse cross vm param length failed")
			}

			c.OutParams.LongStrLen = n
			c.OutParams.SetParam(c.OutParams.LastParamKey, val)
		} else { //Element is a subsequent fragment of a long string
			value = append(c.OutParams.GetParam(c.OutParams.LastParamKey), val...)
			c.OutParams.SetParam(c.OutParams.LastParamKey, value)
		}

		if int64(len(value)*2+1) == c.OutParams.LongStrLen {
			//Reset long parameters that exceed 32 bytes
			c.OutParams.ResetLongStrParamStatus()
		}
	}
}

func (c ContractStorage) CallContract(name string, rtType int32, method string, byteCode []byte,
	parameters map[string][]byte, gasUsed uint64, isCreate bool) (res *common.ContractResult, stat common.TxStatusCode) {

	//Parameter storage mode is enabled only when the contract is invoked across virtual machines
	if c.OutParams.IsCrossVm {
		for k, v := range c.OutParams.ParamsCache {
			parameters[k] = v
		}
	}

	caller := &common.Contract{
		Address: string(parameters[syscontract.CrossParams_SENDER.String()]),
	}

	if isCreate {
		//If the cross-contract invocation is creating the contract, you are actually calling
		//the installContract method that manages the contract
		var contract common.Contract
		method = syscontract.ContractManageFunction_INIT_CONTRACT.String()
		contract.Name = syscontract.SystemContract_CONTRACT_MANAGE.String()
		contract.Status = common.ContractStatus_NORMAL

		parameters[syscontract.InitContract_CONTRACT_NAME.String()] = []byte(name)
		parameters[syscontract.InitContract_CONTRACT_VERSION.String()] = []byte("1.0.0")
		parameters[syscontract.InitContract_CONTRACT_RUNTIME_TYPE.String()] = []byte(common.RuntimeType(rtType).String())
		parameters[syscontract.InitContract_CONTRACT_BYTECODE.String()] = byteCode
		res, _, stat = c.Ctx.CallContract(caller, &contract, method, byteCode, parameters, gasUsed, common.TxType_INVOKE_CONTRACT)
	} else {
		contract, err := c.Ctx.GetContractByName(name)
		if err != nil && c.Ctx.GetBlockVersion() >= params.V2030500 {
			return &common.ContractResult{
				Code:    uint32(1),
				Result:  nil,
				Message: fmt.Sprintf("get contract by name[%s] failed", name),
				GasUsed: gasUsed,
			}, common.TxStatusCode_CONTRACT_FAIL

		}
		if c.OutParams.IsCrossVm {
			//Method is not required to create a contract. The init_contract method is automatically called
			method = string(parameters[CrossVmCallMethodKey])
		}

		res, _, stat = c.Ctx.CallContract(caller, contract, method, byteCode, parameters, gasUsed, common.TxType_INVOKE_CONTRACT)
	}

	c.OutParams.Reset()
	return res, stat
}
