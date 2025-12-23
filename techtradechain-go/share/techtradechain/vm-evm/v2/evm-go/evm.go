/*
 * Copyright (c) 2021.  BAEC.ORG.CN All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package evm_go

import (
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/instructions"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/memory"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/opcodes"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/precompiledContracts"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/stack"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/storage"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/utils"
)

type EVMResultCallback func(result *ExecuteResult, err error)
type EVMParam struct {
	MaxStackDepth  int
	ExternalStore  storage.IExternalStorage
	UpperStorage   *storage.Storage
	ResultCallback EVMResultCallback
	Context        *environment.Context
}

type EVM struct {
	stack        *stack.Stack
	memory       *memory.Memory
	storage      *storage.Storage //same pointer of instructionsContext.storage
	context      *environment.Context
	instructions instructions.IInstructions
	resultNotify EVMResultCallback
}

type ExecuteResult struct {
	ResultData   []byte
	GasLeft      uint64
	StorageCache storage.ResultCache
	ExitOpCode   opcodes.OpCode
	ByteCodeHead []byte
	ByteCodeBody []byte
}

func init() {
	Load()
}
func Load() {
	instructions.Load()
}

func New(param EVMParam) *EVM {
	if param.Context.Block.GasLimit.Cmp(param.Context.Transaction.GasLimit.Int) < 0 {
		param.Context.Transaction.GasLimit = evmutils.FromBigInt(param.Context.Block.GasLimit.Int)
	}

	evm := &EVM{
		stack:        stack.New(param.MaxStackDepth),
		memory:       memory.New(),
		storage:      storage.New(param.UpperStorage, param.ExternalStore), //param.ExternalStore's implement is ContractStore
		context:      param.Context,
		instructions: nil,
		resultNotify: param.ResultCallback, //is RuntimeInstance.callback
	}

	//For versions less than 2300, cross-contract calls(delegatecall/callcode) go inside EVM
	evm.instructions = instructions.New(evm, evm.stack, evm.memory, evm.storage, evm.context, nil, closure)

	return evm
}

// only be used in the end of becalled contract(in cross call)
func (e *EVM) subResult(result *ExecuteResult, err error) {
	if err == nil && result.ExitOpCode != opcodes.REVERT {
		//merge result storage of invoked contract to original contract storage
		//for example: contract A call contract B, merge B's result storage to A's storage
		if e.storage.GetCurrentBlockVersion() < 2211 {
			storage.MergeResultCache(&result.StorageCache, &e.storage.ResultCache)
		} else {
			storage.MergeResultCache2211(&result.StorageCache, &e.storage.ResultCache)
		}
	}
}

func (e *EVM) executePreCompiled(addr uint64, input []byte) (ExecuteResult, error) {
	contract := precompiledContracts.Contracts[addr]
	if e.storage.GetCurrentBlockVersion() >= params.V2030200 {
		contract = precompiledContracts.ContractsNew[addr]
	}

	switch addr {
	case 10:
		input = []byte(e.context.Parameters[protocol.ContractSenderOrgIdParam])
		//contract.SetValue(e.context.Parameters[protocol.ContractSenderOrgIdParam])
	case 11:
		input = []byte(e.context.Parameters[protocol.ContractSenderRoleParam])
		//contract.SetValue(e.context.Parameters[protocol.ContractSenderRoleParam])
	case 12:
		input = []byte(e.context.Parameters[protocol.ContractSenderPkParam])
		//contract.SetValue(e.context.Parameters[protocol.ContractSenderPkParam])
	case 13:
		input = []byte(e.context.Parameters[protocol.ContractCreatorOrgIdParam])
		//contract.SetValue(e.context.Parameters[protocol.ContractCreatorOrgIdParam])
	case 14:
		input = []byte(e.context.Parameters[protocol.ContractCreatorRoleParam])
		//contract.SetValue(e.context.Parameters[protocol.ContractCreatorRoleParam])
	case 15:
		input = []byte(e.context.Parameters[protocol.ContractCreatorPkParam])
		//contract.SetValue(e.context.Parameters[protocol.ContractCreatorPkParam])
		//default:
		//	if addr < 1 || addr > 15 {
		//		return ExecuteResult{}, errors.New("not existed precompiled contract")
		//	}
	}
	gasCost := contract.GasCost(input)
	gasLeft := e.instructions.GetGasLeft()

	result := ExecuteResult{
		ResultData:   nil,
		GasLeft:      gasLeft,
		StorageCache: e.storage.ResultCache,
	}

	if gasLeft < gasCost {
		return result, utils.ErrOutOfGas
	}

	execRet, err := contract.Execute(input, e.storage.GetCurrentBlockVersion(), e.context)
	gasLeft -= gasCost
	e.instructions.SetGasLimit(gasLeft)
	result.ResultData = execRet
	return result, err
}

func (e *EVM) ExecuteContract(isCreate bool) (ExecuteResult, error) {
	contractAddr := e.context.Contract.Address
	//gasLeft := e.instructions.GetGasLeft()
	//return e.executePreCompiled(1, e.context.Message.Data)
	if contractAddr != nil {
		//if contractAddr.IsUint64() {
		//	addr := contractAddr.Uint64()
		//if addr < precompiledContracts.ContractsMaxAddress {
		if precompiledContracts.IsPrecompiledContract(contractAddr, e.storage.GetCurrentBlockVersion()) {
			return e.executePreCompiled(contractAddr.Uint64(), e.context.Message.Data)
		}
		//}
	}

	execRet, gasLeft, byteCodeHead, byteCodeBody, err := e.instructions.ExecuteContract(isCreate)
	result := ExecuteResult{
		ResultData:   execRet,
		GasLeft:      gasLeft,
		StorageCache: e.storage.ResultCache, //same pointer of instructionsContext.storage.ResultCache
		ExitOpCode:   e.instructions.ExitOpCode(),
		ByteCodeBody: byteCodeBody,
		ByteCodeHead: byteCodeHead,
	}

	if e.resultNotify != nil {
		//== call EVMParam.ResultCallback == call RuntimeInstance.callback or evm.subResult
		e.resultNotify(&result, err)
	}
	return result, err
}

func (e *EVM) GetPcCountAndTimeUsed() (uint64, int64) {
	return e.instructions.GetPcCountAndTimeUsed()
}

// Create an independent EVM instance
func (e *EVM) getClosureDefaultEVM(param instructions.ClosureParam) *EVM {
	newEVM := New(EVMParam{
		MaxStackDepth: 1024,
		//External read-write interface
		ExternalStore:  e.storage.ExternalStorage,
		UpperStorage:   e.storage,
		ResultCallback: e.subResult, //will be called as evm.resultNotify in closure evm.ExecuteContract()
		//Interface to get on-chain information
		Context: &environment.Context{
			Block:       e.context.Block,
			Transaction: e.context.Transaction,
			Message: environment.Message{
				//The contract parameters
				Data: param.CallData,
			},
			Parameters: e.context.Parameters,
			EvmLog:     e.context.EvmLog,
		},
	})

	newEVM.context.Contract = environment.Contract{
		Address: param.ContractAddress,
		Code:    param.ContractCode,
		Hash:    param.ContractHash,
		Version: param.ContractVersion,
	}

	return newEVM
}

func (e *EVM) commonCall(param instructions.ClosureParam) ([]byte, error) {
	newEVM := e.getClosureDefaultEVM(param)

	//set storage address and call value
	switch param.OpCode {
	case opcodes.CALLCODE:
		//Use only the caller's code and keep the execution context of the current contract
		newEVM.context.Contract.Address = e.context.Contract.Address
		newEVM.context.Message.Value = param.CallValue
		newEVM.context.Message.Caller = e.context.Contract.Address

	case opcodes.DELEGATECALL:
		//Equivalent to callcode but reserving the caller and callValue
		newEVM.context.Contract.Address = e.context.Contract.Address
		newEVM.context.Message.Value = e.context.Message.Value
		newEVM.context.Message.Caller = e.context.Message.Caller
	case opcodes.CALL:
		//Regular cross-contract invocation
		newEVM.context.Contract.Address = param.ContractAddress
		newEVM.context.Message.Value = e.context.Message.Value
		if e.storage.GetCurrentBlockVersion() <= 2212 {
			newEVM.context.Message.Caller = e.context.Message.Caller
		} else {
			newEVM.context.Message.Caller = e.context.Contract.Address
		}
	}
	if param.OpCode == opcodes.STATICCALL || e.instructions.IsReadOnly() {
		if e.storage.GetCurrentBlockVersion() > 2212 {
			newEVM.context.Contract.Address = param.ContractAddress
			newEVM.context.Message.Value = e.context.Message.Value
			newEVM.context.Message.Caller = e.context.Contract.Address
		}

		//Static call, the caller does not modify the caller's own state after execution
		newEVM.instructions.SetReadOnly()
	}

	ret, err := newEVM.ExecuteContract(false)
	//ret, err := newEVM.ExecuteContract(opcodes.CALL == param.OpCode)

	e.instructions.SetGasLimit(ret.GasLeft)
	return ret.ResultData, err
}

func (e *EVM) commonCreate(param instructions.ClosureParam) ([]byte, error) {
	//var addr *utils.Int
	//if opcodes.CREATE == param.OpCode {
	//	addr = e.storage.ExternalStorage.CreateAddress(e.context.Message.Caller, e.context.Transaction)
	//} else {
	//	addr = e.storage.ExternalStorage.CreateFixedAddress(e.context.Message.Caller, param.CreateSalt, e.context.Transaction)
	//}
	newEVM := e.getClosureDefaultEVM(param)

	//newEVM.context.Contract.Address = addr
	newEVM.context.Message.Value = param.CallValue
	//The caller contract is the new caller, and the user sending transactions is now Origin
	newEVM.context.Message.Caller = e.context.Contract.Address

	ret, err := newEVM.ExecuteContract(true)
	e.instructions.SetGasLimit(ret.GasLeft)
	return ret.ResultData, err
}

// Closure functions that are invoked across contracts
func closure(param instructions.ClosureParam) ([]byte, error) {
	evm, ok := param.VM.(*EVM)
	if !ok {
		return nil, utils.ErrInvalidEVMInstance
	}

	switch param.OpCode {
	case opcodes.CALL, opcodes.CALLCODE, opcodes.DELEGATECALL, opcodes.STATICCALL:
		return evm.commonCall(param)
	case opcodes.CREATE, opcodes.CREATE2:
		return evm.commonCreate(param)
	}

	return nil, nil
}
