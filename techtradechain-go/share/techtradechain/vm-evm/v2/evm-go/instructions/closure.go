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

package instructions

import (
	"encoding/hex"
	"errors"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/opcodes"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/precompiledContracts"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/storage"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/utils"
)

type ClosureExecute func(ClosureParam) ([]byte, error)

type ClosureParam struct {
	VM           interface{}
	OpCode       opcodes.OpCode
	GasRemaining *evmutils.Int

	ContractAddress *evmutils.Int
	ContractHash    *evmutils.Int
	ContractCode    []byte
	ContractVersion string

	CallData   []byte
	CallValue  *evmutils.Int
	CreateSalt *evmutils.Int
}

func loadClosure() {
	//Normal call
	instructionTable[opcodes.CALL] = opCodeInstruction{
		action:            callAction,
		requireStackDepth: 7,
		enabled:           true,
		returns:           true,
	}

	//Use only the caller's code and keep the execution context of the current contract
	instructionTable[opcodes.CALLCODE] = opCodeInstruction{
		action:            callCodeAction,
		requireStackDepth: 7,
		enabled:           true,
		returns:           true,
	}

	//Equivalent to callcode but reserving the caller and callValue
	instructionTable[opcodes.DELEGATECALL] = opCodeInstruction{
		action:            delegateCallAction,
		requireStackDepth: 6,
		enabled:           true,
		returns:           true,
	}

	//Static call, the caller does not modify the caller's own state after execution
	instructionTable[opcodes.STATICCALL] = opCodeInstruction{
		action:            staticCallAction,
		requireStackDepth: 6,
		enabled:           true,
		returns:           true,
	}

	//Addresses are automatically created before version 2300,
	//and contract names in address format are automatically created after version 2300
	instructionTable[opcodes.CREATE] = opCodeInstruction{
		action:            createAction,
		requireStackDepth: 3,
		enabled:           true,
		returns:           true,
		isWriter:          true,
	}

	//Before version 2300, you can add salt value in the parameter of generating address. After version 2300,
	//the parameter of salt value is changed to allow users to pass contract name
	instructionTable[opcodes.CREATE2] = opCodeInstruction{
		action:            create2Action,
		requireStackDepth: 2,
		enabled:           true,
		returns:           true,
		isWriter:          true,
	}
}

func proCrossCallRet2217(ctx *instructionsContext, offset, size *evmutils.Int, ret []byte, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}

	//gas check
	o, s, _, e := ctx.memoryGasCostAndMalloc(offset, size)
	if e != nil {
		return nil, e
	}

	e = ctx.memory.StoreNBytes(o, s, ret)
	if e != nil {
		ctx.stack.Push(evmutils.New(0))
	} else {
		ctx.stack.Push(evmutils.New(1))
	}
	return ret, e
}

func proCrossCallRet2218(ctx *instructionsContext, offset, size *evmutils.Int, ret []byte, err error) ([]byte, error) {
	if err != nil {
		ctx.stack.Push(evmutils.New(0))
	} else {
		ctx.stack.Push(evmutils.New(1))
	}

	if err == nil || err == utils.ErrExecutionReverted {
		o, s, _, e := ctx.memoryGasCostAndMalloc(offset, size)
		if e != nil {
			return nil, e
		}

		err = ctx.memory.StoreNBytes(o, s, ret)
		if err != nil && ctx.storage.GetCurrentBlockVersion() >= params.V2030500 {
			return nil, err
		}
	}

	return ret, nil
}

func proCrossCallRet2300(ctx *instructionsContext, offset, size *evmutils.Int, gasLeft uint64,
	res *common.ContractResult, stat common.TxStatusCode) ([]byte, error) {
	if stat != common.TxStatusCode_SUCCESS || res.Code != 0 {
		if ctx.storage.GetCurrentBlockVersion() < params.V2030100 {
			return nil, nil
		} else {
			return nil, errors.New(res.Message)
		}
	}

	ctx.storage.ResultCache.ContractEvent = append(ctx.storage.ResultCache.ContractEvent, res.ContractEvent...)
	gasLeft -= res.GasUsed
	ctx.gasRemaining.SetUint64(gasLeft)
	callRet := res.Result

	//gas check
	o, s, _, err := ctx.memoryGasCostAndMalloc(offset, size)
	if err != nil {
		return nil, err
	}

	//Write the result to memory
	err = ctx.memory.StoreNBytes(o, s, callRet)
	if err != nil {
		//Presses the execution successfully or not
		ctx.stack.Push(evmutils.New(0))
	} else {
		ctx.stack.Push(evmutils.New(1))
	}
	return callRet, err
}

func proCrossCallRet2030101(ctx *instructionsContext, offset, size *evmutils.Int, gasLeft uint64,
	res *common.ContractResult, stat common.TxStatusCode) ([]byte, error) {
	callRet := res.Result
	if stat != common.TxStatusCode_SUCCESS || res.Code != 0 {
		ctx.stack.Push(evmutils.New(0))
		callRet = []byte(res.Message)
	} else {
		ctx.stack.Push(evmutils.New(1))
	}

	ctx.storage.ResultCache.ContractEvent = append(ctx.storage.ResultCache.ContractEvent, res.ContractEvent...)
	gasLeft -= res.GasUsed
	ctx.gasRemaining.SetUint64(gasLeft)

	//gas check
	o, s, _, err := ctx.memoryGasCostAndMalloc(offset, size)
	if err != nil {
		return nil, err
	}

	//Write the result to memory
	err = ctx.memory.StoreNBytes(o, s, callRet)
	if err != nil && ctx.storage.GetCurrentBlockVersion() >= params.V2030500 {
		return nil, err
	}
	return callRet, nil
}

func crossCallOld(ctx *instructionsContext, opCode opcodes.OpCode, code, data []byte, val, addr *evmutils.Int) ([]byte,
	error) {
	contractCodeHash, errInner := ctx.storage.GetCodeHash(addr)
	if errInner != nil {
		return nil, errInner
	}

	cParam := ClosureParam{
		VM:              ctx.vm,
		OpCode:          opCode, //Tell the new virtual machine instance the call type
		GasRemaining:    ctx.gasRemaining,
		ContractAddress: addr,
		ContractCode:    code,
		ContractHash:    contractCodeHash,
		CallData:        data,
		CallValue:       val,
	}

	//crossCallOld是230版本之前的跨合约调用逻辑，但调用预编译合约也是走这个逻辑
	//如果调用的是非预编译合约，不需要对ContractVersion赋值，因为230版本之前的普通跨合约调用无此功能
	//如果是调用预编译合约，本质上是调用一个功能，而非真正跨合约调用，所以合约版本号仍然是主调合约的版本号
	if precompiledContracts.IsPrecompiledContract(addr, ctx.storage.GetCurrentBlockVersion()) {
		cParam.ContractVersion = ctx.environment.Contract.Version
	}

	return ctx.closureExec(cParam)
}

func crossCallNew(ctx *instructionsContext, code, data []byte, addr *evmutils.Int,
	gasLeft uint64) (*common.ContractResult, common.TxStatusCode) {

	var method string
	if len(data) >= 4 {
		method = hex.EncodeToString(data)[0:8]
	}

	//name := hex.EncodeToString(addr.Bytes())
	name := ctx.storage.IntAddr2HexStr(addr)
	sender := hex.EncodeToString(ctx.environment.Contract.Address.Bytes())
	gasUsed := ctx.environment.Block.GasLimit.Uint64() - gasLeft

	parameter := make(map[string][]byte)
	//Add cross-contract invocation parameters
	//calldata
	parameter[protocol.ContractEvmParamKey] = []byte(hex.EncodeToString(data))
	//The current contract is called the caller of the called contract
	parameter[syscontract.CrossParams_SENDER.String()] = []byte(sender)
	//Indicates whether the current invocation is a user or cross-contract invocation
	parameter[syscontract.CrossParams_CALL_TYPE.String()] = []byte(syscontract.CallType_CROSS.String())
	//The invocation contract does not require the vm type, only for deployment
	return ctx.storage.ExternalStorage.CallContract(name, 0, method, code, parameter,
		gasUsed, false)
}

func commonCall(ctx *instructionsContext, opCode opcodes.OpCode) ([]byte, error) {
	_ = ctx.stack.Pop()
	addr := ctx.stack.Pop()
	var v *evmutils.Int = nil

	if opCode != opcodes.DELEGATECALL && opCode != opcodes.STATICCALL {
		//Neither delegate invocation nor static invocation modifies the state of the invoked contract,
		//so value is not required
		v = ctx.stack.Pop()
	}

	//DOffset and dLen are used to read callData
	dOffset := ctx.stack.Pop()
	dLen := ctx.stack.Pop()
	//ROffset and rLen are used to read the return value
	rOffset := ctx.stack.Pop()
	rLen := ctx.stack.Pop()

	//gas check
	offset, size, gasLeft, err := ctx.memoryGasCostAndMalloc(dOffset, dLen)
	if err != nil {
		return nil, err
	}

	//read calldata
	data, err := ctx.memory.Copy(offset, size)
	//view data
	if err != nil {
		return nil, err
	}

	//read code
	contractCode, err := ctx.storage.GetCode(addr)
	if err != nil {
		return nil, err
	}

	var callRet []byte
	version := ctx.storage.GetCurrentBlockVersion()
	if opCode == opcodes.DELEGATECALL || opCode == opcodes.CALLCODE {
		callRet, err = crossCallOld(ctx, opCode, contractCode, data, v, addr)
		if version < params.V2218 || version == params.V2300 || version == params.V2030100 {
			return proCrossCallRet2217(ctx, rOffset, rLen, callRet, err)
		} else {
			return proCrossCallRet2218(ctx, rOffset, rLen, callRet, err)
		}
	}

	if version < params.V2300 {
		//If version is smaller than 2300, a separate VM instance is created in the VM to execute the invoked contract
		callRet, err = crossCallOld(ctx, opCode, contractCode, data, v, addr)
		if version < params.V2218 {
			return proCrossCallRet2217(ctx, rOffset, rLen, callRet, err)
		} else {
			return proCrossCallRet2218(ctx, rOffset, rLen, callRet, err)
		}
	} else if version == params.V2300 {
		//Cross-contract calls greater than 2300 board will go Techtradechain-go
		result, status := crossCallNew(ctx, contractCode, data, addr, gasLeft)
		return proCrossCallRet2300(ctx, rOffset, rLen, gasLeft, result, status)
	} else if version == params.V2030100 {
		if precompiledContracts.IsPrecompiledContract(addr, version) {
			callRet, err = crossCallOld(ctx, opCode, contractCode, data, v, addr)
			return proCrossCallRet2217(ctx, rOffset, rLen, callRet, err)
		} else {
			result, status := crossCallNew(ctx, contractCode, data, addr, gasLeft)
			return proCrossCallRet2300(ctx, rOffset, rLen, gasLeft, result, status)
		}
	} else { // version > params.V2030100
		if precompiledContracts.IsPrecompiledContract(addr, version) {
			callRet, err = crossCallOld(ctx, opCode, contractCode, data, v, addr)
			return proCrossCallRet2218(ctx, rOffset, rLen, callRet, err)
		} else {
			result, status := crossCallNew(ctx, contractCode, data, addr, gasLeft)
			return proCrossCallRet2030101(ctx, rOffset, rLen, gasLeft, result, status)
		}
	}
}

func callAction(ctx *instructionsContext) ([]byte, error) {
	return commonCall(ctx, opcodes.CALL)
}

func callCodeAction(ctx *instructionsContext) ([]byte, error) {
	return commonCall(ctx, opcodes.CALLCODE)
}

func delegateCallAction(ctx *instructionsContext) ([]byte, error) {
	return commonCall(ctx, opcodes.DELEGATECALL)
}

func staticCallAction(ctx *instructionsContext) ([]byte, error) {
	return commonCall(ctx, opcodes.STATICCALL)
}

func createOld(ctx *instructionsContext, opCode opcodes.OpCode, addr, value, salt *evmutils.Int,
	code []byte) ([]byte, error) {
	ctx.storage.SetCode(addr, code)
	hash := evmutils.Keccak256(code)
	i := evmutils.New(0)
	i.SetBytes(hash)
	ctx.storage.SetCodeHash(addr, i)
	ctx.storage.SetCodeSize(addr, evmutils.New(int64(len(code))))

	//Versions less than 2300 are still created using EVM
	cParam := ClosureParam{
		VM:              ctx.vm,
		OpCode:          opCode,
		GasRemaining:    ctx.gasRemaining,
		ContractAddress: addr,
		ContractCode:    code,
		CallData:        []byte{},
		CallValue:       value,
		CreateSalt:      salt,
	}

	ret, err := ctx.closureExec(cParam)
	if err != nil {
		ctx.stack.Push(evmutils.New(0))
	} else {
		//addr:=ctx.storage.CreateFixedAddress(ctx.environment.Message.Caller,salt,ctx.environment.Transaction)
		ctx.stack.Push(addr)
	}

	//The RET of all instructions except RETURN is ignored
	return ret, err
}

func proCreateRet2030100(ctx *instructionsContext, gasLeft uint64, res *common.ContractResult,
	stat common.TxStatusCode) (ret []byte, err error) {
	if stat != common.TxStatusCode_SUCCESS || res.Code != 0 {
		if ctx.storage.GetCurrentBlockVersion() < params.V2030100 {
			err = errors.New(res.Message)
			//Ret other than the RETURN command is ignored
			ret = res.Result
		} else {
			return ret, errors.New(res.Message)
		}
	}

	ctx.storage.ResultCache.ContractEvent = append(ctx.storage.ResultCache.ContractEvent, res.ContractEvent...)
	var contract common.Contract
	if err := contract.Unmarshal(res.Result); err != nil {
		return nil, err
	}
	addr := evmutils.FromHexString(contract.Address)

	gasLeft -= res.GasUsed
	ctx.gasRemaining.SetUint64(gasLeft)

	if err != nil {
		ctx.stack.Push(evmutils.New(0))
	} else {
		//addr:=ctx.storage.CreateFixedAddress(ctx.environment.Message.Caller,salt,ctx.environment.Transaction)
		ctx.stack.Push(addr)
	}

	//The RET of all instructions except RETURN is ignored
	return ret, err
}

func proCreateRet2030101(ctx *instructionsContext, gasLeft uint64, res *common.ContractResult,
	stat common.TxStatusCode) (ret []byte, err error) {
	if stat != common.TxStatusCode_SUCCESS || res.Code != 0 {
		ctx.stack.Push(evmutils.New(0))
		err = utils.ErrExecutionReverted
		if m, e := hex.DecodeString(res.Message); e == nil {
			ctx.lastReturn = m
		}

		////08c379a0 is revert signature, if err message include 08c379a0, so parse the revert info
		//index := strings.Index(res.Message, "08c379a0")
		//if index != -1 {
		//	m, e := hex.DecodeString(res.Message[index:])
		//	//revert info format: signature + offset + msgLen + msg, len(signature)==4, len(offset)==32, len(msgLen)==32
		//	if e == nil && len(m) > (68) {
		//		ctx.lastReturn = m
		//		err = utils.ErrExecutionReverted
		//	}
		//}
	} else {
		ctx.storage.ResultCache.ContractEvent = append(ctx.storage.ResultCache.ContractEvent, res.ContractEvent...)
		var contract common.Contract
		if err = contract.Unmarshal(res.Result); err != nil {
			ctx.stack.Push(evmutils.New(0))
		} else {
			addr := evmutils.FromHexString(contract.Address)
			ctx.stack.Push(addr)
		}
	}

	if err == utils.ErrExecutionReverted {
		//ctx.lastReturn = res.Result
		return ctx.lastReturn, nil
	}

	gasLeft -= res.GasUsed
	ctx.gasRemaining.SetUint64(gasLeft)

	//The RET of all instructions except RETURN is ignored
	return nil, nil
}

func commonCreate(ctx *instructionsContext, opCode opcodes.OpCode) ([]byte, error) {
	value := ctx.stack.Pop()
	mOffset := ctx.stack.Pop()
	mSize := ctx.stack.Pop()
	//rand.Seed(time.Now().UnixNano())

	var name string
	var addr *evmutils.Int
	var rtType int32 = int32(common.RuntimeType_EVM)
	var salt *evmutils.Int = evmutils.New(0)
	if ctx.storage.GetCurrentBlockVersion() < params.V2300 {
		if opCode == opcodes.CREATE2 {
			//The create2 directive has an extra user-assigned salt value
			salt = salt.Add(ctx.stack.Pop())
		}

		//The bytecode that the contract executes to this point is used as one of the parameters to calculate
		//the address
		solt0 := evmutils.New(0).SetBytes(ctx.environment.Contract.Code[0:ctx.pc])
		salt.Add(evmutils.FromBigInt(solt0))
		//The address is obtained by hashing the caller, salt, and transaction
		addr = ctx.storage.CreateFixedAddress(ctx.environment.Message.Caller, salt, ctx.environment.Transaction,
			ctx.environment.Cfg.AddrType)
	} else {
		if opCode == opcodes.CREATE2 {
			//In versions greater than 2300, salt is modified to the name of the created contract that can be
			//assigned by the user
			customize := ctx.stack.Pop().Bytes()
			if ctx.storage.GetCurrentBlockVersion() < params.V2030300 {
				rtType = int32(customize[0])
				customize = customize[1:]
			} else {
				if len(customize) > 1 {
					rtType = int32(customize[0])
					customize = customize[1:]
				} else {
					ctx.lastReturn = []byte("contract type or name is empty in cross creation")
					return ctx.lastReturn, utils.ErrExecutionReverted
				}
			}

			salt.SetBytes(customize)
			//addr = ctx.storage.CreateAddress(salt, ctx.environment.Cfg.AddrType)
			bName := storage.TruncateNullTail(customize)
			name = string(bName)
		} else {
			//The CREATE command has no salt parameter
			salt.SetBytes(ctx.environment.Contract.Code[0:ctx.pc])
			fixAddress := ctx.storage.CreateFixedAddress(ctx.environment.Message.Caller, salt, ctx.environment.Transaction,
				ctx.environment.Cfg.AddrType)
			//The CREATE command has no salt parameter, so only one address is automatically calculated as the name,
			//and subsequent operations will use that address as the name to calculate the address
			name = hex.EncodeToString(fixAddress.Bytes())
		}
	}

	//gas check
	offset, size, gasLeft, err := ctx.memoryGasCostAndMalloc(mOffset, mSize)
	if err != nil {
		return nil, err
	}

	//read code
	code, err := ctx.memory.Copy(offset, size)
	if err != nil {
		return nil, err
	}

	if ctx.storage.GetCurrentBlockVersion() < params.V2300 {
		//For use prior to 2300, since cross-contract calls are done within VM-EVM
		return createOld(ctx, opCode, addr, value, salt, code)
	} else {
		//Version creation logic greater than 2300 goes through Techtradechain-go
		gasUsed := ctx.environment.Block.GasLimit.Uint64() - gasLeft

		parameter := make(map[string][]byte, 2)
		//Sets cross-contract invocation parameters
		sender := hex.EncodeToString(ctx.environment.Contract.Address.Bytes())
		//The sender has become the current contract
		parameter[syscontract.CrossParams_SENDER.String()] = []byte(sender)
		//Indicates that the call type is a cross-contract call
		parameter[syscontract.CrossParams_CALL_TYPE.String()] = []byte(syscontract.CallType_CROSS.String())
		result, status := ctx.storage.ExternalStorage.CallContract(name, rtType,
			protocol.ContractInitMethod, code, parameter, gasUsed, true)
		if ctx.storage.GetCurrentBlockVersion() <= params.V2030100 {
			return proCreateRet2030100(ctx, gasLeft, result, status)
		} else {
			return proCreateRet2030101(ctx, gasLeft, result, status)
		}
	}
}

func createAction(ctx *instructionsContext) ([]byte, error) {
	return commonCreate(ctx, opcodes.CREATE)
}

func create2Action(ctx *instructionsContext) ([]byte, error) {
	return commonCreate(ctx, opcodes.CREATE2)
}
