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
	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/opcodes"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/storage"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/utils"
)

func loadStorage() {
	instructionTable[opcodes.SLOAD] = opCodeInstruction{
		action:            sLoadAction,
		requireStackDepth: 1,
		enabled:           true,
	}

	instructionTable[opcodes.SSTORE] = opCodeInstruction{
		action:            sStoreAction,
		requireStackDepth: 2,
		enabled:           true,
		isWriter:          true,
	}

	instructionTable[opcodes.TLOAD] = opCodeInstruction{
		action:            tLoadAction,
		requireStackDepth: 1,
		enabled:           true,
	}

	instructionTable[opcodes.TSTORE] = opCodeInstruction{
		action:            tStoreAction,
		requireStackDepth: 2,
		enabled:           true,
		isWriter:          true,
	}
}

//func sLoadAction(ctx *instructionsContext) ([]byte, error) {
//	k := ctx.stack.Peek()
//
//	v, err := ctx.storage.SLoad2217(ctx.environment.Contract.Address, k)
//	if err != nil {
//		return nil, err
//	}
//
//	k.Set(v.Int)
//	return nil, nil
//}

func sLoadAction(ctx *instructionsContext) ([]byte, error) {
	k := ctx.stack.Peek()
	version := ctx.storage.GetCurrentBlockVersion()
	var (
		v   *evmutils.Int
		err error
	)

	if version < params.V2218 || version == params.V2300 || version == params.V2030100 {
		//2300 and 2310 have been released, but 2217 has found bugs, so versions before 2218, as well as 2300 and 2310
		//that have been released, use the old logic, and other versions use the new logic
		v, err = ctx.storage.SLoad2217(ctx.environment.Contract.Address, k)
		if err != nil {
			return nil, err
		}

	} else {
		v, err = ctx.storage.SLoad(ctx.environment.Contract.Address, k)
		if err != nil {
			return nil, err
		}

	}

	k.Set(v.Int)
	return nil, nil
}

func sStoreAction(ctx *instructionsContext) ([]byte, error) {
	k := ctx.stack.Pop()
	v := ctx.stack.Pop()

	ctx.storage.SStore(ctx.environment.Contract.Address, k, v)
	return nil, nil
}

func tLoadAction(ctx *instructionsContext) ([]byte, error) {
	version := ctx.storage.ExternalStorage.GetCurrentBlockVersion()
	if version < params.V2218 || version == params.V2300 || version == params.V2030100 {
		//2300 and 2310 have been released, but 2217 has found bugs, so versions before 2218, as well as 2300 and 2310
		//that have been released, use the old logic, and other versions use the new logic
		return nil, utils.InvalidOpCode(byte(opcodes.TLOAD))
	}

	//loc := scope.Stack.peek()
	//hash := common.Hash(loc.Bytes32())
	//val := interpreter.evm.StateDB.GetTransientState(scope.Contract.Address(), hash)
	//loc.SetBytes(val.Bytes())
	//return nil, nil

	k := ctx.stack.Peek()
	key := storage.Hash{}
	copy(key[:], k.Bytes())
	val := ctx.storage.TransientCache.Get(ctx.environment.Contract.Address, key)
	k.SetBytes(val[:])
	return nil, nil
}

func tStoreAction(ctx *instructionsContext) ([]byte, error) {
	version := ctx.storage.ExternalStorage.GetCurrentBlockVersion()
	if version < params.V2218 || version == params.V2300 || version == params.V2030100 {
		//2300 and 2310 have been released, but 2217 has found bugs, so versions before 2218, as well as 2300 and 2310
		//that have been released, use the old logic, and other versions use the new logic
		return nil, utils.InvalidOpCode(byte(opcodes.TSTORE))
	}

	//if interpreter.readOnly {
	//	return nil, ErrWriteProtection
	//}
	//loc := scope.Stack.pop()
	//val := scope.Stack.pop()
	//interpreter.evm.StateDB.SetTransientState(scope.Contract.Address(), loc.Bytes32(), val.Bytes32())
	//return nil, nil

	if ctx.readOnly {
		return nil, utils.ErrWriteProtection
	}

	k := ctx.stack.Pop()
	key := storage.Hash{}
	copy(key[:], k.Bytes())

	v := ctx.stack.Pop()
	val := storage.Hash{}
	copy(val[:], v.Bytes())
	ctx.storage.TransientCache.Set(ctx.environment.Contract.Address, key, val)

	return nil, nil
}
