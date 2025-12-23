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
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/utils"
)

func loadStack() {
	instructionTable[opcodes.POP] = opCodeInstruction{
		action: func(ctx *instructionsContext) (bytes []byte, err error) {
			_ = ctx.stack.Pop()
			return nil, nil
		},
		requireStackDepth: 1,
		enabled:           true,
	}

	setPushActions()
	setSwapActions()
	setDupActions()
}

func setPushActions() {
	instructionTable[opcodes.PUSH0] = opCodeInstruction{
		action: func(ctx *instructionsContext) ([]byte, error) {
			version := ctx.storage.ExternalStorage.GetCurrentBlockVersion()
			if version < params.V2218 || version == params.V2300 || version == params.V2030100 {
				//2300 and 2310 have been released, but 2217 has found bugs, so versions before 2218, as well as 2300 and 2310
				//that have been released, use the old logic, and other versions use the new logic
				return nil, utils.InvalidOpCode(byte(opcodes.PUSH0))
			}

			ctx.stack.Push(evmutils.New(0))
			return nil, nil
		},
		willIncreaseStack: 1,
		enabled:           true,
	}

	for i := opcodes.PUSH1; i <= opcodes.PUSH32; i++ {
		bytesSize := uint64(i - opcodes.PUSH1 + 1)

		instructionTable[i] = opCodeInstruction{
			action: func(ctx *instructionsContext) ([]byte, error) {
				start := ctx.pc + 1

				codeBytes := evmutils.GetDataFrom(ctx.environment.Contract.Code, start, bytesSize)

				i := evmutils.New(0)
				i.SetBytes(codeBytes)
				ctx.stack.Push(i)
				ctx.pc += bytesSize
				return nil, nil
			},

			willIncreaseStack: 1,
			enabled:           true,
		}
	}
}

func setSwapActions() {
	for i := opcodes.SWAP1; i <= opcodes.SWAP16; i++ {
		swapDepth := int(i - opcodes.SWAP1 + 1)

		instructionTable[i] = opCodeInstruction{
			action: func(ctx *instructionsContext) ([]byte, error) {
				ctx.stack.Swap(swapDepth)
				return nil, nil
			},

			requireStackDepth: swapDepth + 1,
			enabled:           true,
		}
	}
}

func setDupActions() {
	for i := opcodes.DUP1; i <= opcodes.DUP16; i++ {
		dupDepth := int(i - opcodes.DUP1 + 1)

		instructionTable[i] = opCodeInstruction{
			action: func(ctx *instructionsContext) ([]byte, error) {
				ctx.stack.Dup(dupDepth)
				return nil, nil
			},

			requireStackDepth: dupDepth,
			willIncreaseStack: 1,
			enabled:           true,
		}
	}
}
