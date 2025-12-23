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
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/utils"
	"math/big"
	"math/bits"
)

type IntArr [4]uint64

// bitLen returns the number of bits required to represent i
func (i *IntArr) bitLen() int {
	switch {
	case i[3] != 0:
		return 192 + bits.Len64(i[3])
	case i[2] != 0:
		return 128 + bits.Len64(i[2])
	case i[1] != 0:
		return 64 + bits.Len64(i[1])
	default:
		return bits.Len64(i[0])
	}
}

// joint computes (high * 2^64 + low) = z + (x * y)
func joint(z, x, y uint64) (high, low uint64) {
	high, low = bits.Mul64(x, y)
	low, relay := bits.Add64(low, z, 0)
	high, _ = bits.Add64(high, 0, relay)
	return high, low
}

// joint2 computes (high * 2^64 + low) = z + (x * y) + relay.
func joint2(z, x, y, relay uint64) (high, low uint64) {
	high, low = bits.Mul64(x, y)
	low, relay = bits.Add64(low, relay, 0)
	high, _ = bits.Add64(high, 0, relay)
	low, relay = bits.Add64(low, z, 0)
	high, _ = bits.Add64(high, 0, relay)
	return high, low
}

// mul computu x*y
func mul(x, y *IntArr) IntArr {
	var res IntArr
	var relay uint64
	var res1, res2, res3 uint64

	relay, res[0] = bits.Mul64(x[0], y[0])
	relay, res1 = joint(relay, x[1], y[0])
	relay, res2 = joint(relay, x[2], y[0])
	res3 = x[3]*y[0] + relay

	relay, res[1] = joint(res1, x[0], y[1])
	relay, res2 = joint2(res2, x[1], y[1], relay)
	res3 = res3 + x[2]*y[1] + relay

	relay, res[2] = joint(res2, x[0], y[2])
	res3 = res3 + x[1]*y[2] + relay

	res[3] = res3 + x[0]*y[3]
	return res
}

//compute num * num
func square(num *IntArr) IntArr {
	var pow IntArr
	var relay0, relay1, relay2 uint64
	var res1, res2 uint64

	relay0, pow[0] = bits.Mul64(num[0], num[0])
	relay0, res1 = joint(relay0, num[0], num[1])
	relay0, res2 = joint(relay0, num[0], num[2])

	relay1, pow[1] = joint(res1, num[0], num[1])
	relay1, res2 = joint2(res2, num[1], num[1], relay1)

	relay2, pow[2] = joint(res2, num[0], num[2])

	pow[3] = 2*(num[0]*num[3]+num[1]*num[2]) + relay0 + relay1 + relay2

	*num = pow
	return pow
}

func bigIntExp(base, expo *IntArr) IntArr {
	pow := IntArr{1, 0, 0, 0}
	mult := *base
	bitLen := expo.bitLen()

	elem := expo[0]
	for i := 0; i < bitLen && i < 256; i++ {
		if i != 0 && i%64 == 0 {
			elem = expo[i/64]
		}

		if elem&1 == 1 {
			pow = mul(&pow, &mult)
		}

		square(&mult)
		elem >>= 1
	}

	return pow
}
func loadArithmetic() {
	instructionTable[opcodes.STOP] = opCodeInstruction{
		action:   stopAction,
		enabled:  true,
		finished: true,
	}

	instructionTable[opcodes.ADD] = opCodeInstruction{
		action:            addAction,
		requireStackDepth: 2,
		enabled:           true,
	}

	instructionTable[opcodes.MUL] = opCodeInstruction{
		action:            mulAction,
		requireStackDepth: 2,
		enabled:           true,
	}

	instructionTable[opcodes.SUB] = opCodeInstruction{
		action:            subAction,
		requireStackDepth: 2,
		enabled:           true,
	}

	instructionTable[opcodes.DIV] = opCodeInstruction{
		action:            divAction,
		requireStackDepth: 2,
		enabled:           true,
	}

	instructionTable[opcodes.SDIV] = opCodeInstruction{
		action:            sDivAction,
		requireStackDepth: 2,
		enabled:           true,
	}

	instructionTable[opcodes.MOD] = opCodeInstruction{
		action:            modAction,
		requireStackDepth: 2,
		enabled:           true,
	}

	instructionTable[opcodes.SMOD] = opCodeInstruction{
		action:            sModAction,
		requireStackDepth: 2,
		enabled:           true,
	}

	instructionTable[opcodes.ADDMOD] = opCodeInstruction{
		action:            addModAction,
		requireStackDepth: 3,
		enabled:           true,
	}

	instructionTable[opcodes.MULMOD] = opCodeInstruction{
		action:            mulModAction,
		requireStackDepth: 3,
		enabled:           true,
	}

	instructionTable[opcodes.EXP] = opCodeInstruction{
		action:            expAction,
		requireStackDepth: 2,
		enabled:           true,
	}

	instructionTable[opcodes.SIGNEXTEND] = opCodeInstruction{
		action:            signExtendAction,
		requireStackDepth: 2,
		enabled:           true,
	}
}

func stopAction(_ *instructionsContext) ([]byte, error) {
	return nil, nil
}

func addAction(ctx *instructionsContext) ([]byte, error) {
	x := ctx.stack.Pop()
	y := ctx.stack.Peek()

	y.Add(x)
	return nil, nil
}

func mulAction(ctx *instructionsContext) ([]byte, error) {
	x := ctx.stack.Pop()
	y := ctx.stack.Peek()

	y.Mul(x)
	return nil, nil
}

func subAction(ctx *instructionsContext) ([]byte, error) {
	x := ctx.stack.Pop()
	y := ctx.stack.Peek()

	y.Set(x.Sub(y).Int)
	return nil, nil
}

func divAction(ctx *instructionsContext) ([]byte, error) {
	x := ctx.stack.Pop()
	y := ctx.stack.Peek()

	y.Set(x.Div(y).Int)
	return nil, nil
}

func sDivAction(ctx *instructionsContext) ([]byte, error) {
	x := ctx.stack.Pop()
	y := ctx.stack.Peek()

	y.Set(x.SDiv(y).Int)
	return nil, nil
}

func modAction(ctx *instructionsContext) ([]byte, error) {
	x := ctx.stack.Pop()
	y := ctx.stack.Peek()

	y.Set(x.Mod(y).Int)
	return nil, nil
}

func sModAction(ctx *instructionsContext) ([]byte, error) {
	x := ctx.stack.Pop()
	y := ctx.stack.Peek()

	y.Set(x.SMod(y).Int)
	return nil, nil
}

func addModAction(ctx *instructionsContext) ([]byte, error) {
	x := ctx.stack.Pop()
	y := ctx.stack.Pop()
	m := ctx.stack.Peek()

	m.Set(x.AddMod(y, m).Int)
	return nil, nil
}

func mulModAction(ctx *instructionsContext) ([]byte, error) {
	x := ctx.stack.Pop()
	y := ctx.stack.Pop()
	m := ctx.stack.Peek()

	m.Set(x.MulMod(y, m).Int)
	return nil, nil
}

func bigInt2IntArr(b *evmutils.Int) IntArr {
	abs := b.Bits()
	res := IntArr{0, 0, 0, 0}
	length := len(abs)

	for i := 0; i < length; i++ {
		res[i] = uint64(abs[i])
	}

	return res
}

func IntArr2BigInt(a *IntArr, res *evmutils.Int) {
	abs := []big.Word{0, 0, 0, 0}
	for i := 0; i < 4; i++ {
		abs[i] = big.Word(a[i])

	}

	res.SetBits(abs)
}

func expAction(ctx *instructionsContext) ([]byte, error) {
	x := ctx.stack.Pop()
	e := ctx.stack.Peek()

	gasLeft := ctx.gasRemaining.Uint64()
	if e.Sign() > 0 {
		gasCost := uint64((e.BitLen()+7)/8) * ctx.gasSetting.DynamicCost.EXPBytesCost
		if gasLeft < gasCost {
			return nil, utils.ErrOutOfGas
		} else {
			gasLeft -= gasCost
		}

		ctx.gasRemaining.SetUint64(gasLeft)
	}
	//e.Set(x.Exp(e).Int)

	base := bigInt2IntArr(x)
	expo := bigInt2IntArr(e)
	res := bigIntExp(&base, &expo)
	IntArr2BigInt(&res, e)

	return nil, nil
}

func signExtendAction(ctx *instructionsContext) ([]byte, error) {
	b := ctx.stack.Pop()
	x := ctx.stack.Peek()

	x.Set(x.SignExtend(b).Int)

	return nil, nil
}
