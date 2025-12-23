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
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
	"math/big"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"
	bn256 "github.com/umbracle/go-eth-bn256"
)

// bn256ScalarMulIstanbul implements a native elliptic curve scalar
// multiplication conforming to Istanbul consensus rules.
type bn256ScalarMulIstanbul struct{}

//func (c *bn256ScalarMulIstanbul)SetValue(v string){}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256ScalarMulIstanbul) GasCost(input []byte) uint64 {
	return params.Bn256ScalarMulGasIstanbul
}

func (c *bn256ScalarMulIstanbul) Execute(input []byte, version uint32, ctx *environment.Context) ([]byte, error) {
	if version < params.V2030300 {
		return nil, nil
	}

	return runBn256ScalarMul(input)
}

// runBn256ScalarMul implements the Bn256ScalarMul precompile, referenced by
// both Byzantium and Istanbul operations.
func runBn256ScalarMul(input []byte) ([]byte, error) {
	//p, err := newCurvePoint(evmutils.GetDataFrom(input, 0, 64))
	//if err != nil {
	//	return nil, err
	//}
	//res := new(bn256.G1)
	//res.ScalarMult(p, new(big.Int).SetBytes(evmutils.GetDataFrom(input, 64, 32)))
	//return res.Marshal(), nil

	b0 := new(bn256.G1)
	v0 := evmutils.GetDataFrom(input, 0, 64)
	if _, err := b0.Unmarshal(v0); err != nil {
		return nil, err
	}

	v1 := evmutils.GetDataFrom(input, 64, 32)
	k := new(big.Int).SetBytes(v1)

	c := new(bn256.G1)
	c.ScalarMult(b0, k)

	return c.Marshal(), nil
}
