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
	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"
	bn256 "github.com/umbracle/go-eth-bn256"
)

// bn256Add implements a native elliptic curve point addition conforming to
// Istanbul consensus rules.
type bn256AddIstanbul struct{}

//func (c *bn256AddIstanbul)SetValue(v string){}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256AddIstanbul) GasCost(input []byte) uint64 {
	return params.Bn256AddGasIstanbul
}

func (c *bn256AddIstanbul) Execute(input []byte, version uint32, ctx *environment.Context) ([]byte, error) {
	if version < params.V2030300 {
		return nil, nil
	}

	return runBn256Add(input)
}

// runBn256Add implements the Bn256Add precompile, referenced by both
// Byzantium and Istanbul operations.
func runBn256Add(input []byte) ([]byte, error) {
	//x, err := newCurvePoint(evmutils.GetDataFrom(input, 0, 64))
	//if err != nil {
	//	return nil, err
	//}
	//y, err := newCurvePoint(evmutils.GetDataFrom(input, 64, 64))
	//if err != nil {
	//	return nil, err
	//}
	//res := new(bn256.G1)
	//res.Add(x, y)
	//return res.Marshal(), nil

	b1 := new(bn256.G1)
	b2 := new(bn256.G1)

	v1 := evmutils.GetDataFrom(input, 0, 64)
	if _, err := b1.Unmarshal(v1); err != nil {
		return nil, err
	}

	v2 := evmutils.GetDataFrom(input, 64, 64)
	if _, err := b2.Unmarshal(v2); err != nil {
		return nil, err
	}

	c := new(bn256.G1)
	c.Add(b1, b2)

	return c.Marshal(), nil
}
