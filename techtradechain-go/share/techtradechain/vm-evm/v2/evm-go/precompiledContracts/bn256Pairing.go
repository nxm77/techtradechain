/*
 * Copyright (c) 2021. TechTradeChain.org
 */

package precompiledContracts

import (
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
	"errors"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"
	bn256 "github.com/umbracle/go-eth-bn256"
)

// bn256PairingIstanbul implements a pairing pre-compile for the bn256 curve
// conforming to Istanbul consensus rules.
type bn256PairingIstanbul struct{}

var (
	falseBytes = make([]byte, 32)
	trueBytes  = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
)

//func (c *bn256PairingIstanbul)SetValue(v string){}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256PairingIstanbul) GasCost(input []byte) uint64 {
	return params.Bn256PairingBaseGasIstanbul + uint64(len(input)/192)*params.Bn256PairingPerPointGasIstanbul
}

func (c *bn256PairingIstanbul) Execute(input []byte, version uint32, ctx *environment.Context) ([]byte, error) {
	if version < params.V2030300 {
		return nil, nil
	}

	return runBn256Pairing(input)
}

// runBn256Pairing implements the Bn256Pairing precompile, referenced by both
// Byzantium and Istanbul operations.
func runBn256Pairing(input []byte) ([]byte, error) {
	//// Handle some corner cases cheaply
	//// Convert the input into a set of coordinates
	//var (
	//	cs []*bn256.G1
	//	ts []*bn256.G2
	//)
	//for i := 0; i < len(input); i += 192 {
	//	c, err := newCurvePoint(input[i : i+64])
	//	if err != nil {
	//		return nil, err
	//	}
	//	t, err := newTwistPoint(input[i+64 : i+192])
	//	if err != nil {
	//		return nil, err
	//	}
	//	cs = append(cs, c)
	//	ts = append(ts, t)
	//}
	//// Execute the pairing checks and return the results
	//if bn256.PairingCheck(cs, ts) {
	//	return true32Byte, nil
	//}
	//return false32Byte, nil

	if len(input) == 0 {
		return trueBytes, nil
	}

	if len(input)%192 != 0 {
		return nil, errors.New("bad input size for runBn256Pairing")
	}

	num := len(input) / 192
	ar := make([]*bn256.G1, num)
	br := make([]*bn256.G2, num)

	for i := 0; i < num; i++ {
		ag := new(bn256.G1)
		bg := new(bn256.G2)

		av := evmutils.GetDataFrom(input, uint64(i*192), 64)
		if _, err := ag.Unmarshal(av); err != nil {
			return nil, err
		}

		bv := evmutils.GetDataFrom(input, uint64(i*192)+64, 128)
		if _, err := bg.Unmarshal(bv); err != nil {
			return nil, err
		}

		ar[i] = ag
		br[i] = bg
	}

	if bn256.PairingCheck(ar, br) {
		return trueBytes, nil
	}

	return falseBytes, nil
}
