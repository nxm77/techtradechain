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
	"bytes"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"math/big"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"
	"github.com/btcsuite/btcd/btcec"
)

var (
	// S256 is the secp256k1 elliptic curve
	S256          = btcec.S256()
	secp256k1N, _ = evmutils.FromHex("0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141")
	one           = []byte{0x01}
)

// Currently, only secp256k1 curve of Ethereum is supported.
// P256 curve of techtradechain is not supported, which needs to be added later
type ecRecover struct{}

//func (c *ecRecover)SetValue(v string){}

func (e ecRecover) GasCost(input []byte) uint64 {
	return params.EcrecoverGas
}

func allZero(src []byte) bool {
	for _, b := range src {
		if b != 0 {
			return false
		}
	}
	return true
}

func (e ecRecover) Execute(input []byte, version uint32, ctx *environment.Context) ([]byte, error) {
	//const ecRecoverInputLength = 128
	//
	//input = evmutils.RightPaddingSlice(input, ecRecoverInputLength)
	//// "input" is (hash, v, r, s), each 32 bytes
	//// but for ecrecover we want (r, s, v)
	//
	//r := new(big.Int).SetBytes(input[64:96])
	//s := new(big.Int).SetBytes(input[96:128])
	//v := input[63] - 27
	//
	//// tighter sig s values input homestead only apply to tx sigs
	//if !allZero(input[32:63]) || !crypto.ValidateSignatureValues(v, r, s, false) {
	//	return nil, nil
	//}
	//// We must make sure not to modify the 'input', so placing the 'v' along with
	//// the signature needs to be done on a new allocation
	//sig := make([]byte, 65)
	//copy(sig, input[64:128])
	//sig[64] = v
	//// v needs to be at the end for libsecp256k1
	//pubKey, err := crypto.Ecrecover(input[:32], sig)
	//// make sure the public key is a valid one
	//if err != nil {
	//	return nil, nil
	//}
	//
	//// the first byte of pubkey is bitcoin heritage
	//return evmutils.LeftPaddingSlice(crypto.Keccak256(pubKey[1:])[12:], 32), nil
	//return nil, nil

	if version < params.V2030300 {
		return nil, nil
	}

	input = evmutils.GetDataFrom(input, 0, 128)

	// recover the value v. Expect all zeros except the last byte
	for i := 32; i < 63; i++ {
		if input[i] != 0 {
			return nil, nil
		}
	}

	v := input[63] - 27
	r := big.NewInt(0).SetBytes(input[64:96])
	s := big.NewInt(0).SetBytes(input[96:128])

	if !validateSignatureValues(v, r, s) {
		return nil, nil
	}

	pub, err := recoverPubkey(append(input[64:128], v), input[:32])
	if err != nil {
		return nil, err
	}

	pubKey := elliptic.Marshal(S256, pub.X, pub.Y)
	dst := evmutils.Keccak256(pubKey[1:])

	return evmutils.LeftPaddingSlice(dst[12:], 32), nil
}

// validateSignatureValues checks if the signature values are correct
func validateSignatureValues(v byte, r, s *big.Int) bool {
	if r == nil || s == nil {
		return false
	}

	if v > 1 {
		return false
	}

	rr := r.Bytes()
	rr = trimLeftZeros(rr)

	if bytes.Compare(rr, secp256k1N) >= 0 || bytes.Compare(rr, one) < 0 {
		return false
	}

	ss := s.Bytes()
	ss = trimLeftZeros(ss)

	if bytes.Compare(ss, secp256k1N) >= 0 || bytes.Compare(ss, one) < 0 {
		return false
	}

	return true
}
func trimLeftZeros(b []byte) []byte {
	i := 0
	for i = range b {
		if b[i] != 0 {
			break
		}
	}

	return b[i:]
}

// recoverPubkey verifies the compact signature "signature" of "hash" for the
// secp256k1 curve.
func recoverPubkey(signature, hash []byte) (*ecdsa.PublicKey, error) {
	size := len(signature)
	term := byte(27)

	// Make sure the signature is present
	if signature == nil || size < 1 {
		return nil, errors.New("invalid signature")
	}

	if signature[size-1] == 1 {
		term = 28
	}

	sig := append([]byte{term}, signature[:size-1]...)
	pub, _, err := btcec.RecoverCompact(S256, sig, hash)

	if err != nil {
		return nil, err
	}

	return pub.ToECDSA(), nil
}
