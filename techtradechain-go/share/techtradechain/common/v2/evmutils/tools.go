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

package evmutils

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/asn1"
	"encoding/hex"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/crypto"

	"github.com/tjfoc/gmsm/sm2"
	"golang.org/x/crypto/sha3"
)

const (
	hashLength    = 32
	AddressLength = 20
)

// Address is the 20 byte address of an Ethereum account.
type Address [AddressLength]byte

// SetBytes sets the address to the value of b.
// @param b
func (a *Address) SetBytes(b []byte) {
	if len(b) > len(a) {
		b = b[len(b)-AddressLength:]
	}
	copy(a[AddressLength-len(b):], b)
}

// String returns the hex string representation of the address.
// @return string
func (a *Address) String() string {
	return hex.EncodeToString(a[:])
}

var (
	BlankHash = make([]byte, hashLength)
	ZeroHash  = Keccak256(nil)
)

// EVMIntToHashBytes returns the absolute value of x as a big-endian fixed length byte slice.
func EVMIntToHashBytes(i *Int) [hashLength]byte {
	iBytes := i.Bytes()
	iLen := len(iBytes)

	var hash [hashLength]byte
	if iLen > hashLength {
		copy(hash[:], iBytes[iLen-hashLength:])
	} else {
		copy(hash[hashLength-iLen:], iBytes)
	}

	return hash
}

// EthHashBytesToEVMInt EVMIntToHashBytes reverse
func EthHashBytesToEVMInt(hash [hashLength]byte) (*Int, error) {
	return HashBytesToEVMInt(hash[:])
}

// HashBytesToEVMInt byte to bigInt
func HashBytesToEVMInt(hash []byte) (*Int, error) {
	i := New(0)
	i.SetBytes(hash[:])
	return i, nil
}

// BytesDataToEVMIntHash fixed length bytes
func BytesDataToEVMIntHash(data []byte) *Int {
	var hashBytes []byte
	srcLen := len(data)
	if srcLen < hashLength {
		hashBytes = LeftPaddingSlice(data, hashLength)
	} else {
		hashBytes = data[:hashLength]
	}

	i := New(0)
	i.SetBytes(hashBytes)

	return i
}

// GetDataFrom get data from src
// @param src
// @param offset
// @param size
// @return []byte
func GetDataFrom(src []byte, offset uint64, size uint64) []byte {
	ret := make([]byte, size)
	dLen := uint64(len(src))
	if dLen < offset {
		return ret
	}

	end := offset + size
	if dLen < end {
		end = dLen
	}

	copy(ret, src[offset:end])
	return ret
}

// LeftPaddingSlice left padding
// @param src
// @param toSize
// @return []byte
func LeftPaddingSlice(src []byte, toSize int) []byte {
	sLen := len(src)
	if toSize <= sLen {
		return src
	}

	ret := make([]byte, toSize)
	copy(ret[toSize-sLen:], src)

	return ret
}

// RightPaddingSlice right padding
// @param src
// @param toSize
// @return []byte
func RightPaddingSlice(src []byte, toSize int) []byte {
	sLen := len(src)
	if toSize <= sLen {
		return src
	}

	ret := make([]byte, toSize)
	copy(ret, src)

	return ret
}

// Keccak256 keccak256 hash
// @param data
// @return []byte
func Keccak256(data []byte) []byte {
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(data)
	return hasher.Sum(nil)
}

// LeftPadBytes zero-pads slice to the left up to length l.
func LeftPadBytes(slice []byte, l int) []byte {
	if l <= len(slice) {
		return slice
	}

	padded := make([]byte, l)
	copy(padded[l-len(slice):], slice)

	return padded
}

// MakeAddressFromHex any hex str make an evm.Int
func MakeAddressFromHex(str string) (*Int, error) {
	data, err := FromHex(str)
	if err != nil {
		return nil, err
	}
	return MakeAddress(data), nil
}

// MakeAddressFromString any str make an evm.Int
func MakeAddressFromString(str string) (*Int, error) {
	return MakeAddress([]byte(str)), nil
}

// MakeAddress any byte make an evm.Int
func MakeAddress(data []byte) *Int {
	address := Keccak256(data)
	addr := hex.EncodeToString(address)[24:]
	return FromHexString(addr)
}

// BytesToAddress any byte set to an evm address
func BytesToAddress(b []byte) Address {
	var a Address
	a.SetBytes(b)
	return a
}

// StringToAddress any string make an evm address
func StringToAddress(s string) (Address, error) {
	addrInt, err := MakeAddressFromString(s)
	if err != nil {
		var a Address
		return a, err
	}
	return BigToAddress(addrInt), nil
}

// HexToAddress direct convert hex to an evm address,直接将十六进制字符串转换为Address类型
func HexToAddress(s string) (Address, error) {
	addrInt, err := FromHex(s)
	if err != nil {
		var a Address
		return a, err
	}
	return BytesToAddress(addrInt), nil
}

// MakeHexToAddress 基于十六进制数据，经过Hash计算获得一个EVM地址
// @param s
// @return Address
// @return error
func MakeHexToAddress(s string) (Address, error) {
	addrInt, err := MakeAddressFromHex(s)
	if err != nil {
		var a Address
		return a, err
	}
	return BigToAddress(addrInt), nil
}

// BigToAddress math.bigInt to evm address
func BigToAddress(b *Int) Address {
	return BytesToAddress(b.Bytes())
}

// FromHex convert hex string to bytes
// @param s
// @return []byte
// @return error
func FromHex(s string) ([]byte, error) {
	if Has0xPrefix(s) {
		s = s[2:]
	}
	if len(s)%2 == 1 {
		s = "0" + s
	}
	return hex.DecodeString(s)
}

// Has0xPrefix check if the string has 0x prefix
// @param str
// @return bool
func Has0xPrefix(str string) bool {
	return len(str) >= 2 && str[0] == '0' && (str[1] == 'x' || str[1] == 'X')
}

// MarshalPublicKey serialize pk to bytes
func MarshalPublicKey(pk crypto.PublicKey) (pkBytes []byte, err error) {
	pub := pk.ToStandardKey()
	switch k := pub.(type) {
	case *sm2.PublicKey:
		pkBytes = elliptic.Marshal(k.Curve, k.X, k.Y)
	case *ecdsa.PublicKey:
		pkBytes = elliptic.Marshal(k.Curve, k.X, k.Y)
	case *rsa.PublicKey:
		pkBytes, err = asn1.Marshal(rsaPublicKeyASN{
			N: k.N,
			E: k.E,
		})
		if err != nil {
			return nil, fmt.Errorf("fail to marshal RSA public key: %v", err)
		}
	default:
		return nil, fmt.Errorf("unsupported public key type [%T]", k)
	}

	return pkBytes, nil
}
