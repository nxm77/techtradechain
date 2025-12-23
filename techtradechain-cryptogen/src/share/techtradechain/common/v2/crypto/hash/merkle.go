/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hash

import (
	"bytes"
	"crypto/sha256"
	"math"

	"techtradechain.com/techtradechain/common/v2/crypto"
)

// nolint: deadcode,unused
var h = sha256.New()

// GetMerkleRoot used to calculate merkleroot of hashes by hashType
func GetMerkleRoot(hashType string, hashes [][]byte) ([]byte, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	merkleTree, err := BuildMerkleTree(hashType, hashes)
	if err != nil {
		return nil, err
	}
	return merkleTree[len(merkleTree)-1], nil
}

// BuildMerkleTree take leaf node hash array and build merkle tree
func BuildMerkleTree(hashType string, hashes [][]byte) ([][]byte, error) {
	var hasher = Hash{
		hashType: crypto.HashAlgoMap[hashType],
	}

	var err error
	if len(hashes) == 0 {
		return nil, nil
	}

	// use array to store merkle tree entries
	nextPowOfTwo := getNextPowerOfTwo(len(hashes))
	arraySize := nextPowOfTwo*2 - 1
	merkelTree := make([][]byte, arraySize)

	// 1. copy hashes first
	copy(merkelTree[:len(hashes)], hashes[:])

	// 2. compute merkle step by step
	offset := nextPowOfTwo
	for i := 0; i < arraySize-1; i += 2 {
		switch {
		case merkelTree[i] == nil:
			// parent node is nil if left is nil
			merkelTree[offset] = nil
		case merkelTree[i+1] == nil:
			// hash(left, left) if right is nil
			merkelTree[offset], err = hashMerkleBranches(hasher, merkelTree[i], merkelTree[i])
			if err != nil {
				return nil, err
			}
		default:
			// default hash(left||right)
			merkelTree[offset], err = hashMerkleBranches(hasher, merkelTree[i], merkelTree[i+1])
			if err != nil {
				return nil, err
			}
		}
		offset++
	}

	return merkelTree, nil
}

func GetMerklePath(index int32, merkleTree [][]byte) [][]byte {
	paths := make([][]byte, 0)

	dep := int(math.Log2(float64(len(merkleTree) + 1)))
	merkleTree = merkleTree[:len(merkleTree)-1]
	if index == -1 {
		return nil
	}
	for i := 1; i < dep; i++ {
		levelCount := int(math.Pow(2, float64(i)))
		levelList := merkleTree[len(merkleTree)-levelCount:]
		merkleTree = merkleTree[:len(merkleTree)-levelCount]
		mask := index >> (dep - i - 1)
		if mask%2 == 0 {
			mask++
		} else {
			mask--
		}
		if levelList[mask] == nil {
			mask--
		}
		paths = leftJoin(levelList[mask], paths)
	}
	return paths
}

func Prove(paths [][]byte, merkleRoot, txHash []byte, index uint32, hashType string) bool {
	var hasher = Hash{
		hashType: crypto.HashAlgoMap[hashType],
	}

	intermediateNodes := paths
	// Shortcut the empty-block case
	if bytes.Equal(txHash[:], merkleRoot[:]) && index == 0 && len(intermediateNodes) == 0 {
		return true
	}

	current := txHash
	idx := index
	proofLength := len(intermediateNodes)

	numSteps := (proofLength)

	for i := 0; i < numSteps; i++ {
		next := intermediateNodes[i]
		if idx%2 == 1 {
			current, _ = hashMerkleBranches(hasher, next, current)
		} else {
			current, _ = hashMerkleBranches(hasher, current, next)
		}
		idx >>= 1
	}

	return bytes.Equal(current, merkleRoot)
}

func leftJoin(n []byte, list [][]byte) [][]byte {
	result := make([][]byte, len(list)+1)
	result[0] = n
	for i, x := range list {
		result[i+1] = x
	}
	return result
}

func getNextPowerOfTwo(n int) int {
	if n&(n-1) == 0 {
		return n
	}

	exponent := uint(math.Log2(float64(n))) + 1
	return 1 << exponent
}

func hashMerkleBranches(hasher Hash, left []byte, right []byte) ([]byte, error) {
	data := make([]byte, len(left)+len(right))
	copy(data[:len(left)], left)
	copy(data[len(left):], right)
	return hasher.Get(data)
}

func getNextPowerOfTen(n int) (int, int) {
	//if n&(n-1) == 0 {
	//	return n, 0
	//}
	if n == 1 {
		return 1, 0
	}

	exponent := int(math.Log10(float64(n-1))) + 1
	rootsSize := 0
	for i := 0; i < exponent; i++ {
		rootsSize += int(math.Pow10(i))
	}
	return int(math.Pow10(exponent)), rootsSize
}
