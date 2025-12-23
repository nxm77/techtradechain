//+build linux,amd64

/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

/*
  Bulletproofs provide zero-knowledge proof for the statement integer x in the range [0, 2^64)
*/

package bulletproofs_cgo

//#cgo CFLAGS: -g -O2 -pthread -I./c_include
//#cgo LDFLAGS: -L./c_lib -lbulletproofs -lm -L/usr/lib/x86_64-linux-gnu -ldl
//#cgo LDFLAGS: -L/usr/bin/ld -lpthread
//#include <stdlib.h>
//#include <stdio.h>
//#include <string.h>
//#include <bulletproofs.h>
import "C"
import (
	"bytes"
	"fmt"
	"unsafe"
)

// ProveRandomOpening Generate proof with randomly pick opening
// x: prove x is in the range [0, 2^64)
// return 1: proof in []byte
// return 2: commitment of x: xB + rB'
// return 3: opening, the randomness r used to commit x (secret key)
func ProveRandomOpening(x uint64) ([]byte, []byte, []byte, error) {
	var proofData C.proof_content

	ret := C.bulletproofs_prove_with_random_opening(&proofData, C.uint(x))
	if ret != OK {
		return nil, nil, nil, fmt.Errorf("fail to generate proof: " + getErrMsg(int64(ret)))
	}

	proofContent := C.GoBytes(proofData.proof, C.int(SINGLE_PROOF_SIZE))
	commitment := C.GoBytes(proofData.commitment, C.int(POINT_SIZE))
	opening := C.GoBytes(proofData.opening, C.int(POINT_SIZE))

	defer C.free(proofData.proof)
	defer C.free(proofData.commitment)
	defer C.free(proofData.opening)

	return proofContent, commitment, opening, nil
}

// ProveSpecificOpening Generate proof with a chosen opening
// x: prove x is in the range [0, 2^64)
// opening: the chosen randomness to commit x (secret key)
// return 1: proof in []byte
// return 2: commitment of x using opening
func ProveSpecificOpening(x uint64, opening []byte) ([]byte, []byte, error) {
	if len(opening) != POINT_SIZE {
		return nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": commitment opening")
	}

	var proofData C.proof_content

	ret := C.bulletproofs_prove_with_specific_opening(&proofData, C.uint(x), unsafe.Pointer(&opening[0]))
	if ret != OK {
		return nil, nil, fmt.Errorf("fail to generate proof: " + getErrMsg(int64(ret)))
	}

	proofContent := C.GoBytes(proofData.proof, C.int(SINGLE_PROOF_SIZE))
	commitment := C.GoBytes(proofData.commitment, C.int(POINT_SIZE))

	defer C.free(proofData.proof)
	defer C.free(proofData.commitment)
	defer C.free(proofData.opening)

	return proofContent, commitment, nil
}

// Verify Verify the validity of a proof
// proof: the zero-knowledge proof proving the number committed in commitment is in the range [0, 2^64)
// commitment: commitment bindingly hiding the number x
// return: true on valid proof, false otherwise
func Verify(proof []byte, commitment []byte) (bool, error) {
	if len(proof) != SINGLE_PROOF_SIZE || len(commitment) != POINT_SIZE {
		return false, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": proof length should be 672-byte and commitment length should be 32-byte")
	}
	ret := C.bulletproofs_verify_single(unsafe.Pointer(&proof[0]), unsafe.Pointer(&commitment[0]))
	if ret != OK {
		return false, nil
	}
	return true, nil
}

// ProveAfterAddNum Update a commitment of x (xB + rB') to x + y and generate a proof of it with the same opening
// x, y: prove x + y is in the range [0, 2^64)
// openingX: the randomness r used to commit x, also used in the new proof
// commitmentX: commitment of x: xB + rB'
// return 1: proof in []byte
// return 2: commitment of x + y: (x + y)B + rB'
func ProveAfterAddNum(x, y uint64, openingX, commitmentX []byte) ([]byte, []byte, error) {
	ret, err := PedersenVerify(commitmentX, openingX, x)
	if err != nil {
		return nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": " + err.Error())
	}

	if ret != true {
		return nil, nil, fmt.Errorf(ERR_MSG_DEFAULT + ": verify fail")
	}

	z := x + y
	if int64(z) < 0 {
		return nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": result summation is not in the range [0, 2^64)")
	}

	proof, commitment, err := ProveSpecificOpening(z, openingX)
	if err != nil {
		return nil, nil, err
	}
	commitmentDup, err := PedersenAddNum(commitmentX, y)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to generate proof: " + err.Error())
	}
	if bytes.Compare(commitment, commitmentDup) != 0 {
		return nil, nil, fmt.Errorf("fail to generate proof: result summation is not in the range [0, 2^64)")
	}
	return proof, commitment, nil
}

// ProveAfterAddCommitment Update commitments of x (xB + rB') and y (yB + sB') to x + y and generate a proof of it with the sum of the two opening
// x, y: prove x + y is in the range [0, 2^64)
// openingX: the randomness r used to commit x
// openingY: the randomness s used to commit y
// commitmentX: commitment of x: xB + rB'
// commitmentX: commitment of y: yB + sB'
// return 1: proof in []byte
// return 2: commitment of x + y: (x + y)B + (r + s)B'
// return 3: new opening for the result commitment (r + s)
func ProveAfterAddCommitment(x, y uint64, openingX, openingY, commitmentX, commitmentY []byte) ([]byte, []byte, []byte, error) {
	ret, err := PedersenVerify(commitmentX, openingX, x)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": " + err.Error())
	}

	if ret != true {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_DEFAULT + ": verify fail")
	}

	ret, err = PedersenVerify(commitmentY, openingY, y)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": " + err.Error())
	}

	if ret != true {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_DEFAULT + ": verify fail")
	}

	z := x + y
	if int64(z) < 0 {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": result summation is not in the range [0, 2^64)")
	}

	commitmentDup, opening, err := PedersenAddCommitmentWithOpening(commitmentX, commitmentY, openingX, openingY)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("fail to generate proof: " + err.Error())
	}

	proof, commitment, err := ProveSpecificOpening(z, opening)
	if err != nil {
		return nil, nil, nil, err
	}
	if bytes.Compare(commitment, commitmentDup) != 0 {
		return nil, nil, nil, fmt.Errorf("fail to generate proof: result summation is not in the range [0, 2^64)")
	}
	return proof, commitment, opening, nil
}

// ProveAfterSubNum Update a commitment of x (xB + rB') to x - y and generate a proof of it with the same opening
// x, y: prove x - y is in the range [0, 2^64)
// openingX: the randomness r used to commit x, also used in the new proof
// commitmentX: commitment of x (old commitment)
// return 1: proof in []byte
// return 2: commitment of x - y: (x - y)B + rB'
func ProveAfterSubNum(x, y uint64, openingX, commitmentX []byte) ([]byte, []byte, error) {
	ret, err := PedersenVerify(commitmentX, openingX, x)
	if err != nil {
		return nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": " + err.Error())
	}

	if ret != true {
		return nil, nil, fmt.Errorf(ERR_MSG_DEFAULT + ": verify fail")
	}

	z := x - y
	if int64(z) < 0 {
		return nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": result subtraction is not in the range [0, 2^64)")
	}

	proof, commitment, err := ProveSpecificOpening(z, openingX)
	if err != nil {
		return nil, nil, err
	}
	commitmentDup, err := PedersenSubNum(commitmentX, y)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to generate proof: " + err.Error())
	}
	if bytes.Compare(commitment, commitmentDup) != 0 {
		return nil, nil, fmt.Errorf("fail to generate proof: result subtraction is not in the range [0, 2^64)")
	}
	return proof, commitment, nil
}

// ProveAfterSubCommitment Update commitments of x (xB + rB') and y (yB + sB') to x - y and generate a proof of it with the subtraction of the two openings
// x, y: prove x + y is in the range [0, 2^64)
// openingX: the randomness r used to commit x
// openingY: the randomness s used to commit y
// commitmentX: commitment of x: xB + rB'
// commitmentX: commitment of y: yB + sB'
// return 1: proof in []byte
// return 2: commitment of x - y: (x - y)B + (r - s)B'
// return 3: new opening for the result commitment (r - s)
func ProveAfterSubCommitment(x, y uint64, openingX, openingY, commitmentX, commitmentY []byte) ([]byte, []byte, []byte, error) {
	ret, err := PedersenVerify(commitmentX, openingX, x)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": " + err.Error())
	}

	if ret != true {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_DEFAULT + ": verify fail")
	}

	ret, err = PedersenVerify(commitmentY, openingY, y)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": " + err.Error())
	}

	if ret != true {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_DEFAULT + ": verify fail")
	}

	z := x - y
	if int64(z) < 0 {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": result subtraction is not in the range [0, 2^64)")
	}

	commitmentDup, opening, err := PedersenSubCommitmentWithOpening(commitmentX, commitmentY, openingX, openingY)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("fail to generate proof: " + err.Error())
	}

	proof, commitment, err := ProveSpecificOpening(z, opening)
	if err != nil {
		return nil, nil, nil, err
	}
	if bytes.Compare(commitment, commitmentDup) != 0 {
		return nil, nil, nil, fmt.Errorf("fail to generate proof: result subtraction is not in the range [0, 2^64)")
	}
	return proof, commitment, opening, nil
}

// ProveAfterMulNum Update commitment of x (xB + rB') to commitment of x * y and generate a proof of it with the an updated opening, where y is a value
// x, y: prove x * y is in the range [0, 2^64)
// openingX: the randomness r used to commit x
// commitmentX: commitment of x: xB + rB'
// return 1: proof in []byte
// return 2: commitment of x * y: (x * y)B + (r * y)B'
// return 3: new opening for the result commitment: r * y
func ProveAfterMulNum(x, y uint64, openingX, commitmentX []byte) ([]byte, []byte, []byte, error) {
	ret, err := PedersenVerify(commitmentX, openingX, x)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": " + err.Error())
	}

	if ret != true {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_DEFAULT + ": verify fail")
	}

	z := x * y
	if int64(z) < 0 {
		return nil, nil, nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": result multiplication is not in the range [0, 2^64)")
	}

	var opening [POINT_SIZE]byte
	openingSlice := opening[:]
	rt := C.pedersen_scalar_mul(unsafe.Pointer(&openingSlice[0]), unsafe.Pointer(&openingX[0]), C.uint(y))
	if rt != OK {
		return nil, nil, nil, fmt.Errorf("fail to compute new opening for the multiplicaiton")
	}
	proof, commitment, err := ProveSpecificOpening(z, openingSlice)
	if err != nil {
		return nil, nil, nil, err
	}
	commitmentDup, openingDup, err := PedersenMulNumWithOpening(commitmentX, openingX, y)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("fail to generate proof: " + err.Error())
	}
	if bytes.Compare(commitment, commitmentDup) != 0 || bytes.Compare(openingSlice, openingDup) != 0 {
		return nil, nil, nil, fmt.Errorf("fail to generate proof: result multiplication is not in the range [0, 2^64)")
	}
	return proof, commitment, openingSlice, nil
}
