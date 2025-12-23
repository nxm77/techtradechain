//+build linux,amd64

/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

/*
  Pedersen commitment is a commitment scheme which bindingly hides a value with a randomness (called opening). This commitment scheme has two major properties: hiding and binding.
  Hiding: given a commitment, it is hard to guess the right value committed to it.
  Binding: given a commitment, the value committed to it, and the opening, it is hard to find another value-opening pair for the commitment.
  A commitment can be opened. This does not mean that the committed value can be extracted from it like a decryption on a ciphertext. It means that with the opening and the value, the validity of this commitment can be verified.
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
	"fmt"
	"unsafe"
)

// PedersenRNG generate a truly random scalar (which can be used as an opening to generate a commitment).
// return: a random scalar in []byte format
func PedersenRNG() ([]byte, error) {
	var r [POINT_SIZE]byte
	randomness := r[:]
	ret := C.bulletproofs_generate_random_scalar(unsafe.Pointer(&randomness[0]))
	if ret != OK {
		return nil, fmt.Errorf("fail to generate random scalar: " + getErrMsg(int64(ret)))
	}
	return randomness, nil
}

// PedersenCommitRandomOpening compute Pedersen commitment on a value x with a randomly chosen opening
// x: the value to commit
// return1: commitment C = xB + rB'
// return2: opening r (randomly picked)
func PedersenCommitRandomOpening(x uint64) ([]byte, []byte, error) {
	var commitment [POINT_SIZE]byte
	var opening [POINT_SIZE]byte
	commitmentSlice := commitment[:]
	openingSlice := opening[:]

	ret := C.pedersen_commit_with_random_opening(unsafe.Pointer(&commitmentSlice[0]), unsafe.Pointer(&openingSlice[0]), C.uint(x))
	if ret != OK {
		return nil, nil, fmt.Errorf("fail to generate commitment:" + getErrMsg(int64(ret)))
	}

	return commitmentSlice, openingSlice, nil
}

// PedersenCommitSpecificOpening compute Pedersen commitment on a value x with a given opening
// x: the value to commit
// return1: commitment C = xB + rB'
func PedersenCommitSpecificOpening(x uint64, r []byte) ([]byte, error) {
	if len(r) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": commitment opening length should 32-byte")
	}
	var commitment [POINT_SIZE]byte
	commitmentSlice := commitment[:]

	ret := C.pedersen_commit_with_specific_opening(unsafe.Pointer(&commitmentSlice[0]), unsafe.Pointer(&r[0]), C.uint(x))
	if ret != OK {
		return nil, fmt.Errorf("fail to generate commitment: " + getErrMsg(int64(ret)))
	}

	return commitmentSlice, nil
}

// PedersenVerify verify the validity of a commitment with respect to a value-opening pair
// commitment: the commitment to be opened or verified: xB + rB'
// opening: the opening of the commitment: r
// value: the value claimed being binding to commitment: x
// return1: true if commitment is valid, false otherwise
func PedersenVerify(commitment, opening []byte, value uint64) (bool, error) {
	if len(commitment) != POINT_SIZE || len(opening) != POINT_SIZE {
		return false, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": commitment or opening length should 32-byte")
	}
	ret := C.pedersen_verify(unsafe.Pointer(&commitment[0]), unsafe.Pointer(&opening[0]), C.uint(value))
	if ret == 1 {
		return true, nil
	}
	return false, nil
}

// PedersenNeg Compute a commitment to -x from a commitment to x without revealing the value x
// commitment: C = xB + rB'
// value: the value y
// return1: the new commitment to x + y: C' = (x + y)B + rB'
func PedersenNeg(commitment []byte) ([]byte, error) {
	if len(commitment) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": commitment length should 32-byte")
	}
	var result [POINT_SIZE]byte
	resultSlice := result[:]

	ret := C.pedersen_point_neg(unsafe.Pointer(&resultSlice[0]), unsafe.Pointer(&commitment[0]))
	if ret != OK {
		return nil, fmt.Errorf("fail to compute negation: " + getErrMsg(int64(ret)))
	}
	return resultSlice, nil
}

// PedersenNegOpening Compute the negation of opening. Openings are big numbers with 256 bits.
// opening: the opening r to be negated
// return: the result opening: -r
func PedersenNegOpening(opening []byte) ([]byte, error) {
	if len(opening) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": opening length should 32-byte")
	}
	var openingNeg [POINT_SIZE]byte
	openingSlice := openingNeg[:]

	ret := C.pedersen_scalar_neg(unsafe.Pointer(&openingSlice[0]),
		unsafe.Pointer(&opening[0]))
	if ret != OK {
		return nil, fmt.Errorf("fail to compute opening negation: " + getErrMsg(int64(ret)))
	}
	return openingSlice, nil
}

// PedersenAddNum Compute a commitment to x + y from a commitment to x without revealing the value x, where y is a scalar
// commitment: C = xB + rB'
// value: the value y
// return1: the new commitment to x + y: C' = (x + y)B + rB'
func PedersenAddNum(commitment []byte, value uint64) ([]byte, error) {
	if len(commitment) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": commitment length should 32-byte")
	}
	var result [POINT_SIZE]byte
	resultSlice := result[:]

	ret := C.pedersen_commitment_add_num(unsafe.Pointer(&resultSlice[0]), unsafe.Pointer(&commitment[0]), C.uint(value))
	if ret != OK {
		return nil, fmt.Errorf("fail to compute Pedersen addition: " + getErrMsg(int64(ret)))
	}
	return resultSlice, nil
}

// PedersenAddCommitment Compute a commitment to x + y from commitments to x and y, without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return: commitment to x + y: C = (x + y)B + (r + s)B'
func PedersenAddCommitment(commitment1, commitment2 []byte) ([]byte, error) {
	if len(commitment1) != POINT_SIZE || len(commitment2) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": commitment length should 32-byte")
	}
	var commitment [POINT_SIZE]byte
	commitmentSlice := commitment[:]

	ret := C.pedersen_point_add(unsafe.Pointer(&commitmentSlice[0]),
		unsafe.Pointer(&commitment1[0]),
		unsafe.Pointer(&commitment2[0]))
	if ret != OK {
		return nil, fmt.Errorf("fail to compute Pedersen addition: " + getErrMsg(int64(ret)))
	}
	return commitmentSlice, nil
}

// PedersenAddOpening Compute the sum of two openings. Openings are big numbers with 256 bits.
// opening1, opening2: the two openings r and s to be summed
// return: the result opening: r + s
func PedersenAddOpening(opening1, opening2 []byte) ([]byte, error) {
	if len(opening1) != POINT_SIZE || len(opening2) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": opening length should 32-byte")
	}
	var opening [POINT_SIZE]byte
	openingSlice := opening[:]

	ret := C.pedersen_scalar_add(unsafe.Pointer(&openingSlice[0]),
		unsafe.Pointer(&opening1[0]),
		unsafe.Pointer(&opening2[0]))
	if ret != OK {
		return nil, fmt.Errorf("fail to compute opening addition: " + getErrMsg(int64(ret)))
	}
	return openingSlice, nil
}

// PedersenAddCommitmentWithOpening Compute a commitment to x + y without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return1: the new commitment to x + y: C' = (x + y)B + rB'
// return2: the new opening r + s
func PedersenAddCommitmentWithOpening(commitment1, commitment2, opening1, opening2 []byte) ([]byte, []byte, error) {
	commitment, err := PedersenAddCommitment(commitment1, commitment2)
	if err != nil {
		return nil, nil, err
	}
	opening, err := PedersenAddOpening(opening1, opening2)
	if err != nil {
		return nil, nil, err
	}
	return commitment, opening, nil
}

// PedersenSubNum Compute a commitment to x - y from a commitment to x without revealing the value x, where y is a scalar
// commitment: C = xB + rB'
// value: the value y
// return1: the new commitment to x - y: C' = (x - y)B + rB'
func PedersenSubNum(commitment []byte, value uint64) ([]byte, error) {
	if len(commitment) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": commitment length should 32-byte")
	}
	var result [POINT_SIZE]byte
	resultSlice := result[:]

	ret := C.pedersen_commitment_sub_num(unsafe.Pointer(&resultSlice[0]), unsafe.Pointer(&commitment[0]), C.uint(value))
	if ret != OK {
		return nil, fmt.Errorf("fail to compute Pedersen subtraction: " + getErrMsg(int64(ret)))
	}
	return resultSlice, nil
}

// PedersenSubCommitment Compute a commitment to x - y from commitments to x and y, without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return: commitment to x - y: C = (x - y)B + (r - s)B'
func PedersenSubCommitment(commitment1, commitment2 []byte) ([]byte, error) {
	if len(commitment1) != POINT_SIZE || len(commitment2) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": commitment length should 32-byte")
	}
	var commitment [POINT_SIZE]byte
	commitmentSlice := commitment[:]

	ret := C.pedersen_point_sub(unsafe.Pointer(&commitmentSlice[0]),
		unsafe.Pointer(&commitment1[0]),
		unsafe.Pointer(&commitment2[0]))
	if ret != OK {
		return nil, fmt.Errorf("fail to compute Pedersen subtraction: " + getErrMsg(int64(ret)))
	}
	return commitmentSlice, nil
}

// PedersenSubOpening Compute opening1 - opening2. Openings are big numbers with 256 bits.
// opening1, opening2: two openings r and s
// return: the result opening r - s
func PedersenSubOpening(opening1, opening2 []byte) ([]byte, error) {
	if len(opening1) != POINT_SIZE || len(opening2) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": commitment or opening length should 32-byte")
	}
	var opening [POINT_SIZE]byte
	openingSlice := opening[:]

	ret := C.pedersen_scalar_sub(unsafe.Pointer(&openingSlice[0]),
		unsafe.Pointer(&opening1[0]),
		unsafe.Pointer(&opening2[0]))
	if ret != OK {
		return nil, fmt.Errorf("fail to compute opening subtraction: " + getErrMsg(int64(ret)))
	}
	return openingSlice, nil
}

// PedersenSubCommitmentWithOpening Compute a commitment to x - y without from two commitments of x and y respectively
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return1: the new commitment to x - y: C' = (x - y)B + (r - s)B'
// return2: the new opening r - s
func PedersenSubCommitmentWithOpening(commitment1, commitment2, opening1, opening2 []byte) ([]byte, []byte, error) {
	commitment, err := PedersenSubCommitment(commitment1, commitment2)
	if err != nil {
		return nil, nil, err
	}
	opening, err := PedersenSubOpening(opening1, opening2)
	if err != nil {
		return nil, nil, err
	}
	return commitment, opening, nil
}

// PedersenMulNum Compute a commitment to x * y from a commitment to x and an integer y, without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// value: integer value y
// return: commitment to x * y: C = (x * y)B + (r * y)B'
func PedersenMulNum(commitment1 []byte, value uint64) ([]byte, error) {
	if len(commitment1) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": commitment length should 32-byte")
	}
	var commitment [POINT_SIZE]byte
	commitmentSlice := commitment[:]

	ret := C.pedersen_commitment_mul_num(unsafe.Pointer(&commitmentSlice[0]),
		unsafe.Pointer(&commitment1[0]),
		C.uint(value))
	if ret != OK {
		return nil, fmt.Errorf("fail to compute Pedersen multiplication: " + getErrMsg(int64(ret)))
	}
	return commitmentSlice, nil
}

// PedersenMulOpening Compute opening1 * integer. Openings are big numbers with 256 bits.
// opening1: the input opening r
// value: the input integer value y
// return: the multiplication r * y as a big number with 256 bits in []byte form
func PedersenMulOpening(opening1 []byte, value uint64) ([]byte, error) {
	if len(opening1) != POINT_SIZE {
		return nil, fmt.Errorf(ERR_MSG_INVALID_INPUT + ": opening length should 32-byte")
	}
	var opening [POINT_SIZE]byte
	openingSlice := opening[:]

	ret := C.pedersen_scalar_mul(unsafe.Pointer(&openingSlice[0]),
		unsafe.Pointer(&opening1[0]),
		C.uint(value))
	if ret != OK {
		return nil, fmt.Errorf("fail to compute opening multiplication: " + getErrMsg(int64(ret)))
	}
	return openingSlice, nil
}

// PedersenMulNumWithOpening Compute a commitment to x * y from a commitment to x and an integer y, without revealing the value x and y
// commitment: commitment to x: Cx = xB + rB'
// opening: opening to Cx: r
// value: integer value y
// return1: commitment to x * y: C = (x * y)B + (r * y)B'
// return2: opening to the result commitment: r * y
func PedersenMulNumWithOpening(commitment []byte, opening []byte, value uint64) ([]byte, []byte, error) {
	commitmentNew, err := PedersenMulNum(commitment, value)
	if err != nil {
		return nil, nil, err
	}
	openingNew, err := PedersenMulOpening(opening, value)
	if err != nil {
		return nil, nil, err
	}
	return commitmentNew, openingNew, nil
}
