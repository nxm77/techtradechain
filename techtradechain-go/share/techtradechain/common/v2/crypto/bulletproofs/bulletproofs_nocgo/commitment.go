/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bulletproofs_nocgo

import "C"

// PedersenRNG generate a truly random scalar (which can be used as an opening to generate a commitment).
// return: a random scalar in []byte format
func PedersenRNG() ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenCommitRandomOpening compute Pedersen commitment on a value x with a randomly chosen opening
// x: the value to commit
// return1: commitment C = xB + rB'
// return2: opening r (randomly picked)
func PedersenCommitRandomOpening(x uint64) ([]byte, []byte, error) {
	return nil, nil, ErrUnsupported
}

// PedersenCommitSpecificOpening compute Pedersen commitment on a value x with a given opening
// x: the value to commit
// return1: commitment C = xB + rB'
func PedersenCommitSpecificOpening(x uint64, r []byte) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenVerify verify the validity of a commitment with respect to a value-opening pair
// commitment: the commitment to be opened or verified: xB + rB'
// opening: the opening of the commitment: r
// value: the value claimed being binding to commitment: x
// return1: true if commitment is valid, false otherwise
func PedersenVerify(commitment, opening []byte, value uint64) (bool, error) {
	return false, ErrUnsupported
}

// PedersenNeg Compute a commitment to -x from a commitment to x without revealing the value x
// commitment: C = xB + rB'
// value: the value y
// return1: the new commitment to x + y: C' = (x + y)B + rB'
func PedersenNeg(commitment []byte) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenNegOpening Compute the negation of opening. Openings are big numbers with 256 bits.
// opening: the opening r to be negated
// return: the result opening: -r
func PedersenNegOpening(opening []byte) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenAddNum Compute a commitment to x + y from a commitment to x without revealing the value x, where y is a scalar
// commitment: C = xB + rB'
// value: the value y
// return1: the new commitment to x + y: C' = (x + y)B + rB'
func PedersenAddNum(commitment []byte, value uint64) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenAddCommitment Compute a commitment to x + y from commitments to x and y, without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return: commitment to x + y: C = (x + y)B + (r + s)B'
func PedersenAddCommitment(commitment1, commitment2 []byte) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenAddOpening Compute the sum of two openings. Openings are big numbers with 256 bits.
// opening1, opening2: the two openings r and s to be summed
// return: the result opening: r + s
func PedersenAddOpening(opening1, opening2 []byte) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenAddCommitmentWithOpening Compute a commitment to x + y without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return1: the new commitment to x + y: C' = (x + y)B + rB'
// return2: the new opening r + s
func PedersenAddCommitmentWithOpening(commitment1, commitment2, opening1, opening2 []byte) ([]byte, []byte, error) {
	return nil, nil, ErrUnsupported
}

// PedersenSubNum Compute a commitment to x - y from a commitment to x without revealing the value x, where y is a scalar
// commitment: C = xB + rB'
// value: the value y
// return1: the new commitment to x - y: C' = (x - y)B + rB'
func PedersenSubNum(commitment []byte, value uint64) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenSubCommitment Compute a commitment to x - y from commitments to x and y, without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return: commitment to x - y: C = (x - y)B + (r - s)B'
func PedersenSubCommitment(commitment1, commitment2 []byte) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenSubOpening Compute opening1 - opening2. Openings are big numbers with 256 bits.
// opening1, opening2: two openings r and s
// return: the result opening r - s
func PedersenSubOpening(opening1, opening2 []byte) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenSubCommitmentWithOpening Compute a commitment to x - y without from two commitments of x and y respectively
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return1: the new commitment to x - y: C' = (x - y)B + (r - s)B'
// return2: the new opening r - s
func PedersenSubCommitmentWithOpening(commitment1, commitment2, opening1, opening2 []byte) ([]byte, []byte, error) {
	return nil, nil, ErrUnsupported
}

// PedersenMulNum Compute a commitment to x * y from a commitment to x and an integer y, without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// value: integer value y
// return: commitment to x * y: C = (x * y)B + (r * y)B'
func PedersenMulNum(commitment1 []byte, value uint64) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenMulOpening Compute opening1 * integer. Openings are big numbers with 256 bits.
// opening1: the input opening r
// value: the input integer value y
// return: the multiplication r * y as a big number with 256 bits in []byte form
func PedersenMulOpening(opening1 []byte, value uint64) ([]byte, error) {
	return nil, ErrUnsupported
}

// PedersenMulNumWithOpening Compute a commitment to x * y from a commitment to x and an integer y, without revealing the value x and y
// commitment: commitment to x: Cx = xB + rB'
// opening: opening to Cx: r
// value: integer value y
// return1: commitment to x * y: C = (x * y)B + (r * y)B'
// return2: opening to the result commitment: r * y
func PedersenMulNumWithOpening(commitment []byte, opening []byte, value uint64) ([]byte, []byte, error) {
	return nil, nil, ErrUnsupported
}
