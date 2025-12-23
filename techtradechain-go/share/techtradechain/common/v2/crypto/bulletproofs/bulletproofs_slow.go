//go:build !linux || !amd64
// +build !linux !amd64

/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bulletproofs

import "techtradechain.com/techtradechain/common/v2/crypto/bulletproofs/bulletproofs_nocgo"

// ProveRandomOpening Generate proof with randomly pick opening
// x: prove x is in the range [0, 2^64)
// return 1: proof in []byte
// return 2: commitment of x: xB + rB'
// return 3: opening, the randomness r used to commit x (secret key)
func ProveRandomOpening(x uint64) ([]byte, []byte, []byte, error) {
	return bulletproofs_nocgo.ProveRandomOpening(x)
}

// ProveSpecificOpening Generate proof with a chosen opening
// x: prove x is in the range [0, 2^64)
// opening: the chosen randomness to commit x (secret key)
// return 1: proof in []byte
// return 2: commitment of x using opening
func ProveSpecificOpening(x uint64, opening []byte) ([]byte, []byte, error) {
	return bulletproofs_nocgo.ProveSpecificOpening(x, opening)
}

// Verify Verify the validity of a proof
// proof: the zero-knowledge proof proving the number committed in commitment is in the range [0, 2^64)
// commitment: commitment bindingly hiding the number x
// return: true on valid proof, false otherwise
func Verify(proof []byte, commitment []byte) (bool, error) {
	return bulletproofs_nocgo.Verify(proof, commitment)
}

// ProveAfterAddNum Update a commitment of x (xB + rB') to x + y and generate a proof of it with the same opening
// x, y: prove x + y is in the range [0, 2^64)
// openingX: the randomness r used to commit x, also used in the new proof
// commitmentX: commitment of x: xB + rB'
// return 1: proof in []byte
// return 2: commitment of x + y: (x + y)B + rB'
func ProveAfterAddNum(x, y uint64, openingX, commitmentX []byte) ([]byte, []byte, error) {
	return bulletproofs_nocgo.ProveAfterAddNum(x, y, openingX, commitmentX)
}

// ProveAfterAddCommitment Update commitments of x (xB + rB') and y (yB + sB') to x + y and
// generate a proof of it with the sum of the two opening
// x, y: prove x + y is in the range [0, 2^64)
// openingX: the randomness r used to commit x
// openingY: the randomness s used to commit y
// commitmentX: commitment of x: xB + rB'
// commitmentX: commitment of y: yB + sB'
// return 1: proof in []byte
// return 2: commitment of x + y: (x + y)B + (r + s)B'
// return 3: new opening for the result commitment (r + s)
func ProveAfterAddCommitment(x, y uint64, openingX, openingY, commitmentX, commitmentY []byte) (
	[]byte, []byte, []byte, error) {
	return bulletproofs_nocgo.ProveAfterAddCommitment(x, y, openingX, openingY, commitmentX, commitmentY)
}

// ProveAfterSubNum Update a commitment of x (xB + rB') to x - y and generate a proof of it with the same opening
// x, y: prove x - y is in the range [0, 2^64)
// openingX: the randomness r used to commit x, also used in the new proof
// commitmentX: commitment of x (old commitment)
// return 1: proof in []byte
// return 2: commitment of x - y: (x - y)B + rB'
func ProveAfterSubNum(x, y uint64, openingX, commitmentX []byte) ([]byte, []byte, error) {
	return bulletproofs_nocgo.ProveAfterSubNum(x, y, openingX, commitmentX)
}

// ProveAfterSubCommitment Update commitments of x (xB + rB') and y (yB + sB') to x - y and generate a proof of
// it with the subtraction of the two openings
// x, y: prove x + y is in the range [0, 2^64)
// openingX: the randomness r used to commit x
// openingY: the randomness s used to commit y
// commitmentX: commitment of x: xB + rB'
// commitmentX: commitment of y: yB + sB'
// return 1: proof in []byte
// return 2: commitment of x - y: (x - y)B + (r - s)B'
// return 3: new opening for the result commitment (r - s)
func ProveAfterSubCommitment(x, y uint64, openingX, openingY, commitmentX, commitmentY []byte) (
	[]byte, []byte, []byte, error) {
	return bulletproofs_nocgo.ProveAfterSubCommitment(x, y, openingX, openingY, commitmentX, commitmentY)
}

// ProveAfterMulNum Update commitment of x (xB + rB') to commitment of x * y and generate a proof of it with the
// updated opening, where y is a value
// x, y: prove x * y is in the range [0, 2^64)
// openingX: the randomness r used to commit x
// commitmentX: commitment of x: xB + rB'
// return 1: proof in []byte
// return 2: commitment of x * y: (x * y)B + (r * y)B'
// return 3: new opening for the result commitment: r * y
func ProveAfterMulNum(x, y uint64, openingX, commitmentX []byte) ([]byte, []byte, []byte, error) {
	return bulletproofs_nocgo.ProveAfterMulNum(x, y, openingX, commitmentX)
}

// PedersenRNG generate a truly random scalar (which can be used as an opening to generate a commitment).
// return: a random scalar in []byte format
func PedersenRNG() ([]byte, error) {
	return bulletproofs_nocgo.PedersenRNG()
}

// PedersenCommitRandomOpening compute Pedersen commitment on a value x with a randomly chosen opening
// x: the value to commit
// return1: commitment C = xB + rB'
// return2: opening r (randomly picked)
func PedersenCommitRandomOpening(x uint64) ([]byte, []byte, error) {
	return bulletproofs_nocgo.PedersenCommitRandomOpening(x)
}

// PedersenCommitSpecificOpening compute Pedersen commitment on a value x with a given opening
// x: the value to commit
// return1: commitment C = xB + rB'
func PedersenCommitSpecificOpening(x uint64, r []byte) ([]byte, error) {
	return bulletproofs_nocgo.PedersenCommitSpecificOpening(x, r)
}

// PedersenVerify verify the validity of a commitment with respect to a value-opening pair
// commitment: the commitment to be opened or verified: xB + rB'
// opening: the opening of the commitment: r
// value: the value claimed being binding to commitment: x
// return1: true if commitment is valid, false otherwise
func PedersenVerify(commitment, opening []byte, value uint64) (bool, error) {
	return bulletproofs_nocgo.PedersenVerify(commitment, opening, value)
}

// PedersenNeg Compute a commitment to -x from a commitment to x without revealing the value x
// commitment: C = xB + rB'
// value: the value y
// return1: the new commitment to x + y: C' = (x + y)B + rB'
func PedersenNeg(commitment []byte) ([]byte, error) {
	return bulletproofs_nocgo.PedersenNeg(commitment)
}

// PedersenNegOpening Compute the negation of opening. Openings are big numbers with 256 bits.
// opening: the opening r to be negated
// return: the result opening: -r
func PedersenNegOpening(opening []byte) ([]byte, error) {
	return bulletproofs_nocgo.PedersenNegOpening(opening)
}

// PedersenAddNum Compute a commitment to x + y from a commitment to x without revealing the value x,
// where y is a scalar
// commitment: C = xB + rB'
// value: the value y
// return1: the new commitment to x + y: C' = (x + y)B + rB'
func PedersenAddNum(commitment []byte, value uint64) ([]byte, error) {
	return bulletproofs_nocgo.PedersenAddNum(commitment, value)
}

// PedersenAddCommitment Compute a commitment to x + y from commitments to x and y, without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return: commitment to x + y: C = (x + y)B + (r + s)B'
func PedersenAddCommitment(commitment1, commitment2 []byte) ([]byte, error) {
	return bulletproofs_nocgo.PedersenAddCommitment(commitment1, commitment2)
}

// PedersenAddOpening Compute the sum of two openings. Openings are big numbers with 256 bits.
// opening1, opening2: the two openings r and s to be summed
// return: the result opening: r + s
func PedersenAddOpening(opening1, opening2 []byte) ([]byte, error) {
	return bulletproofs_nocgo.PedersenAddOpening(opening1, opening2)
}

// PedersenAddCommitmentWithOpening Compute a commitment to x + y without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return1: the new commitment to x + y: C' = (x + y)B + rB'
// return2: the new opening r + s
func PedersenAddCommitmentWithOpening(commitment1, commitment2, opening1, opening2 []byte) ([]byte, []byte, error) {
	return bulletproofs_nocgo.PedersenAddCommitmentWithOpening(commitment1, commitment2, opening1, opening2)
}

// PedersenSubNum Compute a commitment to x - y from a commitment to x without revealing the value x,
// where y is a scalar
// commitment: C = xB + rB'
// value: the value y
// return1: the new commitment to x - y: C' = (x - y)B + rB'
func PedersenSubNum(commitment []byte, value uint64) ([]byte, error) {
	return bulletproofs_nocgo.PedersenSubNum(commitment, value)
}

// PedersenSubCommitment Compute a commitment to x - y from commitments to x and y, without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return: commitment to x - y: C = (x - y)B + (r - s)B'
func PedersenSubCommitment(commitment1, commitment2 []byte) ([]byte, error) {
	return bulletproofs_nocgo.PedersenSubCommitment(commitment1, commitment2)
}

// PedersenSubOpening Compute opening1 - opening2. Openings are big numbers with 256 bits.
// opening1, opening2: two openings r and s
// return: the result opening r - s
func PedersenSubOpening(opening1, opening2 []byte) ([]byte, error) {
	return bulletproofs_nocgo.PedersenSubOpening(opening1, opening2)
}

// PedersenSubCommitmentWithOpening Compute a commitment to x - y without from two commitments of x and y respectively
// commitment1: commitment to x: Cx = xB + rB'
// commitment2: commitment to y: Cy = yB + sB'
// return1: the new commitment to x - y: C' = (x - y)B + (r - s)B'
// return2: the new opening r - s
func PedersenSubCommitmentWithOpening(commitment1, commitment2, opening1, opening2 []byte) ([]byte, []byte, error) {
	return bulletproofs_nocgo.PedersenSubCommitmentWithOpening(commitment1, commitment2, opening1, opening2)
}

// PedersenMulNum Compute a commitment to x * y from a commitment to x and an integer y,
// without revealing the value x and y
// commitment1: commitment to x: Cx = xB + rB'
// value: integer value y
// return: commitment to x * y: C = (x * y)B + (r * y)B'
func PedersenMulNum(commitment1 []byte, value uint64) ([]byte, error) {
	return bulletproofs_nocgo.PedersenMulNum(commitment1, value)
}

// PedersenMulOpening Compute opening1 * integer. Openings are big numbers with 256 bits.
// opening1: the input opening r
// value: the input integer value y
// return: the multiplication r * y as a big number with 256 bits in []byte form
func PedersenMulOpening(opening1 []byte, value uint64) ([]byte, error) {
	return bulletproofs_nocgo.PedersenMulOpening(opening1, value)
}

// PedersenMulNumWithOpening Compute a commitment to x * y from a commitment to x and an integer y,
// without revealing the value x and y
// commitment: commitment to x: Cx = xB + rB'
// opening: opening to Cx: r
// value: integer value y
// return1: commitment to x * y: C = (x * y)B + (r * y)B'
// return2: opening to the result commitment: r * y
func PedersenMulNumWithOpening(commitment []byte, opening []byte, value uint64) ([]byte, []byte, error) {
	return bulletproofs_nocgo.PedersenMulNumWithOpening(commitment, opening, value)
}
