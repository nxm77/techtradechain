/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bulletproofs_nocgo

// ProveRandomOpening Generate proof with randomly pick opening
// x: prove x is in the range [0, 2^64)
// return 1: proof in []byte
// return 2: commitment of x: xB + rB'
// return 3: opening, the randomness r used to commit x (secret key)
func ProveRandomOpening(x uint64) ([]byte, []byte, []byte, error) {
	return nil, nil, nil, ErrUnsupported
}

// ProveSpecificOpening Generate proof with a chosen opening
// x: prove x is in the range [0, 2^64)
// opening: the chosen randomness to commit x (secret key)
// return 1: proof in []byte
// return 2: commitment of x using opening
func ProveSpecificOpening(x uint64, opening []byte) ([]byte, []byte, error) {
	return nil, nil, ErrUnsupported
}

// Verify Verify the validity of a proof
// proof: the zero-knowledge proof proving the number committed in commitment is in the range [0, 2^64)
// commitment: commitment bindingly hiding the number x
// return: true on valid proof, false otherwise
func Verify(proof []byte, commitment []byte) (bool, error) {
	return false, ErrUnsupported
}

// ProveAfterAddNum Update a commitment of x (xB + rB') to x + y and generate a proof of it with the same opening
// x, y: prove x + y is in the range [0, 2^64)
// openingX: the randomness r used to commit x, also used in the new proof
// commitmentX: commitment of x: xB + rB'
// return 1: proof in []byte
// return 2: commitment of x + y: (x + y)B + rB'
func ProveAfterAddNum(x, y uint64, openingX, commitmentX []byte) ([]byte, []byte, error) {
	return nil, nil, ErrUnsupported
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
	return nil, nil, nil, ErrUnsupported
}

// ProveAfterSubNum Update a commitment of x (xB + rB') to x - y and generate a proof of it with the same opening
// x, y: prove x - y is in the range [0, 2^64)
// openingX: the randomness r used to commit x, also used in the new proof
// commitmentX: commitment of x (old commitment)
// return 1: proof in []byte
// return 2: commitment of x - y: (x - y)B + rB'
func ProveAfterSubNum(x, y uint64, openingX, commitmentX []byte) ([]byte, []byte, error) {
	return nil, nil, ErrUnsupported
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
	return nil, nil, nil, ErrUnsupported
}

// ProveAfterMulNum Update commitment of x (xB + rB') to commitment of x * y and generate a proof of it with the an updated opening, where y is a value
// x, y: prove x * y is in the range [0, 2^64)
// openingX: the randomness r used to commit x
// commitmentX: commitment of x: xB + rB'
// return 1: proof in []byte
// return 2: commitment of x * y: (x * y)B + (r * y)B'
// return 3: new opening for the result commitment: r * y
func ProveAfterMulNum(x, y uint64, openingX, commitmentX []byte) ([]byte, []byte, []byte, error) {
	return nil, nil, nil, ErrUnsupported
}
