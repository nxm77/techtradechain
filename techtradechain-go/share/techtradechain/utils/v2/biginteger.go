/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"fmt"
	"math/big"
)

// DecBase 10
const DecBase = 10

// BigInteger wrapper for big.Int
type BigInteger struct {
	Value *big.Int
}

// NewBigInteger create a BigInteger
func NewBigInteger(value string) *BigInteger {
	var val big.Int
	newVal, ok := val.SetString(value, DecBase)
	if ok {
		return &BigInteger{
			Value: newVal,
		}
	}
	return NewZeroBigInteger()
}

// NewZeroBigInteger create a BigInteger of zero
func NewZeroBigInteger() *BigInteger {
	return &BigInteger{
		Value: big.NewInt(0),
	}
}

// Add adds y to x
func (x *BigInteger) Add(y *BigInteger) {
	x.Value = x.Value.Add(x.Value, y.Value)
}

// Sub subs y from x
func (x *BigInteger) Sub(y *BigInteger) {
	x.Value = x.Value.Sub(x.Value, y.Value)
}

// Cmp compares x and y and returns:
//
//   -1 if x <  y
//    0 if x == y
//   +1 if x >  y
func (x *BigInteger) Cmp(y *BigInteger) int {
	return x.Value.Cmp(y.Value)
}

// String gets string
func (x *BigInteger) String() string {
	return x.Value.String()
}

// Sub subtracts two big integer
func Sub(x, y *BigInteger) *BigInteger {
	z := NewBigInteger(x.String())
	z.Sub(y)
	return z
}

// Sum adds two big integer
func Sum(x, y *BigInteger) *BigInteger {
	z := NewZeroBigInteger()
	z.Add(x)
	z.Add(y)
	return z
}

func isValidBigInt(val string) error {
	_, ok := big.NewInt(0).SetString(val, 10)
	if !ok {
		return fmt.Errorf("parse string to big.Int failed, actual: %s", val)
	}
	return nil
}
