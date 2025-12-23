/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkcs11

import (
	"errors"
	"fmt"
)

const (
	defaultRandomLen = 6
)

// GenerateOTP returns a random string with a fixed length
func GenerateOTP(p11 *P11Handle, length int) (string, error) {
	if length <= 0 {
		length = defaultRandomLen
	}
	randBytes, err := GenerateBytesOTP(p11, length)
	if err != nil {
		return "", err
	}
	r, err := bytesToInt(randBytes)
	if err != nil {
		return "", err
	}
	rStr := fmt.Sprintf("%0*d", length, r)
	if len(rStr) < length {
		return "", errors.New("generate random failed, len is too low")
	}
	return rStr[:length], nil
}

// GenerateBytesOTP generate a fixed length hardware random
func GenerateBytesOTP(p11 *P11Handle, length int) ([]byte, error) {
	if length <= 0 {
		length = defaultRandomLen
	}
	return p11.GenerateRandom(length)
}
