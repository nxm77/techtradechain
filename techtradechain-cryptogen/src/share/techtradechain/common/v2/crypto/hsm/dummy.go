/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hsm

import "fmt"

type dummyAdapter struct {
}

func (i dummyAdapter) PKCS11_GetSM2KeyId(keyIdex int, isPrivate bool) (string, error) {
	return fmt.Sprintf("%d", keyIdex), nil
}

func (i dummyAdapter) PKCS11_GetRSAKeyId(keyIdex int, isPrivate bool) (string, error) {
	return fmt.Sprintf("%d", keyIdex), nil
}

func (i dummyAdapter) PKCS11_GetECCKeyId(keyIdex int, isPrivate bool) (string, error) {
	return fmt.Sprintf("%d", keyIdex), nil
}

func (i dummyAdapter) PKCS11_GetSM4KeyId(keyIdex int) (string, error) {
	return fmt.Sprintf("%d", keyIdex), nil
}

func (i dummyAdapter) PKCS11_GetAESKeyId(keyIdex int) (string, error) {
	return fmt.Sprintf("%d", keyIdex), nil
}

func (i dummyAdapter) PKCS11_GetSM3SM2CKM() uint {
	return 0
}

func (i dummyAdapter) SDF_GetSM2KeyAccessRight(keyIdex int) (newKeyIdex int, need bool) {
	return keyIdex, false
}

func (i dummyAdapter) SDF_GetSM4KeyAccessRight(keyIdex int) (newKeyIdex int, need bool) {
	return keyIdex, false
}

func (i dummyAdapter) SDF_GetRSAKeyAccessRight(keyIdex int) (newKeyIdex int, need bool) {
	return keyIdex, false
}

func (i dummyAdapter) SDF_GetAESKeyAccessRight(keyIdex int) (newKeyIdex int, need bool) {
	return keyIdex, false
}
