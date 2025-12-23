/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hsm

// IHSMAdapter is the hsm adapter interface
type IHSMAdapter interface {
	// for PKCS11
	PKCS11_GetSM2KeyId(keyIdex int, isPrivate bool) (string, error)
	PKCS11_GetRSAKeyId(keyIdex int, isPrivate bool) (string, error)
	PKCS11_GetECCKeyId(keyIdex int, isPrivate bool) (string, error)
	PKCS11_GetSM4KeyId(keyIdex int) (string, error)
	PKCS11_GetAESKeyId(keyIdex int) (string, error)

	PKCS11_GetSM3SM2CKM() uint

	// For SDF
	SDF_GetSM2KeyAccessRight(keyIdex int) (newKeyIdex int, need bool)
	SDF_GetSM4KeyAccessRight(keyIdex int) (newKeyIdex int, need bool)
	SDF_GetRSAKeyAccessRight(keyIdex int) (newKeyIdex int, need bool)
	SDF_GetAESKeyAccessRight(keyIdex int) (newKeyIdex int, need bool)
}
