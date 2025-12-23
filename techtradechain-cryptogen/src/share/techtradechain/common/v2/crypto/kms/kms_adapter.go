/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kms

import bccrypto "techtradechain.com/techtradechain/common/v2/crypto"

// IKMSAdapter is kms adapter interface
type IKMSAdapter interface {
	// NewPrivateKey returns a kms PrivateKey
	NewPrivateKey(key PrivateKey) (bccrypto.PrivateKey, error)

	// NewPublicKey returns a kms PublicKey
	NewPublicKey(keyId string) (bccrypto.PublicKey, error)
}
