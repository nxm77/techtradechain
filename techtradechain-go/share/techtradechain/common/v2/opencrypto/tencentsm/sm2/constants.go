/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package sm2

import (
	"techtradechain.com/techtradechain/common/v2/opencrypto/tencentsm/tencentsm"
)

const BIG_NUM_SIZE = 64

const SM2_PRIVATE_KEY_SIZE = 64
const SM2_PUBLIC_KEY_SIZE = 130

const SM2_PRIVATE_KEY_STR_LEN = 66
const SM2_PUBLIC_KEY_STR_LEN = 132

const SM2_CIPHER_EXTRA_SIZE = 120

const SM2_CIPHER_MODE_C1C3C2_ASN1 = tencentsm.SM2CipherMode_C1C3C2_ASN1
const SM2_CIPHER_MODE_C1C2C3_ASN1 = tencentsm.SM2CipherMode_C1C2C3_ASN1
const SM2_CIPHER_MODE_C1C3C2 = tencentsm.SM2CipherMode_C1C3C2
const SM2_CIPHER_MODE_C1C2C3 = tencentsm.SM2CipherMode_C1C2C3

const SM2_SIGNATURE_MAX_SIZE = 80

const SM2_SIGNATURE_MODE_RS_ASN1 = tencentsm.SM2SignMode_RS_ASN1
const SM2_SIGNATURE_MODE_RS = tencentsm.SM2SignMode_RS

const SM3_DIGEST_SIZE = tencentsm.SM3_DIGEST_LENGTH
