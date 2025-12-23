/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	"crypto"
)

// nolint
const (
	// 密码算法默认值，若是此项，将采用配置文件中配置的密码算法
	CRYPTO_ALGO_HASH_DEFAULT = ""
	CRYPTO_ALGO_SYM_DEFAULT  = ""
	CRYPTO_ALGO_ASYM_DEFAULT = ""

	// 哈希算法
	CRYPTO_ALGO_SHA256   = "SHA256"
	CRYPTO_ALGO_SHA3_256 = "SHA3_256"
	CRYPTO_ALGO_SM3      = "SM3"

	// 对称加密
	CRYPTO_ALGO_AES    = "AES"
	CRYPTO_ALGO_AES128 = "AES128"
	CRYPTO_ALGO_AES192 = "AES192"
	CRYPTO_ALGO_AES256 = "AES256"
	CRYPTO_ALGO_SM4    = "SM4"

	// 非对称秘钥
	CRYPTO_ALGO_RSA512        = "RSA512"
	CRYPTO_ALGO_RSA1024       = "RSA1024"
	CRYPTO_ALGO_RSA2048       = "RSA2048"
	CRYPTO_ALGO_RSA3072       = "RSA3072"
	CRYPTO_ALGO_SM2           = "SM2"
	CRYPTO_ALGO_ECC_P256      = "ECC_P256"
	CRYPTO_ALGO_ECC_P384      = "ECC_P384"
	CRYPTO_ALGO_ECC_P521      = "ECC_P521"
	CRYPTO_ALGO_ECC_Ed25519   = "ECC_Ed25519"
	CRYPTO_ALGO_ECC_Secp256k1 = "ECC_Secp256k1"

	//后量子密码(非对称）
	CRYPTO_ALGO_DILITHIUM2 = "DILITHIUM2"
)

// HashType hash type definition
type HashType uint

const (
	// HASH_TYPE_SM3 sm3 HashType
	HASH_TYPE_SM3 HashType = 20
	// HASH_TYPE_SHA256 sha256 HashType
	HASH_TYPE_SHA256 HashType = HashType(crypto.SHA256)
	// HASH_TYPE_SHA3_256 sha3_256 HashType
	HASH_TYPE_SHA3_256 HashType = HashType(crypto.SHA3_256)
)

const (
	// SM3 sm3 hash type definition
	SM3 = crypto.Hash(HASH_TYPE_SM3)
)

// CRYPTO_DEFAULT_UID constant UID for SM2-SM3
const CRYPTO_DEFAULT_UID = "1234567812345678"

// KeyType 秘钥类型
type KeyType int

// nolint
const (
	// 对称秘钥
	AES KeyType = iota
	SM4
	// 非对称秘钥
	RSA512
	RSA1024
	RSA2048
	RSA3072
	SM2
	ECC_Secp256k1
	ECC_NISTP256
	ECC_NISTP384
	ECC_NISTP521
	ECC_Ed25519

	//后量子密码密钥（非对称密钥）
	DILITHIUM2
)

// KeyType2NameMap all keyType:name map
var KeyType2NameMap = map[KeyType]string{
	AES:           CRYPTO_ALGO_AES,
	SM4:           CRYPTO_ALGO_SM4,
	RSA512:        CRYPTO_ALGO_RSA512,
	RSA1024:       CRYPTO_ALGO_RSA1024,
	RSA2048:       CRYPTO_ALGO_RSA2048,
	RSA3072:       CRYPTO_ALGO_RSA3072,
	SM2:           CRYPTO_ALGO_SM2,
	ECC_Secp256k1: CRYPTO_ALGO_ECC_Secp256k1,
	ECC_NISTP256:  "ECC_NISTP256",
	ECC_NISTP384:  "ECC_NISTP384",
	ECC_NISTP521:  "ECC_NISTP521",
	ECC_Ed25519:   CRYPTO_ALGO_ECC_Ed25519,

	DILITHIUM2: CRYPTO_ALGO_DILITHIUM2,
}

// Name2KeyTypeMap all name:keyType map
var Name2KeyTypeMap = map[string]KeyType{
	CRYPTO_ALGO_AES:           AES,
	CRYPTO_ALGO_SM4:           SM4,
	CRYPTO_ALGO_RSA512:        RSA512,
	CRYPTO_ALGO_RSA1024:       RSA1024,
	CRYPTO_ALGO_RSA2048:       RSA2048,
	CRYPTO_ALGO_RSA3072:       RSA3072,
	CRYPTO_ALGO_SM2:           SM2,
	CRYPTO_ALGO_ECC_Secp256k1: ECC_Secp256k1,
	"ECC_NISTP256":            ECC_NISTP256,
	"ECC_NISTP384":            ECC_NISTP384,
	"ECC_NISTP521":            ECC_NISTP521,
	CRYPTO_ALGO_ECC_Ed25519:   ECC_Ed25519,

	CRYPTO_ALGO_DILITHIUM2: DILITHIUM2,
}

// BitsSize bit size definition
type BitsSize int

// nolint
const (
	BITS_SIZE_128  BitsSize = 128
	BITS_SIZE_192  BitsSize = 192
	BITS_SIZE_256  BitsSize = 256
	BITS_SIZE_512  BitsSize = 512
	BITS_SIZE_1024 BitsSize = 1024
	BITS_SIZE_2048 BitsSize = 2048
	BITS_SIZE_3072 BitsSize = 3072
)

// HashAlgoMap all name:HashType map
var HashAlgoMap = map[string]HashType{
	CRYPTO_ALGO_SHA256:   HASH_TYPE_SHA256,
	CRYPTO_ALGO_SHA3_256: HASH_TYPE_SHA3_256,
	CRYPTO_ALGO_SM3:      HASH_TYPE_SM3,
}

// SymAlgoMap all symmetric algo name:KeyType map
var SymAlgoMap = map[string]KeyType{
	// 对称秘钥
	CRYPTO_ALGO_AES:    AES,
	CRYPTO_ALGO_AES128: AES,
	CRYPTO_ALGO_AES192: AES,
	CRYPTO_ALGO_AES256: AES,
	CRYPTO_ALGO_SM4:    SM4,
}

// AsymAlgoMap all asymmetric algo name:KeyType map
var AsymAlgoMap = map[string]KeyType{
	// 非对称秘钥
	CRYPTO_ALGO_RSA512:        RSA512,
	CRYPTO_ALGO_RSA1024:       RSA1024,
	CRYPTO_ALGO_RSA2048:       RSA2048,
	CRYPTO_ALGO_RSA3072:       RSA3072,
	CRYPTO_ALGO_SM2:           SM2,
	CRYPTO_ALGO_ECC_P256:      ECC_NISTP256,
	CRYPTO_ALGO_ECC_P384:      ECC_NISTP384,
	CRYPTO_ALGO_ECC_P521:      ECC_NISTP521,
	CRYPTO_ALGO_ECC_Ed25519:   ECC_Ed25519,
	CRYPTO_ALGO_ECC_Secp256k1: ECC_Secp256k1,

	CRYPTO_ALGO_DILITHIUM2: DILITHIUM2,
}

// SignOpts Signing options
type SignOpts struct {
	Hash         HashType
	UID          string
	EncodingType string
}

// EncOpts Encryption options
type EncOpts struct {
	EncodingType string
	BlockMode    string
	EnableMAC    bool
	Hash         HashType
	Label        []byte
	EnableASN1   bool
}

// Key key interface definition
type Key interface {
	// 获取秘钥字节数组
	Bytes() ([]byte, error)

	// 获取秘钥类型
	Type() KeyType

	// 获取编码后秘钥(PEM格式)
	String() (string, error)
}

// SymmetricKey symmetric encryt and decrypt interface definition
type SymmetricKey interface {
	Key

	// 加密接口
	Encrypt(plain []byte) ([]byte, error)
	EncryptWithOpts(plain []byte, opts *EncOpts) ([]byte, error)

	// 解密接口
	Decrypt(ciphertext []byte) ([]byte, error)
	DecryptWithOpts(ciphertext []byte, opts *EncOpts) ([]byte, error)
}

// PrivateKey private key signing interface definition
type PrivateKey interface {
	Key

	// 私钥签名
	Sign(data []byte) ([]byte, error)

	SignWithOpts(data []byte, opts *SignOpts) ([]byte, error)

	// 返回公钥
	PublicKey() PublicKey

	// 转换为crypto包中的 PrivateKey 接口类
	ToStandardKey() crypto.PrivateKey
}

// PublicKey public key verifying interface definition
type PublicKey interface {
	Key

	// 公钥验签
	Verify(data []byte, sig []byte) (bool, error)

	VerifyWithOpts(data []byte, sig []byte, opts *SignOpts) (bool, error)

	// 转换为crypto包中的 PublicKey 接口类
	ToStandardKey() crypto.PublicKey
}

// DecryptKey decrypt interface definition
type DecryptKey interface {
	Key

	Decrypt(ciphertext []byte) ([]byte, error)

	DecryptWithOpts(ciphertext []byte, opts *EncOpts) ([]byte, error)

	EncryptKey() EncryptKey
}

// EncryptKey encrypt interface definition
type EncryptKey interface {
	Key

	// Encrypt
	Encrypt(data []byte) ([]byte, error)

	EncryptWithOpts(data []byte, opts *EncOpts) ([]byte, error)
}

// Encryptor encrypt and decrypt interface definition
type Encryptor interface {
	Encrypt(data []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}
