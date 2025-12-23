// nolint
package kms

type KeyType string

const (
	SM2      KeyType = "SM2"
	ECC_P256 KeyType = "ECC_P256"

	SM4     KeyType = "SM4"
	AES_128 KeyType = "AES_128"
	AES_192 KeyType = "AES_192"
	AES_256 KeyType = "AES_256"
)
