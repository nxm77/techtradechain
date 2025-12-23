/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package tencentcloudkms

import (
	"fmt"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/common/v2/json"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	common2 "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/http"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	kms "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/kms/v20190118"
)

const (
	MODE_DIGEST = "DIGEST"
	MODE_RAW    = "RAW"

	KEY_TYPE_SM2_SIGNATURE = "ASYMMETRIC_SIGN_VERIFY_SM2"

	ALGORITHM_TYPE_SM2_SIGNATURE = "SM2DSA"
)

var keyTypeMap = map[string]string{
	bccrypto.CRYPTO_ALGO_SM2: KEY_TYPE_SM2_SIGNATURE,
}

var keyTypeList = map[string]string{
	KEY_TYPE_SM2_SIGNATURE: KEY_TYPE_SM2_SIGNATURE,
}

var algorithmTypeMap = map[string]string{
	bccrypto.CRYPTO_ALGO_SM2: ALGORITHM_TYPE_SM2_SIGNATURE,
}

var algorithmTypeList = map[string]string{
	ALGORITHM_TYPE_SM2_SIGNATURE: ALGORITHM_TYPE_SM2_SIGNATURE,
}

type KMSConfig struct {
	SecretId      string
	SecretKey     string
	ServerAddress string
	ServerRegion  string
	KmsSDKScheme  string
}

type KMSPrivateKeyConfig struct {
	KeyType  string `json:"key_type"`
	KeyId    string `json:"key_id"`
	KeyAlias string `json:"key_alias"`
}

func CreateConnection(kmsConfig *KMSConfig) (*kms.Client, error) {
	credential := common.NewCredential(
		kmsConfig.SecretId,
		kmsConfig.SecretKey,
	)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = kmsConfig.ServerAddress
	//NewClient默认scheme为https
	if kmsConfig.KmsSDKScheme == common2.HTTP {
		cpf.HttpProfile.Scheme = common2.HTTP
	}
	return kms.NewClient(credential, kmsConfig.ServerRegion, cpf)
}

func ExportPublicKeyFromKMS(isPublic bool, client *kms.Client, keyId string) (bccrypto.PublicKey, error) {
	var req *kms.GetPublicKeyRequest
	if isPublic {
		req = kms.NewGetPublicKeyRequest()
	} else {
		req = &kms.GetPublicKeyRequest{
			BaseRequest: &common2.BaseRequest{},
		}
		req.Init().WithApiInfo("cvm", kms.APIVersion, "GetPublicKey")
	}
	req.KeyId = common.StringPtr(keyId)
	response, err := client.GetPublicKey(req)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		return nil, fmt.Errorf("KMS API error: %s", err)
	}
	if err != nil {
		return nil, fmt.Errorf("KMS error: %v", err)
	}

	return asym.PublicKeyFromPEM([]byte(*(response.Response.PublicKeyPem)))
}

func GenerateKeyPairFromKMS(isPublic bool, client *kms.Client, keyAlias, keyType string) (bccrypto.PrivateKey, error) {
	keyTypeKMS, ok := keyTypeList[keyType]
	if !ok {
		keyTypeKMS, ok = keyTypeMap[keyType]
		if !ok {
			return nil, fmt.Errorf("KMS error: unsupported algorithm")
		}
	}

	algorithmTypeKMS, ok := algorithmTypeList[keyType]
	if !ok {
		algorithmTypeKMS, ok = algorithmTypeMap[keyType]
		if !ok {
			return nil, fmt.Errorf("KMS error: unsupported algorithm")
		}
	}

	request := kms.NewCreateKeyRequest()

	request.Alias = common.StringPtr(keyAlias)
	request.KeyUsage = common.StringPtr(keyTypeKMS)
	request.Type = common.Uint64Ptr(1)

	response, err := client.CreateKey(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		return nil, fmt.Errorf("KMS API error: %s", err)
	}
	if err != nil {
		return nil, fmt.Errorf("KMS error: %v", err)
	}

	keyId := *(response.Response.KeyId)
	pk, err := ExportPublicKeyFromKMS(isPublic, client, keyId)
	if err != nil {
		return nil, err
	}

	sk := &PrivateKey{
		kms:      client,
		keyType:  algorithmTypeKMS,
		keyId:    keyId,
		keyAlias: keyAlias,
		pubKey:   pk,
	}
	return sk, nil
}

func NewPrivateKey(client *kms.Client, keyConfig *KMSPrivateKeyConfig, isPublic bool) (bccrypto.PrivateKey, error) {
	keyTypeKMS, ok := algorithmTypeList[keyConfig.KeyType]
	if !ok {
		keyTypeKMS, ok = algorithmTypeMap[keyConfig.KeyType]
		if !ok {
			return nil, fmt.Errorf("KMS error: unsupported algorithm")
		}
	}

	pk, err := ExportPublicKeyFromKMS(isPublic, client, keyConfig.KeyId)
	if err != nil {
		return nil, err
	}

	sk := &PrivateKey{
		kms:      client,
		keyType:  keyTypeKMS,
		keyId:    keyConfig.KeyId,
		keyAlias: keyConfig.KeyAlias,
		pubKey:   pk,
		isPublic: isPublic,
	}
	return sk, nil
}

func LoadPrivateKey(client *kms.Client, skInfo []byte, public bool) (bccrypto.PrivateKey, error) {
	var skConfig KMSPrivateKeyConfig
	err := json.Unmarshal(skInfo, &skConfig)
	if err != nil {
		return nil, fmt.Errorf("KMS error: unmarshal private key failed, %v", err)
	}

	return NewPrivateKey(client, &skConfig, public)
}
