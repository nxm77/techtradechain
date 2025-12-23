/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kms

import (
	"os"
	"strings"

	"github.com/pkg/errors"

	kms "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/kms/v20190118"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/kms/tencentcloudkms"
)

// Adapter variable must be defined
// nolint
var Adapter defaultAdapter

type defaultAdapter struct {
	*Config
}

// NewDefaultAdapter return a default kms adapter
func NewDefaultAdapter(config *Config) (IKMSAdapter, error) {
	//XXX: should test connection profile
	return defaultAdapter{config}, nil
}

func (adapter defaultAdapter) NewPrivateKey(key PrivateKey) (bccrypto.PrivateKey, error) {
	cli, err := adapter.getClient()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get kms client")
	}
	keyConfig := &tencentcloudkms.KMSPrivateKeyConfig{
		KeyType:  key.KeyType,
		KeyId:    key.KeyId,
		KeyAlias: key.KeyAlias,
	}
	return tencentcloudkms.NewPrivateKey(cli, keyConfig, adapter.Config.IsPublic)
}

func (adapter defaultAdapter) NewPublicKey(keyId string) (bccrypto.PublicKey, error) {
	cli, err := adapter.getClient()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get kms client")
	}

	return tencentcloudkms.ExportPublicKeyFromKMS(adapter.Config.IsPublic, cli, keyId)
}

func (adapter defaultAdapter) getClient() (*kms.Client, error) {
	if adapter.Config == nil {
		adapter.Config = &Config{
			SecretId:  os.Getenv("KMS_SECRET_ID"),
			SecretKey: os.Getenv("KMS_SECRET_KEY"),
			Address:   os.Getenv("KMS_ADDRESS"),
			Region:    os.Getenv("KMS_REGION"),
			SDKScheme: os.Getenv("KMS_SDK_SCHEME"),
		}
		isPublicStr := os.Getenv("KMS_IS_PUBLIC")
		if strings.EqualFold(strings.ToLower(isPublicStr), "true") {
			adapter.Config.IsPublic = true
		} else {
			adapter.Config.IsPublic = false
		}
	}

	kmsConfig := &tencentcloudkms.KMSConfig{
		SecretId:      adapter.Config.SecretId,
		SecretKey:     adapter.Config.SecretKey,
		ServerAddress: adapter.Config.Address,
		ServerRegion:  adapter.Config.Region,
		KmsSDKScheme:  adapter.Config.SDKScheme,
	}

	return tencentcloudkms.CreateConnection(kmsConfig)
}
