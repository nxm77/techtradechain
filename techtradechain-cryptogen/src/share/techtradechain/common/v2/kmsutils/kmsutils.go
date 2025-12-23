/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kmsutils

import (
	"encoding/json"
	"sync"

	"techtradechain.com/techtradechain/common/v2/crypto/kms"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"github.com/pkg/errors"
)

// KMSContext is a kms context
var KMSContext *kmsContext

var kmsPrivateKeyError = "failed to get kms private key, "
var keyInfoTemplate = "keyId = %s, keyType = %s, keyAlias = %s"

// KMSConfig is kms config struct
type KMSConfig struct {
	Enable bool
	kms.Config
}

type kmsContext struct {
	KMSConfig

	keyId     string
	keyType   string
	keyAlias  string
	extParams map[string]string
}

var once sync.Once

// InitKMS is used to init kms context, run only once
// nolint
func InitKMS(config KMSConfig) *kmsContext {
	once.Do(func() {
		KMSContext = &kmsContext{
			KMSConfig: config,
		}
	})
	return KMSContext
}

func (k *kmsContext) WithPrivKeyId(keyId string) *kmsContext {
	k.keyId = keyId
	return k
}

func (k *kmsContext) WithPrivKeyType(keyType string) *kmsContext {
	k.keyType = keyType
	return k
}

func (k *kmsContext) WithPrivKeyAlias(keyAlias string) *kmsContext {
	k.keyAlias = keyAlias
	return k
}

func (k *kmsContext) WithPrivExtParams(extParams map[string]string) *kmsContext {
	k.extParams = extParams
	return k
}

type kmsKeySpec struct {
	KeyId    string `json:"key_id"`
	KeyType  string `json:"key_type"`
	KeyAlias string `json:"key_alias"`
	// nolint
	extParams map[string]string `json:"ext_params"`
}

// CreateKMSKey - create kms private key
func (k *kmsContext) CreateKMSKey() ([]byte, crypto.PrivateKey, error) {
	var (
		privKey crypto.PrivateKey
		err     error
	)

	privKey, err = kms.GetKMSAdapter(&k.KMSConfig.Config).NewPrivateKey(
		kms.PrivateKey{
			KeyId:    k.keyId,
			KeyType:  k.keyType,
			KeyAlias: k.keyAlias,
		})
	if err != nil {
		return nil, nil, errors.WithMessagef(err, kmsPrivateKeyError+
			keyInfoTemplate, k.keyId, k.keyType, k.keyAlias)
	}

	keySpec := &kmsKeySpec{
		KeyType:  k.keyType,
		KeyId:    k.keyId,
		KeyAlias: k.keyAlias,
	}
	keySpecJson, err := json.Marshal(keySpec)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to get key spec json")
	}

	return keySpecJson, privKey, nil
}

// ParseKMSPrivKey parse a kms private key
func ParseKMSPrivKey(keySpecJson []byte) (crypto.PrivateKey, error) {
	var keySpec kmsKeySpec
	if err := json.Unmarshal(keySpecJson, &keySpec); err != nil {
		return nil, errors.WithMessage(err, "failed to parse kms keySpec")
	}

	return kms.GetKMSAdapter(&KMSContext.KMSConfig.Config).NewPrivateKey(
		kms.PrivateKey{
			KeyId:    keySpec.KeyId,
			KeyType:  keySpec.KeyType,
			KeyAlias: keySpec.KeyAlias,
		})
}

// CreateKMSKeyWithContext - create kms private key with Context
func CreateKMSKeyWithContext(context *kmsContext, extParams map[string]string) ([]byte, crypto.PrivateKey, error) {
	var (
		privKey  crypto.PrivateKey
		err      error
		keyId    = context.keyId
		keyType  = context.keyType
		keyAlias = context.keyAlias
	)

	privKey, err = kms.GetKMSAdapter(&KMSContext.KMSConfig.Config).NewPrivateKey(
		kms.PrivateKey{
			KeyId:    keyId,
			KeyType:  keyType,
			KeyAlias: keyAlias,
		})
	if err != nil {
		return nil, nil, errors.WithMessagef(err, kmsPrivateKeyError+
			keyInfoTemplate, keyId, keyType, keyAlias)
	}

	keySpec := &kmsKeySpec{
		KeyType:  keyType,
		KeyId:    keyId,
		KeyAlias: keyAlias,
	}
	keySpecJson, err := json.Marshal(keySpec)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to get key spec json")
	}

	return keySpecJson, privKey, nil
}
