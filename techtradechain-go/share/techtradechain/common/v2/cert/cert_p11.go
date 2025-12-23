/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cert

import (
	"encoding/json"
	"sync"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/pkcs11"
	"techtradechain.com/techtradechain/common/v2/crypto/sdf"
	"github.com/pkg/errors"
)

var once sync.Once

// P11Context pkcs11的上下文
var P11Context *pkcs11Context

type pkcs11Context struct {
	handle interface{}
	enable bool

	keyId   string
	keyType crypto.KeyType
	keyPwd  string
}

// InitP11Handle 初始化PKCS11 的Handle
// @param handle
func InitP11Handle(handle interface{}) {
	once.Do(func() {
		if P11Context == nil {
			P11Context = &pkcs11Context{
				handle: handle,
				enable: true,
			}
		}
	})
}

func (p *pkcs11Context) WithPrivKeyId(keyId string) *pkcs11Context {
	p.keyId = keyId
	return p
}

func (p *pkcs11Context) WithPrivKeyType(keyType crypto.KeyType) *pkcs11Context {
	p.keyType = keyType
	return p
}

func (p *pkcs11Context) WithPrivKeyPwd(keyPwd string) *pkcs11Context {
	p.keyPwd = keyPwd
	return p
}

type pkcs11KeySpec struct {
	KeyId   string `json:"key_id"`
	KeyType string `json:"key_type"`
	KeyPwd  string `json:"key_pwd"`
}

// CreateP11Key - create pkcs11 private key
func CreateP11Key(handle interface{}, keyType crypto.KeyType, keyId, keyPwd string) ([]byte, crypto.PrivateKey, error) {
	var privKey crypto.PrivateKey
	var err error
	switch h := handle.(type) {
	case *pkcs11.P11Handle:
		privKey, err = pkcs11.NewPrivateKey(h, keyId, keyType)
	case *sdf.SDFHandle:
		privKey, err = sdf.NewPrivateKey(h, keyId, []byte(keyPwd), keyType)
	default:
		err = errors.New("handle type is not supported, must be SDFHandle or P11Handle")
	}
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to construct hsm private key")
	}

	keySpec := &pkcs11KeySpec{
		KeyType: crypto.KeyType2NameMap[keyType],
		KeyId:   keyId,
		KeyPwd:  keyPwd,
	}
	keySpecJson, err := json.Marshal(keySpec)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to get key spec json")
	}

	return keySpecJson, privKey, nil
}

// ParseP11PrivKey 解析PKCS11出私钥对象
// @param handle
// @param keySpecJson
// @return crypto.PrivateKey
// @return error
func ParseP11PrivKey(handle interface{}, keySpecJson []byte) (crypto.PrivateKey, error) {
	var keySpec pkcs11KeySpec
	if err := json.Unmarshal(keySpecJson, &keySpec); err != nil {
		return nil, errors.WithMessage(err, "failed to parse pkcs11 keySpec")
	}

	switch h := handle.(type) {
	case *pkcs11.P11Handle:
		return pkcs11.NewPrivateKey(h, keySpec.KeyId, crypto.Name2KeyTypeMap[keySpec.KeyType])
	case *sdf.SDFHandle:
		return sdf.NewPrivateKey(h, keySpec.KeyId, []byte(keySpec.KeyPwd), crypto.Name2KeyTypeMap[keySpec.KeyType])
	}
	return nil, errors.New("handle type is not supported, must be SDFHandle or P11Handle")
}
