/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"techtradechain.com/techtradechain/common/v2/kmsutils"
	"fmt"
	"github.com/spf13/viper"
	"os"
	"strings"

	"techtradechain.com/techtradechain/common/v2/crypto/kms"

	"techtradechain.com/techtradechain/common/v2/cert"
	"techtradechain.com/techtradechain/common/v2/crypto"
	"github.com/pkg/errors"

	"techtradechain.com/techtradechain/common/v2/crypto/pkcs11"
	"techtradechain.com/techtradechain/common/v2/crypto/sdf"
)

const (
	PKCS11 = "pkcs11"
	SDF    = "sdf"
)

type HSMKeysConfig struct {
	HSMKeysMap map[string]OrgKeys `mapstructure:"hsm_keys"`
}

type OrgKeys struct {
	CA       []string `mapstructure:"ca"`
	UserKeys UserKeys `mapstructure:"user"`
	NodeKeys NodeKeys `mapstructure:"node"`
}

type NodeKeys struct {
	Consensus []string `mapstructure:"consensus"`
	Common    []string `mapstructure:"common"`
}
type UserKeys struct {
	Admin  []string `mapstructure:"admin"`
	Client []string `mapstructure:"client"`
	Light  []string `mapstructure:"light"`
}

type PKKeys struct {
	UserKeys UserKeys `mapstructure:"user"`
	NodeKeys NodeKeys `mapstructure:"node"`
}

type PKUserKeys struct {
	AdminKeys []string `mapstructure:"admin"`
	Client []string `mapstructure:"client"`
}

type NodeKey struct {
	Consensus []string `mapstructure:"consensus"`
	UserKeys PKUserKeys `mapstructure:"user"`
}

type PKHSMKeysConfig struct {
	Nodes     map[string]NodeKey  `mapstructure:"hsm_keys"`
}

var hsmKeysConfig *HSMKeysConfig
var hsmKeysPkConfig *PKHSMKeysConfig

const (
	defaultHSMKeysPath = "../config/hsm_keys.yml"
	defaultPKHSMKeysPath = "../config/hsm_keys_pk.yml"
)

// LoadHSMKeysConfig load local hsm keys for hsm or kms if enable and set
func LoadHSMKeysConfig(path, types string) error {
	switch types {
	case KMS_KEY_TYPE_CERT, "":
		if !PKCS11Enabled() && !KMSEnabled() {
			return nil
		}

		if path == "" {
			path = defaultHSMKeysPath
		}

		hsmKeysConfig = &HSMKeysConfig{}
		viper.SetConfigFile(path)
		if err := viper.ReadInConfig(); err != nil {
			return err
		}
		if err := viper.Unmarshal(&hsmKeysConfig); err != nil {
			return err
		}

		//set global config
		if PKCS11Enabled() {
			return initCertPKCS11()
		} else if KMSEnabled() {
			return initKMS()
		}
	case KMS_KEY_TYPE_PK:
		if path == "" {
			path = defaultPKHSMKeysPath
		}

		hsmKeysPkConfig = &PKHSMKeysConfig{}
		viper.SetConfigFile(path)
		if err := viper.ReadInConfig(); err != nil {
			panic(err)
			return err
		}
		if err := viper.Unmarshal(&hsmKeysPkConfig); err != nil {
			panic(err)
			return err
		}

		//set global config
		if PKCS11Enabled() {
			return initCertPKCS11()
		} else if PKKMSEnabled() {
			return initPKKMS()
		}
	default:
		panic(fmt.Sprintf("wrong keys type:%s", types))
	}

	return nil
}

func initCertPKCS11() error {
	if cryptoGenConfig == nil || len(cryptoGenConfig.Item) <= 0 {
		return errors.New("cryptoGenConfig not initialized")
	}
	p11Config := cryptoGenConfig.Item[0].P11Config
	var handle interface{}
	var err error
	// 暂时不考虑其他情况，如果有新增的时候需要考虑
	if p11Config.Enabled {
		if p11Config.Type == PKCS11 {
			handle, err = pkcs11.New(p11Config.Library, p11Config.Label, p11Config.Password, p11Config.SessionCacheSize, p11Config.Hash)
			if err != nil {
				return errors.WithMessage(err, "failed to initialize pkcs11 handle")
			}
		} else if p11Config.Type == SDF {
			handle, err = sdf.New(p11Config.Library, p11Config.SessionCacheSize)
			if err != nil {
				return errors.WithMessage(err, "failed to initialize sdf handle")
			}
		}
	}
	cert.InitP11Handle(handle)
	return nil
}

func initKMS() error {
	config := cryptoGenConfig.Item[0].KMSConfig
	if !config.Enabled {
		kmsEnable := os.Getenv("KMS_ENABLE")
		if strings.EqualFold(strings.ToLower(kmsEnable), "true") {
			isPublicStr := os.Getenv("KMS_IS_PUBLIC")
			if strings.EqualFold(strings.ToLower(isPublicStr), "true") {
				config.Enabled = true
			}
			config.SecretId = os.Getenv("KMS_SECRET_ID")
			config.SecretKey = os.Getenv("KMS_SECRET_KEY")
			config.Address = os.Getenv("KMS_ADDRESS")
			config.Region = os.Getenv("KMS_REGION")
			config.SdkScheme = os.Getenv("KMS_SDK_SCHEME")
		}
	}
	if !config.Enabled {
		return nil
	}
	kmsutils.InitKMS(kmsutils.KMSConfig{
		Enable: config.Enabled,
		Config: kms.Config{
			IsPublic:  config.IsPublic,
			SecretId:  config.SecretId,
			SecretKey: config.SecretKey,
			Address:   config.Address,
			Region:    config.Region,
			SDKScheme: config.SdkScheme,
		},
	})
	return nil
}

func initPKKMS() error {
	config := pkConfig.Item[0].KMSConfig
	if !config.Enabled {
		kmsEnable := os.Getenv("KMS_ENABLE")
		if strings.EqualFold(strings.ToLower(kmsEnable), "true") {
			isPublicStr := os.Getenv("KMS_IS_PUBLIC")
			if strings.EqualFold(strings.ToLower(isPublicStr), "true") {
				config.Enabled = true
			}
			config.SecretId = os.Getenv("KMS_SECRET_ID")
			config.SecretKey = os.Getenv("KMS_SECRET_KEY")
			config.Address = os.Getenv("KMS_ADDRESS")
			config.Region = os.Getenv("KMS_REGION")
			config.SdkScheme = os.Getenv("KMS_SDK_SCHEME")
		}
	}
	if !config.Enabled {
		return nil
	}
	kmsutils.InitKMS(kmsutils.KMSConfig{
		Enable: config.Enabled,
		Config: kms.Config{
			IsPublic:  config.IsPublic,
			SecretId:  config.SecretId,
			SecretKey: config.SecretKey,
			Address:   config.Address,
			Region:    config.Region,
			SDKScheme: config.SdkScheme,
		},
	})
	return nil
}

// SetPrivKeyContextWithPK distribute hsm keys for specific usage
func SetPrivKeyContextWithPK(keyType crypto.KeyType, nodes string, j int, usage string) error {
	if !PKCS11Enabled() && !PKKMSEnabled() {
		return nil
	}

	if _, exist := hsmKeysPkConfig.Nodes[nodes]; !exist {
		return fmt.Errorf("hsm org keys not set, nodes = %s", nodes)
	}
	var keyLabel string
	switch usage {
	case "admin":
		if j >= len(hsmKeysPkConfig.Nodes[nodes].UserKeys.AdminKeys) {
			return fmt.Errorf("hsm key is not set, nodes = %s, adminId = %d", nodes, j)
		}
		keyLabel = hsmKeysPkConfig.Nodes[nodes].UserKeys.AdminKeys[j]
	case "client":
		if j >= len(hsmKeysPkConfig.Nodes[nodes].UserKeys.Client) {
			return fmt.Errorf("hsm key is not set, nodes = %s, clientId = %d", nodes, j)
		}
		keyLabel = hsmKeysPkConfig.Nodes[nodes].UserKeys.Client[j]
	case "consensus":
		if j >= len(hsmKeysPkConfig.Nodes[nodes].Consensus) {
			return fmt.Errorf("hsm key is not set, nodes = %s, consensusId = %d", nodes, j)
		}
		keyLabel = hsmKeysPkConfig.Nodes[nodes].Consensus[j]
	default:
		return fmt.Errorf("hsm key is not set, nodes = %s, id = %d, usage = %s", nodes, j, usage)
	}

	if PKKMSEnabled() {
		keyId, kType, keyAlias, extParam := getKMSKeyIdPwd(keyLabel, keyType)
		kmsutils.KMSContext.WithPrivKeyId(keyId).WithPrivKeyType(kType).WithPrivKeyAlias(keyAlias).WithPrivExtParams(extParam)
	}
	return nil
}

// SetPrivKeyContext distribute hsm keys for specific usage
func SetPrivKeyContext(keyType crypto.KeyType, orgName string, j int, usage string) error {
	if !PKCS11Enabled() && !KMSEnabled() {
		return nil
	}

	if _, exist := hsmKeysConfig.HSMKeysMap[orgName]; !exist {
		return fmt.Errorf("hsm org keys not set, orgName = %s", orgName)
	}
	var keyLabel string
	switch usage {
	case "ca":
		if j >= len(hsmKeysConfig.HSMKeysMap[orgName].CA) {
			return fmt.Errorf("hsm key is not set, orgName = %s, caId = %d", orgName, j)
		}
		keyLabel = hsmKeysConfig.HSMKeysMap[orgName].CA[j]
	case "admin":
		if j >= len(hsmKeysConfig.HSMKeysMap[orgName].UserKeys.Admin) {
			return fmt.Errorf("hsm key is not set, orgName = %s, adminId = %d", orgName, j)
		}
		keyLabel = hsmKeysConfig.HSMKeysMap[orgName].UserKeys.Admin[j]
	case "light":
		if j >= len(hsmKeysConfig.HSMKeysMap[orgName].UserKeys.Light) {
			return fmt.Errorf("hsm key is not set, orgName = %s, lightId = %d", orgName, j)
		}
		keyLabel = hsmKeysConfig.HSMKeysMap[orgName].UserKeys.Light[j]
	case "client":
		if j >= len(hsmKeysConfig.HSMKeysMap[orgName].UserKeys.Client) {
			return fmt.Errorf("hsm key is not set, orgName = %s, clientId = %d", orgName, j)
		}
		keyLabel = hsmKeysConfig.HSMKeysMap[orgName].UserKeys.Client[j]
	case "consensus":
		if j >= len(hsmKeysConfig.HSMKeysMap[orgName].NodeKeys.Consensus) {
			return fmt.Errorf("hsm key is not set, orgName = %s, consensusId = %d", orgName, j)
		}
		keyLabel = hsmKeysConfig.HSMKeysMap[orgName].NodeKeys.Consensus[j]
	case "common":
		if j >= len(hsmKeysConfig.HSMKeysMap[orgName].NodeKeys.Common) {
			return fmt.Errorf("hsm key is not set, orgName = %s, commonId = %d", orgName, j)
		}
		keyLabel = hsmKeysConfig.HSMKeysMap[orgName].NodeKeys.Common[j]
	default:
		return fmt.Errorf("hsm key is not set, orgName = %s, id = %d, usage = %s", orgName, j, usage)
	}
	if PKCS11Enabled() {
		keyId, keyPwd := getHSMKeyInfo(keyLabel)
		cert.P11Context.WithPrivKeyId(keyId).WithPrivKeyPwd(keyPwd).WithPrivKeyType(keyType)
	} else if KMSEnabled() {
		keyId, kType, keyAlias, extParam := getKMSKeyIdPwd(keyLabel, keyType)
		kmsutils.KMSContext.WithPrivKeyId(keyId).WithPrivKeyType(kType).WithPrivKeyAlias(keyAlias).WithPrivExtParams(extParam)
	}
	return nil
}

// getHSMKeyInfo returns keyId, keyType, keyAlias
func getKMSKeyIdPwd(label string, keyType crypto.KeyType) (keyId, kType, keyAlias string, extParams map[string]string) {
	/*
		//compare global keyType with local keyType from hsm_key file?
			e.g. tencentcloudkms sm2 key type: SM2DSA
		     techtradechain sm2 key type:  SM2
	*/
	//_ = keyType
	key := strings.Split(label, ",")
	if len(key) <= 1 {
		panic("for hsm key, keyId and keyType must be set")
	}
	if len(key) == 2 {
		return strings.TrimSpace(key[0]), strings.TrimSpace(key[1]), "", nil
	}
	return strings.TrimSpace(key[0]), strings.TrimSpace(key[1]), strings.TrimSpace(key[2]), nil
}

// getHSMKeyInfo returns keyId and keyPwd for hsm, if hsm type is pkcs11, only returns keyId
func getHSMKeyInfo(label string) (keyId, keyPwd string) {
	key := strings.Split(label, ",")
	if len(key) <= 0 {
		panic("for hsm key, keyId must be set")
	}
	// this is a pkcs11 key
	if len(key) == 1 {
		return strings.TrimSpace(key[0]), ""
	}
	//this is a sdf key
	return strings.TrimSpace(key[0]), strings.TrimSpace(key[1])
}
