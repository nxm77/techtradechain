/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

const (
	KMS_KEY_TYPE_CERT = "cert"
	KMS_KEY_TYPE_PK   = "pk"
)

type userConfig struct {
	Type       string         `mapstructure:"type"`
	Count      int32          `mapstructure:"count"`
	Location   locationConfig `mapstructure:"location"`
	ExpireYear int32          `mapstructure:"expire_year"`
	KeyId      string         `mapstructure:"key_id"`
}

type nodeConfig struct {
	Type     string         `mapstructure:"type"`
	Count    int32          `mapstructure:"count"`
	Location locationConfig `mapstructure:"location"`
	Specs    specsConfig    `mapstructure:"specs"`
	KeyId    string         `mapstructure:"key_id"`
}

type specsConfig struct {
	ExpireYear int32    `mapstructure:"expire_year"`
	SANS       []string `mapstructure:"sans"`
}

type caConfig struct {
	Location locationConfig `mapstructure:"location"`
	Specs    specsConfig    `mapstructure:"specs"`
	KeyId    string         `mapstructure:"key_id"`
}

type itemConfig struct {
	Domain    string         `mapstructure:"domain"`
	HostName  string         `mapstructure:"host_name"`
	PKAlgo    string         `mapstructure:"pk_algo"`
	SKIHash   string         `mapstructure:"ski_hash"`
	TLSMode   int            `mapstructure:"tls_mode"`
	Specs     specsConfig    `mapstructure:"specs"`
	Location  locationConfig `mapstructure:"location"`
	Count     int32          `mapstructure:"count"`
	CA        caConfig       `mapstructure:"ca"`
	Node      []nodeConfig   `mapstructure:"node"`
	User      []userConfig   `mapstructure:"user"`
	P11Config pkcs11Config   `mapstructure:"pkcs11"`
	KMSConfig kmsConfig      `mapstructure:"kms"`
}

type locationConfig struct {
	Country  string `mapstructure:"country"`
	Locality string `mapstructure:"locality"`
	Province string `mapstructure:"province"`
}

type pkcs11Config struct {
	Enabled          bool   `mapstructure:"enabled"`
	Type             string `mapstructure:"type"`
	Library          string `mapstructure:"library"`
	Label            string `mapstructure:"label"`
	Password         string `mapstructure:"password"`
	SessionCacheSize int    `mapstructure:"session_cache_size"`
	Hash             string `mapstructure:"hash"`
}

type kmsConfig struct {
	Enabled   bool   `mapstructure:"enabled"`
	IsPublic  bool   `mapstructure:"is_public"`
	SecretId  string `mapstructure:"secret_id"`
	SecretKey string `mapstructure:"secret_key"`
	Address   string `mapstructure:"address"`
	Region    string `mapstructure:"region"`
	SdkScheme string `mapstructure:"sdk_scheme"`
	ExtParams string `mapstructure:"ext_params"`
}

type CryptoGenConfig struct {
	Item []itemConfig `mapstructure:"crypto_config"`
}
