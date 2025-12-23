/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

type PKConfig struct {
	Item []pkItemConfig `mapstructure:"pk_config"`
}

type pkItemConfig struct {
	PKAlgo    string         `mapstructure:"pk_algo"`
	HashAlgo  string         `mapstructure:"hash_algo"`
	Count     int32          `mapstructure:"count"`
	Admin     pkAdminConfig  `mapstructure:"admin"`
	Node      []pkNodeConfig `mapstructure:"node"`
	User      []pkUserConfig `mapstructure:"user"`
	KMSConfig kmsConfig      `mapstructure:"kms"`
}

type pkUserConfig struct {
	Count int32  `mapstructure:"count"`
	Type  string `mapstructure:"type"`
}

type pkNodeConfig struct {
	Count int32 `mapstructure:"count"`
}

type pkAdminConfig struct {
	Count int32 `mapstructure:"count"`
}
