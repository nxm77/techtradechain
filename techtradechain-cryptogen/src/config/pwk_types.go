/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

type PWKConfig struct {
	Item []pwkItemConfig `mapstructure:"pwk_config"`
}

type pwkItemConfig struct {
	Domain   string          `mapstructure:"domain"`
	HostName string          `mapstructure:"host_name"`
	PKAlgo   string          `mapstructure:"pk_algo"`
	HashAlgo string          `mapstructure:"hash_algo"`
	Location locationConfig  `mapstructure:"location"`
	Count    int32           `mapstructure:"count"`
	Admin    pwkAdminConfig  `mapstructure:"admin"`
	Node     []pwkNodeConfig `mapstructure:"node"`
	User     []pwkUserConfig `mapstructure:"user"`
}

type pwkUserConfig struct {
	Type  string `mapstructure:"type"`
	Count int32  `mapstructure:"count"`
}

type pwkNodeConfig struct {
	Type  string `mapstructure:"type"`
	Count int32  `mapstructure:"count"`
}

type pwkAdminConfig struct {
}
