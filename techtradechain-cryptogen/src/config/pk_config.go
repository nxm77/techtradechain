/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"log"

	"github.com/spf13/viper"
)

const (
	defaultPKConfigPath = "../config/pk_config_template.yml"
)

var pkConfig *PKConfig

func LoadPKConfig(path string) {
	CMViper = viper.New()
	pkConfig = &PKConfig{}

	if err := pkConfig.loadConfig(path); err != nil {
		log.Fatalf("load crypto config [%s] failed, %s", path, err.Error())
	}
}

func GetPKConfig() *PKConfig {
	return pkConfig
}

func (c *PKConfig) loadConfig(path string) error {
	if path == "" {
		path = defaultPKConfigPath
	}

	CMViper.SetConfigFile(path)
	if err := CMViper.ReadInConfig(); err != nil {
		return err
	}

	if err := CMViper.Unmarshal(&pkConfig); err != nil {
		return err
	}

	return nil
}
