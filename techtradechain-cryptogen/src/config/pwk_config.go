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
	defaultPWKConfigPath = "../config/pwk_config_template.yml"
)

var pwkConfig *PWKConfig

func LoadPWKConfig(path string) {
	CMViper = viper.New()
	pwkConfig = &PWKConfig{}

	if err := pwkConfig.loadConfig(path); err != nil {
		log.Fatalf("load crypto config [%s] failed, %s", path, err.Error())
	}
}

func GetPWKConfig() *PWKConfig {
	return pwkConfig
}

func (c *PWKConfig) loadConfig(path string) error {
	if path == "" {
		path = defaultPWKConfigPath
	}

	CMViper.SetConfigFile(path)
	if err := CMViper.ReadInConfig(); err != nil {
		return err
	}

	if err := CMViper.Unmarshal(&pwkConfig); err != nil {
		return err
	}

	return nil
}
