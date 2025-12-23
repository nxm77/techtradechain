/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kms

import (
	"errors"
	"fmt"
	"os"
	"plugin"
	"sync"
)

var once sync.Once
var adapter IKMSAdapter

// GetKMSAdapter returns a kms adapter
func GetKMSAdapter(config *Config) IKMSAdapter {
	once.Do(func() {
		var err error
		if config == nil || len(config.SecretId) == 0 || len(config.SecretKey) == 0 {
			// 配置文件里缺失则从环境变量读取
			adapter, err = LoadFromEnv()
		} else {
			adapter, err = NewDefaultAdapter(config)
		}
		if err != nil {
			panic(err)
		}
	})
	return adapter
}

// LoadFromEnv load a kms adapter which set by env
func LoadFromEnv() (IKMSAdapter, error) {
	pluginPath := os.Getenv("KMS_ADAPTER_LIB")
	if len(pluginPath) == 0 {
		return nil, errors.New("KMS_ADAPTER_LIB environment is not set")
	}
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, err
	}
	s, err := p.Lookup("Adapter")
	if err != nil {
		return nil, err
	}
	adapterFunc, ok := s.(func() (IKMSAdapter, error))
	if !ok {
		return nil, fmt.Errorf("failed to assert IKMSAdapter, KMS_ADAPTER_LIB = %s", pluginPath)
	}
	adapterStruct, err := adapterFunc()
	if err == nil {
		fmt.Println("new kms plugin success")
	}
	return adapterStruct, err
}
