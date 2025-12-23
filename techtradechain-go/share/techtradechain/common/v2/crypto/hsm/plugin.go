/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hsm

import (
	"log"
	"os"
	"plugin"
	"sync"

	"github.com/pkg/errors"
)

var once sync.Once
var adapter IHSMAdapter

// GetHSMAdapter returns the actual hsm adapter by pluginPath
// if puglinPath not set, returns a default adapter
func GetHSMAdapter(pluginPath string) IHSMAdapter {
	once.Do(func() {
		var err error
		if adapter, err = Load(pluginPath); err != nil {
			log.Println("load hsm adapter plugin failed, use default dummyAdapter")
			adapter = dummyAdapter{}
		}
	})
	return adapter
}

// Load loads the hsm adapter from a plugin specified by HSM_ADAPTER_LIB path
func Load(pluginPath string) (IHSMAdapter, error) {
	if len(pluginPath) == 0 {
		pluginPathEnv := os.Getenv("HSM_ADAPTER_LIB")
		log.Printf("invalid pluginPath params[%s], use HSM_ADAPTER_LIB[%s] from env", pluginPath, pluginPathEnv)
		pluginPath = pluginPathEnv
	}
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to open plugin lib = %s", pluginPath)
	}
	s, err := p.Lookup("Adapter")
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to lookup hsm Adapter")
	}

	adapter, ok := s.(IHSMAdapter)
	if !ok {
		return nil, errors.New("Adapter obj not implement IHSMAdapter interface")
	}

	return adapter, nil
}
