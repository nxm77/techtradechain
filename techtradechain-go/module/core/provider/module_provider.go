/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package provider

import (
	"techtradechain.com/techtradechain-go/module/core/provider/conf"
	"techtradechain.com/techtradechain/protocol/v2"
)

type CoreProvider interface {
	NewCoreEngine(config *conf.CoreEngineConfig) (protocol.CoreEngine, error)
}
