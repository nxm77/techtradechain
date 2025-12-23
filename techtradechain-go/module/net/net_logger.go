/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package net

import (
	"techtradechain.com/techtradechain/logger/v2"
	liquid "techtradechain.com/techtradechain/net-liquid/liquidnet"
	"techtradechain.com/techtradechain/protocol/v2"
)

var GlobalNetLogger protocol.Logger

func init() {
	GlobalNetLogger = logger.GetLogger(logger.MODULE_NET)
	liquid.InitLogger(GlobalNetLogger, func(chainId string) protocol.Logger {
		return logger.GetLoggerByChain(logger.MODULE_NET, chainId)
	})
}
