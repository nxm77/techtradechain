/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package syncmode

import (
	"techtradechain.com/techtradechain-go/module/core/provider"
	"techtradechain.com/techtradechain-go/module/core/provider/conf"
	"techtradechain.com/techtradechain/protocol/v2"
)

const ConsensusTypeDPOS = "DPOS"

var NilDPOSProvider provider.CoreProvider = (*dposProvider)(nil)

type dposProvider struct {
}

func (tp *dposProvider) NewCoreEngine(config *conf.CoreEngineConfig) (protocol.CoreEngine, error) {
	return NewCoreEngine(config)
}
