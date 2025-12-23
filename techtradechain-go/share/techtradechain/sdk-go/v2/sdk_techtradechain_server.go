/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"context"
	"fmt"

	"techtradechain.com/techtradechain/pb-go/v2/config"
)

// GetTechTradeChainServerVersion get techtradechain version
func (cc *ChainClient) GetTechTradeChainServerVersion() (string, error) {
	cc.logger.Debug("[SDK] begin to get techtradechain server version")
	req := &config.TechTradeChainVersionRequest{}
	client, err := cc.pool.getClient()
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	res, err := client.rpcNode.GetTechTradeChainVersion(ctx, req)
	if err != nil {
		return "", err
	}
	if res.Code != 0 {
		return "", fmt.Errorf("get techtradechain server version failed, %s", res.Message)
	}
	return res.Version, nil
}

// GetTechTradeChainServerVersionCustom get techtradechain version
func (cc *ChainClient) GetTechTradeChainServerVersionCustom(ctx context.Context) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cc.logger.Debug("[SDK] begin to get techtradechain server version")
	req := &config.TechTradeChainVersionRequest{}
	client, err := cc.pool.getClient()
	if err != nil {
		return "", err
	}
	res, err := client.rpcNode.GetTechTradeChainVersion(ctx, req)
	if err != nil {
		return "", err
	}
	if res.Code != 0 {
		return "", fmt.Errorf("get techtradechain server version failed, %s", res.Message)
	}
	return res.Version, nil
}
