/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package techtradechain_sdk_go

import (
	"context"

	"techtradechain.com/techtradechain/pb-go/v2/sync"
)

// GetSyncState returns sync state of node
// withOthersState indicates whether to return state of other nodes known by this accessing node
func (cc *ChainClient) GetSyncState(withOthersState bool) (*sync.SyncState, error) {
	cc.logger.Debug("[SDK] begin GetSyncState")
	client, err := cc.pool.getClient()
	if err != nil {
		return nil, err
	}
	req := &sync.GetSyncStateRequest{
		ChainId:   cc.chainId,
		WithPeers: withOthersState,
	}
	resp, err := client.rpcNode.GetSyncState(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
