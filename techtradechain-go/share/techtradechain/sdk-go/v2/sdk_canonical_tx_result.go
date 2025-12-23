/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"context"
	"errors"
	"fmt"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// queryCanonical 同步查询权威的公认的链上数据，即超过半数共识的链上数据
func (cc *ChainClient) queryCanonical(txRequest *common.TxRequest, timeout int64) (*common.TxResponse, error) {
	txRespC := make(chan *common.TxResponse, 1)
	defer close(txRespC)
	txResultCount := make(map[string]int)
	receiveCount := 0
	canonicalNum := (len(cc.canonicalTxFetcherPools) / 2) + 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for nodeAddr, pool := range cc.canonicalTxFetcherPools {
		cc.logger.Debugf("node [%s] start sendTxRequestWithConnPool", nodeAddr)
		go sendTxRequestWithConnPool(ctx, cc, pool, txRequest, timeout, txRespC)
	}

	ticker := time.NewTicker(time.Second * time.Duration(timeout))
	defer ticker.Stop()
	for {
		select {
		case r := <-txRespC:
			if r != nil {
				bz, err := proto.Marshal(r)
				if err != nil {
					return nil, err
				}
				sum, err := hash.Get(crypto.HASH_TYPE_SHA256, bz)
				if err != nil {
					return nil, err
				}
				sumStr := string(sum)
				if count, ok := txResultCount[sumStr]; ok {
					txResultCount[sumStr] = count + 1
				} else {
					txResultCount[sumStr] = 1
				}
				if txResultCount[sumStr] >= canonicalNum {
					return r, nil
				}
			}
			receiveCount++
			if receiveCount >= len(cc.canonicalTxFetcherPools) {
				return nil, errors.New("queryCanonical failed")
			}
		case <-ticker.C:
			return nil, fmt.Errorf("queryCanonical timed out, timeout=%ds", timeout)
		}
	}
}

func sendTxRequestWithConnPool(ctx context.Context, cc *ChainClient, pool ConnectionPool, txRequest *common.TxRequest,
	timeout int64, txRespC chan *common.TxResponse) {
	cc.logger.Debugf("[SDK] begin sendTxRequestWithConnPool, [method:%s]/[txId:%s]",
		txRequest.Payload.Method, txRequest.Payload.TxId)

	var (
		errMsg string
		logger = pool.getLogger()
	)

	ignoreAddrs := make(map[string]struct{})
	for {
		client, err := pool.getClientWithIgnoreAddrs(ignoreAddrs)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			txRespC <- nil
			return
		}

		if len(ignoreAddrs) > 0 {
			logger.Debugf("[SDK] begin try to connect node [%s]", client.ID)
		}

		resp, err := client.sendRequest(txRequest, timeout)
		if err != nil {
			resp := &common.TxResponse{
				Message: err.Error(),
				TxId:    txRequest.Payload.TxId,
			}

			statusErr, ok := status.FromError(err)
			if ok && (statusErr.Code() == codes.DeadlineExceeded ||
				// desc = "transport: Error while dialing dial tcp 127.0.0.1:12301: connect: connection refused"
				statusErr.Code() == codes.Unavailable) {

				resp.Code = common.TxStatusCode_TIMEOUT
				errMsg = fmt.Sprintf("call [%s] meet network error, try to connect another node if has, %s",
					client.ID, err.Error())

				logger.Errorf(sdkErrStringFormat, errMsg)
				ignoreAddrs[client.ID] = struct{}{}
				continue
			}

			logger.Errorf("statusErr.Code() : %s", statusErr.Code())

			resp.Code = common.TxStatusCode_INTERNAL_ERROR
			errMsg = fmt.Sprintf("client.call failed, %+v", err)
			logger.Errorf(sdkErrStringFormat, errMsg)
			select {
			case <-ctx.Done():
				return
			default:
			}
			txRespC <- resp
			return
		}

		resp.TxId = txRequest.Payload.TxId
		select {
		case <-ctx.Done():
			return
		default:
		}
		txRespC <- resp
		return
	}
}
