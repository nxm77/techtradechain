/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"fmt"
	"time"

	bcx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
)

const (
	// DefaultRetryLimit 默认轮询交易结果最大次数
	DefaultRetryLimit = 10
	// DefaultRetryInterval 默认每次轮询交易结果时的等待时间，单位ms
	DefaultRetryInterval = 500
	// defaultSeq default sequence
	defaultSeq = 0
)

// GetSyncResult get sync result of tx
func (cc *ChainClient) GetSyncResult(txId string, timeout int64) (*common.Result, error) {
	var (
		r   *txResult
		err error
	)
	if cc.enableTxResultDispatcher {
		if timeout <= 0 {
			timeout = DefaultGetTxTimeout
		}
		r, err = cc.asyncTxResult(txId, timeout)
	} else {
		r, err = cc.pollingTxResult(txId)
	}
	if err != nil {
		return nil, err
	}
	return r.Result, nil
}

func (cc *ChainClient) pollingTxResult(txId string) (*txResult, error) {
	var (
		txInfo *common.TransactionInfo
		err    error
	)

	err = retry.Retry(func(uint) error {
		txInfo, err = cc.GetTxByTxId(txId)
		if err != nil {
			return err
		}

		return nil
	},
		strategy.Wait(time.Duration(cc.retryInterval)*time.Millisecond),
		strategy.Limit(uint(cc.retryLimit)),
	)

	if err != nil {
		return nil, fmt.Errorf("get tx by txId [%s] failed, %s", txId, err.Error())
	}

	if txInfo == nil || txInfo.Transaction == nil || txInfo.Transaction.Result == nil {
		return nil, fmt.Errorf("get result by txId [%s] failed, %+v", txId, txInfo)
	}

	return &txResult{
		Result:        txInfo.Transaction.Result,
		TxTimestamp:   txInfo.Transaction.Payload.Timestamp,
		TxBlockHeight: txInfo.BlockHeight,
	}, nil
}

func (cc *ChainClient) asyncTxResult(txId string, timeout int64) (*txResult, error) {
	txResultC := cc.txResultDispatcher.register(txId)
	defer cc.txResultDispatcher.unregister(txId)

	duration := time.Duration(timeout) * time.Second
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	select {
	case r := <-txResultC:
		return r, nil
	case <-ticker.C:
		return nil, fmt.Errorf("get transaction result timed out, timeout=%s", duration)
	}
}

// CreatePayload create unsigned payload
func (cc *ChainClient) CreatePayload(txId string, txType common.TxType, contractName, method string,
	kvs []*common.KeyValuePair, seq uint64, limit *common.Limit) *common.Payload {
	if txId == "" {
		if cc.enableNormalKey {
			txId = utils.GetRandTxId()
		} else {
			txId = utils.GetTimestampTxId()
		}
	}

	payload := utils.NewPayload(
		utils.WithChainId(cc.chainId),
		utils.WithTxType(txType),
		utils.WithTxId(txId),
		utils.WithTimestamp(time.Now().Unix()),
		utils.WithContractName(contractName),
		utils.WithMethod(method),
		utils.WithParameters(kvs),
		utils.WithSequence(seq),
		utils.WithLimit(limit),
	)

	return payload
}

// SignPayload sign payload, returns *common.EndorsementEntry
func (cc *ChainClient) SignPayload(payload *common.Payload) (*common.EndorsementEntry, error) {
	var (
		sender    *accesscontrol.Member
		signBytes []byte
		err       error
	)
	if cc.authType == PermissionedWithCert {

		hashalgo, err := bcx509.GetHashFromSignatureAlgorithm(cc.userCrt.SignatureAlgorithm)
		if err != nil {
			return nil, fmt.Errorf("invalid algorithm: %s", err.Error())
		}

		signBytes, err = utils.SignPayloadWithHashType(cc.privateKey, hashalgo, payload)
		if err != nil {
			return nil, fmt.Errorf("SignPayload failed, %s", err)
		}

		sender = &accesscontrol.Member{
			OrgId:      cc.orgId,
			MemberInfo: cc.userCrtBytes,
			MemberType: accesscontrol.MemberType_CERT,
		}

	} else {
		signBytes, err = utils.SignPayloadWithHashType(cc.privateKey, cc.hashType, payload)
		if err != nil {
			return nil, fmt.Errorf("SignPayload failed, %s", err.Error())
		}
		sender = &accesscontrol.Member{
			OrgId:      cc.orgId,
			MemberInfo: cc.pkBytes,
			MemberType: accesscontrol.MemberType_PUBLIC_KEY,
		}
	}

	entry := &common.EndorsementEntry{
		Signer:    sender,
		Signature: signBytes,
	}
	return entry, nil
}
