/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
)

func (cc *ChainClient) createPubkeyManagePayload(method string, kvs []*common.KeyValuePair) *common.Payload {
	cc.logger.Debugf("[SDK] create PubkeyManagePayload, method: %s", method)
	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_PUBKEY_MANAGE.String(),
		method, kvs, defaultSeq, nil)
	return payload
}

// CreatePubkeyAddPayload create pubkey add payload
func (cc *ChainClient) CreatePubkeyAddPayload(pubkey string, orgId string, role string) (*common.Payload, error) {

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyPubkey,
			Value: []byte(pubkey),
		},
		{
			Key:   utils.KeyPubkeyOrgId,
			Value: []byte(orgId),
		},
		{
			Key:   utils.KeyPubkeyRole,
			Value: []byte(role),
		},
	}

	return cc.createPubkeyManagePayload(syscontract.PubkeyManageFunction_PUBKEY_ADD.String(), pairs), nil
}

// CreatePubkeyDelPayload create pubkey delete payload
func (cc *ChainClient) CreatePubkeyDelPayload(pubkey string, orgId string) (*common.Payload, error) {
	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyPubkey,
			Value: []byte(pubkey),
		},
		{
			Key:   utils.KeyPubkeyOrgId,
			Value: []byte(orgId),
		},
	}

	return cc.createPubkeyManagePayload(syscontract.PubkeyManageFunction_PUBKEY_DELETE.String(), pairs), nil
}

// CreatePubkeyQueryPayload create pubkey query payload
func (cc *ChainClient) CreatePubkeyQueryPayload(pubkey string) (*common.Payload, error) {
	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyPubkey,
			Value: []byte(pubkey),
		},
	}

	return cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_PUBKEY_MANAGE.String(),
		syscontract.PubkeyManageFunction_PUBKEY_QUERY.String(), pairs, defaultSeq, nil), nil
}

// SendPubkeyManageRequest send pubkey manage request to node
func (cc *ChainClient) SendPubkeyManageRequest(payload *common.Payload, endorsers []*common.EndorsementEntry,
	timeout int64, withSyncResult bool) (*common.TxResponse, error) {
	return cc.proposalRequest(payload, endorsers, nil, timeout, withSyncResult)
}
