/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"fmt"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
)

// MultiSignContractReq send online multi sign contract request to node
func (cc *ChainClient) MultiSignContractReq(payload *common.Payload, endorsers []*common.EndorsementEntry,
	timeout int64, withSyncResult bool) (*common.TxResponse, error) {
	return cc.MultiSignContractReqWithPayer(payload, endorsers, nil, timeout, withSyncResult)
}

// MultiSignContractReqWithPayer send online multi sign contract request to node
func (cc *ChainClient) MultiSignContractReqWithPayer(payload *common.Payload, endorsers []*common.EndorsementEntry,
	payer *common.EndorsementEntry, timeout int64, withSyncResult bool) (*common.TxResponse, error) {
	return cc.proposalRequest(payload, endorsers, payer, timeout, withSyncResult)
}

// MultiSignContractVote send online multi sign vote request to node
func (cc *ChainClient) MultiSignContractVote(multiSignReqPayload *common.Payload,
	endorser *common.EndorsementEntry, isAgree bool, timeout int64, withSyncResult bool) (*common.TxResponse, error) {
	agree := syscontract.VoteStatus_AGREE
	if !isAgree {
		agree = syscontract.VoteStatus_REJECT
	}
	msvi := &syscontract.MultiSignVoteInfo{
		Vote:        agree,
		Endorsement: endorser,
	}
	msviByte, _ := msvi.Marshal()
	pairs := []*common.KeyValuePair{
		{
			Key:   syscontract.MultiVote_VOTE_INFO.String(),
			Value: msviByte,
		},
		{
			Key:   syscontract.MultiVote_TX_ID.String(),
			Value: []byte(multiSignReqPayload.TxId),
		},
	}
	payload := cc.createMultiSignVotePayload(pairs, nil)

	return cc.proposalRequest(payload, nil, nil, timeout, withSyncResult)
}

// MultiSignContractVoteWithGasLimit send online multi sign vote request to node
func (cc *ChainClient) MultiSignContractVoteWithGasLimit(multiSignReqPayload *common.Payload,
	endorser *common.EndorsementEntry, isAgree bool, timeout int64,
	gasLimit uint64, withSyncResult bool) (*common.TxResponse, error) {
	return cc.MultiSignContractVoteWithGasLimitAndPayer(multiSignReqPayload, endorser, nil,
		isAgree, timeout, gasLimit, withSyncResult)
}

// MultiSignContractVoteWithGasLimitAndPayer send online multi sign vote request to node
func (cc *ChainClient) MultiSignContractVoteWithGasLimitAndPayer(multiSignReqPayload *common.Payload,
	endorser *common.EndorsementEntry, payer *common.EndorsementEntry, isAgree bool, timeout int64,
	gasLimit uint64, withSyncResult bool) (*common.TxResponse, error) {
	agree := syscontract.VoteStatus_AGREE
	if !isAgree {
		agree = syscontract.VoteStatus_REJECT
	}
	msvi := &syscontract.MultiSignVoteInfo{
		Vote:        agree,
		Endorsement: endorser,
	}
	msviByte, _ := msvi.Marshal()
	pairs := []*common.KeyValuePair{
		{
			Key:   syscontract.MultiVote_VOTE_INFO.String(),
			Value: msviByte,
		},
		{
			Key:   syscontract.MultiVote_TX_ID.String(),
			Value: []byte(multiSignReqPayload.TxId),
		},
	}

	var limit *common.Limit
	if gasLimit > 0 {
		limit = &common.Limit{
			GasLimit: gasLimit,
		}
	}
	payload := cc.createMultiSignVotePayload(pairs, limit)

	return cc.proposalRequest(payload, nil, payer, timeout, withSyncResult)
}

// MultiSignContractTrig send online multi sign trig request to node
// this function is plugin after v2.3.1
func (cc *ChainClient) MultiSignContractTrig(multiSignReqPayload *common.Payload,
	timeout int64, limit *common.Limit, withSyncResult bool) (*common.TxResponse, error) {
	return cc.MultiSignContractTrigWithPayer(multiSignReqPayload, nil, timeout, limit, withSyncResult)
}

// MultiSignContractTrigWithPayer send online multi sign trig request to node
// this function is plugin after v2.3.1
func (cc *ChainClient) MultiSignContractTrigWithPayer(multiSignReqPayload *common.Payload,
	payer *common.EndorsementEntry, timeout int64, limit *common.Limit,
	withSyncResult bool) (*common.TxResponse, error) {

	pairs := []*common.KeyValuePair{
		{
			Key:   syscontract.MultiVote_TX_ID.String(),
			Value: []byte(multiSignReqPayload.TxId),
		},
	}
	payload := cc.createMultiSignTrigPayload(pairs, limit)

	return cc.proposalRequest(payload, nil, payer, timeout, withSyncResult)
}

// MultiSignContractQuery query online multi sign
func (cc *ChainClient) MultiSignContractQuery(txId string) (*common.TxResponse, error) {

	pairs := []*common.KeyValuePair{
		{
			Key:   syscontract.MultiVote_TX_ID.String(),
			Value: []byte(txId),
		},
	}
	payload := cc.createMultiSignQueryPayload(pairs)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return resp, fmt.Errorf(errStringFormat, payload.TxType.String(), err.Error())
	}

	if err = utils.CheckProposalRequestResp(resp, false); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType.String(), err.Error())
	}

	return resp, nil
}

// MultiSignContractQueryWithParams query online multi sign
func (cc *ChainClient) MultiSignContractQueryWithParams(
	txId string, params []*common.KeyValuePair) (*common.TxResponse, error) {

	pairs := append(params, &common.KeyValuePair{
		Key:   syscontract.MultiVote_TX_ID.String(),
		Value: []byte(txId),
	})
	payload := cc.createMultiSignQueryPayload(pairs)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return resp, fmt.Errorf(errStringFormat, payload.TxType.String(), err.Error())
	}

	if err = utils.CheckProposalRequestResp(resp, false); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType.String(), err.Error())
	}

	return resp, nil
}

// CreateMultiSignReqPayload create multi sign req payload
func (cc *ChainClient) CreateMultiSignReqPayload(
	pairs []*common.KeyValuePair) *common.Payload {

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_MULTI_SIGN.String(),
		syscontract.MultiSignFunction_REQ.String(), pairs, defaultSeq, nil)
	return payload
}

// CreateMultiSignReqPayloadWithGasLimit create multi sign req payload
func (cc *ChainClient) CreateMultiSignReqPayloadWithGasLimit(
	pairs []*common.KeyValuePair, gasLimit uint64) *common.Payload {

	var limit *common.Limit
	if gasLimit > 0 {
		limit = &common.Limit{
			GasLimit: gasLimit,
		}
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_MULTI_SIGN.String(),
		syscontract.MultiSignFunction_REQ.String(), pairs, defaultSeq, limit)
	return payload
}

// CreateMultiSignReqPayload create multi sign trig payload
func (cc *ChainClient) createMultiSignTrigPayload(pairs []*common.KeyValuePair, limit *common.Limit) *common.Payload {
	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_MULTI_SIGN.String(),
		syscontract.MultiSignFunction_TRIG.String(), pairs, defaultSeq, limit)

	return payload
}

// CreateMultiSignReqPayload create multi sign vote payload
func (cc *ChainClient) createMultiSignVotePayload(pairs []*common.KeyValuePair, limit *common.Limit) *common.Payload {
	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_MULTI_SIGN.String(),
		syscontract.MultiSignFunction_VOTE.String(), pairs, defaultSeq, limit)

	return payload
}

// CreateMultiSignReqPayload create multi sign query payload
func (cc *ChainClient) createMultiSignQueryPayload(pairs []*common.KeyValuePair) *common.Payload {
	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_MULTI_SIGN.String(),
		syscontract.MultiSignFunction_QUERY.String(), pairs, defaultSeq, nil)

	return payload
}
