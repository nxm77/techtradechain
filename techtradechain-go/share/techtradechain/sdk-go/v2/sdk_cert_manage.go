/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"fmt"
	"strings"

	"github.com/gogo/protobuf/proto"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
)

// GetCertHash get cert hash on chain
func (cc *ChainClient) GetCertHash() ([]byte, error) {
	chainConfig, err := cc.GetChainConfig()

	if err != nil {
		return nil, fmt.Errorf("get cert hash failed, %s", err.Error())
	}

	certHash, err := utils.GetCertificateId(cc.userCrtBytes, chainConfig.Crypto.Hash)
	if err != nil {
		return nil, fmt.Errorf("calc cert hash failed, %s", err.Error())
	}

	return certHash, nil
}

// QueryCert query cert on chain, returns *common.CertInfos
func (cc *ChainClient) QueryCert(certHashes []string) (*common.CertInfos, error) {
	cc.logger.Infof("[SDK] begin to query cert, [contract:%s]/[method:%s]",
		syscontract.SystemContract_CERT_MANAGE.String(), syscontract.CertManageFunction_CERTS_QUERY.String())

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyCertHashes,
			Value: []byte(strings.Join(certHashes, ",")),
		},
	}

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CERT_MANAGE.String(),
		syscontract.CertManageFunction_CERTS_QUERY.String(), pairs, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType.String(), err.Error())
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType.String(), err.Error())
	}

	certInfos := &common.CertInfos{}
	if err = proto.Unmarshal(resp.ContractResult.Result, certInfos); err != nil {
		return nil, fmt.Errorf("unmarshal cert infos payload failed, %s", err.Error())
	}

	return certInfos, nil
}

// AddCert add ChainClient myself cert hash to chain
func (cc *ChainClient) AddCert() (*common.TxResponse, error) {
	cc.logger.Infof("[SDK] begin to add cert, [contract:%s]/[method:%s]",
		syscontract.SystemContract_CERT_MANAGE.String(), syscontract.CertManageFunction_CERT_ADD.String())

	certHash, err := cc.GetCertHash()
	if err != nil {
		return nil, fmt.Errorf("get cert hash in hex failed, %s", err.Error())
	}

	payload := cc.CreateCertManagePayload(syscontract.CertManageFunction_CERT_ADD.String(), nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return resp, fmt.Errorf(errStringFormat, payload.TxType.String(), err.Error())
	}

	if err = utils.CheckProposalRequestResp(resp, false); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType.String(), err.Error())
	}

	resp.ContractResult = &common.ContractResult{
		Code:   utils.SUCCESS,
		Result: certHash,
	}

	return resp, nil
}

// DeleteCert delete myself cert hash on chain
func (cc *ChainClient) DeleteCert(certHashes []string) (*common.TxResponse, error) {
	cc.logger.Infof("[SDK] begin to delete cert, [contract:%s]/[method:%s]",
		syscontract.SystemContract_CERT_MANAGE.String(), syscontract.CertManageFunction_CERTS_DELETE.String())

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyCertHashes,
			Value: []byte(strings.Join(certHashes, ",")),
		},
	}

	payload := cc.CreateCertManagePayload(syscontract.CertManageFunction_CERTS_DELETE.String(), pairs)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return resp, fmt.Errorf(errStringFormat, payload.TxType.String(), err.Error())
	}

	if err = utils.CheckProposalRequestResp(resp, false); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType.String(), err.Error())
	}

	return resp, nil
}

// CreateCertManagePayload create `cert manage` payload
func (cc *ChainClient) CreateCertManagePayload(method string, kvs []*common.KeyValuePair) *common.Payload {
	cc.logger.Debugf("[SDK] create CertManagePayload, method: %s", method)
	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CERT_MANAGE.String(),
		method, kvs, defaultSeq, nil)
	return payload
}

// CreateCertManageFrozenPayload create `cert manage frozen` payload
func (cc *ChainClient) CreateCertManageFrozenPayload(certs []string) *common.Payload {
	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyCerts,
			Value: []byte(strings.Join(certs, ",")),
		},
	}

	return cc.CreateCertManagePayload(syscontract.CertManageFunction_CERTS_FREEZE.String(), pairs)
}

// CreateCertManageUnfrozenPayload create `cert manage unfrozen` payload
func (cc *ChainClient) CreateCertManageUnfrozenPayload(certs []string) *common.Payload {
	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyCerts,
			Value: []byte(strings.Join(certs, ",")),
		},
	}

	return cc.CreateCertManagePayload(syscontract.CertManageFunction_CERTS_UNFREEZE.String(), pairs)
}

// CreateCertManageRevocationPayload create `cert manage revocation` payload
func (cc *ChainClient) CreateCertManageRevocationPayload(certCrl string) *common.Payload {
	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyCertCrl,
			Value: []byte(certCrl),
		},
	}

	return cc.CreateCertManagePayload(syscontract.CertManageFunction_CERTS_REVOKE.String(), pairs)
}

// SignCertManagePayload sign `cert manage` payload
func (cc *ChainClient) SignCertManagePayload(payload *common.Payload) (*common.EndorsementEntry, error) {
	return cc.SignPayload(payload)
}

// SendCertManageRequest send cert manage request to node
func (cc *ChainClient) SendCertManageRequest(payload *common.Payload, endorsers []*common.EndorsementEntry,
	timeout int64, withSyncResult bool) (*common.TxResponse, error) {
	return cc.proposalRequest(payload, endorsers, nil, timeout, withSyncResult)
}
