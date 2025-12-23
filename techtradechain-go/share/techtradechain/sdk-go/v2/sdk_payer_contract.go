package techtradechain_sdk_go

import (
	"fmt"

	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/common/v2/random/uuid"
	acPb "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	sdkutils "techtradechain.com/techtradechain/sdk-go/v2/utils"
	"github.com/gogo/protobuf/proto"
)

// SetContractMethodPayer set contract-method payer
func (cc *ChainClient) SetContractMethodPayer(
	payerAddress string, contractName string, methodName string, requestId string,
	payerOrgId string, payerKeyPem []byte, payerCertPem []byte,
	gasLimit uint64) (
	*common.TxResponse, error) {

	privateKey, err := asym.PrivateKeyFromPEM(payerKeyPem, nil)
	if err != nil {
		return nil, err
	}

	// 构建参数
	params := syscontract.SetContractMethodPayerParams{}
	if methodName != "" {
		params.ContractName = contractName
		params.Method = methodName
		params.PayerAddress = payerAddress
	} else {
		params.ContractName = contractName
		params.PayerAddress = payerAddress
	}
	params.RequestId += uuid.GetUUID()
	message, err := proto.Marshal(&params)
	if err != nil {
		return nil, fmt.Errorf("marshal params failed, err = %v", err)
	}

	signature, err := sdkutils.SignPayloadBytesWithHashType(
		privateKey,
		cc.GetHashType(),
		[]byte(message))
	if err != nil {
		return nil, err
	}

	memberInfo := payerCertPem
	var memberType acPb.MemberType
	if cc.GetAuthType() == PermissionedWithCert {
		memberType = acPb.MemberType_CERT
	} else if cc.GetAuthType() == PermissionedWithKey {
		memberType = acPb.MemberType_PUBLIC_KEY
	} else if cc.GetAuthType() == Public {
		memberType = acPb.MemberType_PUBLIC_KEY
	}
	endorsement := sdkutils.NewEndorserWithMemberType(payerOrgId, memberInfo, memberType, signature)
	endorsementBytes, err := proto.Marshal(endorsement)
	if err != nil {
		return nil, err
	}

	var parameters []*common.KeyValuePair
	parameters = append(parameters, &common.KeyValuePair{
		Key:   syscontract.SetContractMethodPayer_PARAMS.String(),
		Value: []byte(message),
	})
	parameters = append(parameters, &common.KeyValuePair{
		Key:   syscontract.SetContractMethodPayer_ENDORSEMENT.String(),
		Value: endorsementBytes,
	})

	payload := cc.CreatePayload(
		"", common.TxType_INVOKE_CONTRACT,
		syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		syscontract.GasAccountFunction_SET_CONTRACT_METHOD_PAYER.String(),
		parameters,
		0, &common.Limit{GasLimit: gasLimit})
	// 产生 Request
	request, err := cc.GenerateTxRequest(payload, nil)
	if err != nil {
		return nil, err
	}
	cc.logger.Infof("request = %v \n", request.String())

	// 发送 Request, 读取 Response
	resp, err := cc.SendTxRequest(request, -1, true)
	if err != nil {
		return nil, err
	}

	cc.logger.Infof("resp = %v \n", resp.String())
	return resp, nil
}

// UnsetContractMethodPayer unset contract-method payer
func (cc *ChainClient) UnsetContractMethodPayer(
	contractName string, methodName string, gasLimit uint64) (*common.TxResponse, error) {

	// 构建参数
	var params []*common.KeyValuePair
	if methodName != "" {
		params = append(params, &common.KeyValuePair{
			Key:   syscontract.UnsetContractMethodPayer_CONTRACT_NAME.String(),
			Value: []byte(contractName),
		})
		params = append(params, &common.KeyValuePair{
			Key:   syscontract.UnsetContractMethodPayer_METHOD.String(),
			Value: []byte(methodName),
		})
	} else {
		params = append(params, &common.KeyValuePair{
			Key:   syscontract.UnsetContractMethodPayer_CONTRACT_NAME.String(),
			Value: []byte(contractName),
		})
	}

	payload := cc.CreatePayload(
		"", common.TxType_INVOKE_CONTRACT,
		syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		syscontract.GasAccountFunction_UNSET_CONTRACT_METHOD_PAYER.String(),
		params,
		0, &common.Limit{GasLimit: gasLimit})

	// 产生 Request
	request, err := cc.GenerateTxRequest(payload, nil)
	if err != nil {
		return nil, err
	}
	cc.logger.Infof("request = %v \n", request.String())

	// 发送 Request, 读取 Response
	resp, err := cc.SendTxRequest(request, -1, true)
	if err != nil {
		return nil, err
	}

	cc.logger.Infof("resp = %v \n", resp.String())
	return resp, nil
}

// QueryContractMethodPayer query contract-method payer
func (cc *ChainClient) QueryContractMethodPayer(
	contractName string, methodName string, gasLimit uint64) (*common.TxResponse, error) {
	// 构建参数
	var params []*common.KeyValuePair
	if methodName == "" {
		params = append(params, &common.KeyValuePair{
			Key:   syscontract.GetContractMethodPayer_CONTRACT_NAME.String(),
			Value: []byte(contractName),
		})
	} else {
		params = append(params, &common.KeyValuePair{
			Key:   syscontract.GetContractMethodPayer_CONTRACT_NAME.String(),
			Value: []byte(contractName),
		})
		params = append(params, &common.KeyValuePair{
			Key:   syscontract.GetContractMethodPayer_METHOD.String(),
			Value: []byte(methodName),
		})
	}
	// 构建 payload
	payload := cc.CreatePayload(
		"", common.TxType_INVOKE_CONTRACT,
		syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		syscontract.GasAccountFunction_GET_CONTRACT_METHOD_PAYER.String(),
		params,
		0, &common.Limit{GasLimit: gasLimit})

	// 产生 Request
	request, err := cc.GenerateTxRequest(payload, nil)
	if err != nil {
		return nil, err
	}
	cc.logger.Infof("request = %v \n", request.String())

	// 发送 Request, 读取 Response
	resp, err := cc.SendTxRequest(request, -1, true)
	if err != nil {
		return nil, err
	}

	cc.logger.Infof("resp = %v \n", resp.String())
	return resp, nil
}

// QueryTxPayer query tx payer
func (cc *ChainClient) QueryTxPayer(txId string, gasLimit uint64) (*common.TxResponse, error) {
	var params []*common.KeyValuePair
	params = append(params, &common.KeyValuePair{
		Key:   syscontract.GetTxPayer_TX_ID.String(),
		Value: []byte(txId),
	})

	// 构建 payload
	payload := cc.CreatePayload(
		"", common.TxType_INVOKE_CONTRACT,
		syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		syscontract.GasAccountFunction_GET_TX_PAYER.String(),
		params,
		0, &common.Limit{GasLimit: gasLimit})

	// 产生 Request
	request, err := cc.GenerateTxRequest(payload, nil)
	if err != nil {
		return nil, err
	}
	cc.logger.Infof("request = %v \n", request.String())

	// 发送 Request, 读取 Response
	resp, err := cc.SendTxRequest(request, -1, true)
	if err != nil {
		return nil, err
	}

	cc.logger.Infof("resp = %v \n", resp.String())
	return resp, nil
}
