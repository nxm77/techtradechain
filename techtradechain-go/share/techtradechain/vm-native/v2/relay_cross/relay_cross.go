/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Package relaycross is package for relaycross
package relaycross

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	pbcommon "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	tcipcommon "techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
	"github.com/gogo/protobuf/proto"
)

var (
	lastGatewayIdKey          = []byte("lgi")
	crossChainIdKey           = []byte("cc")
	notEndCrossChainIdListKey = []byte("nnchil")
	notEndCrossChainId        = []byte("cne")
	failedCrossChainId        = []byte("fcci")
	admin                     = ([]byte("adaddr"))

	contractName = syscontract.SystemContract_RELAY_CROSS.String()
)

// RelayCrossManager 提供中继跨链管理
type RelayCrossManager struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewRelayCrossManager create a new RelayCrossManager instance
// @param log
// @return *ContractManager
func NewRelayCrossManager(log protocol.Logger) *RelayCrossManager {
	return &RelayCrossManager{
		log:     log,
		methods: registerContractManagerMethods(log),
	}
}

// RelayCrossRuntime 提供中继跨链管理功能
type RelayCrossRuntime struct {
	log protocol.Logger
}

// GetMethod get register method by name
// @receiver r
// @param methodName
// @return common.ContractFunc
func (r *RelayCrossManager) GetMethod(methodName string) common.ContractFunc {
	return r.methods[methodName]
}

func registerContractManagerMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	runtime := &RelayCrossRuntime{log: log}

	methodMap[syscontract.RelayCrossFunction_SAVE_GATEWAY.String()] = common.WrapEventResult(
		runtime.SaveGateway)
	methodMap[syscontract.RelayCrossFunction_UPDATE_GATEWAY.String()] = common.WrapEventResult(
		runtime.UpdateGateway)
	methodMap[syscontract.RelayCrossFunction_GET_GATEWAY_NUM.String()] = common.WrapResultFunc(
		runtime.GetGatewayNum)
	methodMap[syscontract.RelayCrossFunction_GET_GATEWAY.String()] = common.WrapResultFunc(
		runtime.GetGateway)
	methodMap[syscontract.RelayCrossFunction_GET_GATEWAY_BY_RANGE.String()] = common.WrapResultFunc(
		runtime.GetGatewayByRange)
	methodMap[syscontract.RelayCrossFunction_SAVE_CROSS_CHAIN_INFO.String()] = common.WrapEventResult(
		runtime.SaveCrossChainInfo)
	methodMap[syscontract.RelayCrossFunction_UPDATE_CROSS_CHAIN_TRY.String()] = common.WrapEventResult(
		runtime.UpdateCrossChainTry)
	methodMap[syscontract.RelayCrossFunction_UPDATE_CROSS_CHAIN_RESULT.String()] = common.WrapEventResult(
		runtime.UpdateCrossChainResult)
	methodMap[syscontract.RelayCrossFunction_DELETE_ERROR_CROSS_CHAIN_TX_LIST.String()] = common.WrapResultFunc(
		runtime.DeleteErrorCrossChainTxList)
	methodMap[syscontract.RelayCrossFunction_UPDATE_CROSS_CHAIN_CONFIRM.String()] = common.WrapEventResult(
		runtime.UpdateCrossChainConfirm)
	methodMap[syscontract.RelayCrossFunction_UPDATE_SRC_GATEWAY_CONFIRM.String()] = common.WrapEventResult(
		runtime.UpdateSrcGatewayConfirm)
	//methodMap[syscontract.RelayCrossFunction_GET_CROSS_CHAIN_NUM.String()] = common.WrapResultFunc(
	//	runtime.GetCrossChainNum)
	methodMap[syscontract.RelayCrossFunction_GET_CROSS_CHAIN_INFO.String()] = common.WrapResultFunc(
		runtime.GetCrossChainInfo)
	methodMap[syscontract.RelayCrossFunction_GET_CROSS_CHAIN_INFO_BY_RANGE.String()] = common.WrapResultFunc(
		runtime.GetCrossChainInfoByRange)
	methodMap[syscontract.RelayCrossFunction_GET_NOT_END_CROSS_CHIAN_ID_LIST.String()] = common.WrapResultFunc(
		runtime.GetNotEndCrossChainIdList)
	methodMap[syscontract.RelayCrossFunction_SET_CROSS_ADMIN.String()] = common.WrapEventResult(
		runtime.SetCrossAdmin)
	methodMap[syscontract.RelayCrossFunction_DELETE_CROSS_ADEMIN.String()] = common.WrapEventResult(
		runtime.DeleteCrossAdmin)
	methodMap[syscontract.RelayCrossFunction_IS_CROSS_ADMIN.String()] = common.WrapResultFunc(
		runtime.IsCrossAdmin)
	methodMap[syscontract.RelayCrossFunction_SAVE_SYNC_CROSS_CHAIN_INFO.String()] = common.WrapEventResult(
		runtime.SaveSyncCrossChainInfo)

	return methodMap
}

// SetCrossAdmin 设置管理员
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return []*pbcommon.ContractEvent
//	@return error
func (r *RelayCrossRuntime) SetCrossAdmin(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, []*pbcommon.ContractEvent, error) {
	err := checkParams(params,
		syscontract.SetCrossAdmin_CROSS_ADMIN_ADDRESS.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.SetCrossAdmin checkParams param error: [%v]", err)
		return nil, nil, err
	}
	address := params[syscontract.SetCrossAdmin_CROSS_ADMIN_ADDRESS.String()]
	addressMap, err := getAdminAddr(ctx)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.SetCrossAdmin getAdminAddr: [%v]", err)
		return nil, nil, err
	}
	addressMap[string(address)] = true
	addressByte, err := putAdminAddr(ctx, addressMap)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.SetCrossAdmin putAdminAddr: [%v]", err)
		return nil, nil, err
	}

	cfg, err := common.GetChainConfigNoRecord(ctx)
	if err != nil {
		return nil, nil, err
	}
	event := []*pbcommon.ContractEvent{
		{
			Topic:           tcipcommon.EventName_SET_CROSS_ADMIN.String(),
			TxId:            ctx.GetTx().Payload.TxId,
			ContractName:    contractName,
			ContractVersion: cfg.Version,
			EventData: []string{
				string(address),
			},
		},
	}
	return addressByte, event, nil
}

// DeleteCrossAdmin 删除管理员
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return []*pbcommon.ContractEvent
//	@return error
func (r *RelayCrossRuntime) DeleteCrossAdmin(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, []*pbcommon.ContractEvent, error) {
	err := checkParams(params,
		syscontract.DeleteCrossAdmin_CROSS_ADMIN_ADDRESS.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.DeleteCrossAdmin checkParams param error: [%v]", err)
		return nil, nil, err
	}
	address := params[syscontract.SetCrossAdmin_CROSS_ADMIN_ADDRESS.String()]
	addressMap, err := getAdminAddr(ctx)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.DeleteCrossAdmin getAdminAddr: [%v]", err)
		return nil, nil, err
	}
	delete(addressMap, string(address))
	addressString, err := putAdminAddr(ctx, addressMap)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.DeleteCrossAdmin putAdminAddr: [%v]", err)
		return nil, nil, err
	}

	cfg, err := common.GetChainConfigNoRecord(ctx)
	if err != nil {
		return nil, nil, err
	}
	event := []*pbcommon.ContractEvent{
		{
			Topic:           tcipcommon.EventName_DELETE_CROSS_ADMIN.String(),
			TxId:            ctx.GetTx().Payload.TxId,
			ContractName:    contractName,
			ContractVersion: cfg.Version,
			EventData: []string{
				string(address),
			},
		},
	}
	return addressString, event, nil
}

// IsCrossAdmin 是否是管理员
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
func (r *RelayCrossRuntime) IsCrossAdmin(ctx protocol.TxSimContext, params map[string][]byte) ([]byte, error) {
	isAdmin, err := isCrossAdmin(ctx)
	if err != nil || !isAdmin {
		return []byte("false"), err
	}
	return []byte("true"), nil
}

// SaveGateway 保存gateway
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
func (r *RelayCrossRuntime) SaveGateway(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, []*pbcommon.ContractEvent, error) {
	isAdmin, _ := isCrossAdmin(ctx)
	if !isAdmin {
		return nil, nil, errors.New("sender not admin")
	}
	err := checkParams(params, syscontract.SaveGateway_GATEWAY_INFO_BYTE.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.SaveGateway checkParams param error: [%v]", err)
		return nil, nil, err
	}
	// 获取参数
	gatewayInfoByte := params[syscontract.SaveGateway_GATEWAY_INFO_BYTE.String()]
	gatewayInfoByte = []byte(base64.StdEncoding.EncodeToString(gatewayInfoByte))

	lastGatewayIdNum, err := r.getNewGatewayId(ctx)
	if err != nil {
		r.log.Warnf("RelayCrossRuntime.SaveGateway get new gateway id error: [%v]", err)
		return nil, nil, err
	}

	// 保存gateway
	err = ctx.Put(contractName, parseGatewayKey(lastGatewayIdNum), gatewayInfoByte)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.SaveGateway put gateway error: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.SaveGateway put gateway error: [%v]", err)
	}
	cfg, err := common.GetChainConfigNoRecord(ctx)
	if err != nil {
		return nil, nil, err
	}
	event := []*pbcommon.ContractEvent{
		{
			Topic:           tcipcommon.EventName_NEW_CROSS_GATEWAY.String(),
			TxId:            ctx.GetTx().Payload.TxId,
			ContractName:    contractName,
			ContractVersion: cfg.Version,
			EventData: []string{
				string(gatewayInfoByte),
			},
		},
	}
	return []byte(fmt.Sprintf("%d", lastGatewayIdNum)), event, nil
}

// UpdateGateway 更新gateway
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
func (r *RelayCrossRuntime) UpdateGateway(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, []*pbcommon.ContractEvent, error) {
	isAdmin, _ := isCrossAdmin(ctx)
	if !isAdmin {
		return nil, nil, errors.New("sender not admin")
	}
	err := checkParams(params,
		syscontract.UpdateGateway_GATEWAY_INFO_BYTE.String(),
		syscontract.UpdateGateway_GATEWAY_ID.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateGateway checkParams param error: [%v]", err)
		return nil, nil, err
	}

	// 获取参数
	gatewayId := string(params[syscontract.UpdateGateway_GATEWAY_ID.String()])
	gatewayInfoByte := params[syscontract.UpdateGateway_GATEWAY_INFO_BYTE.String()]

	gatewayInfoByte = []byte(base64.StdEncoding.EncodeToString(gatewayInfoByte))

	// 根据最后一个gatewayId判断用户传的gatewayId是否存在
	lastGatewayIdNum := r.getLastGatewayId(ctx)
	gatewayIdNum, err := strconv.Atoi(gatewayId)
	if err != nil || int64(gatewayIdNum) > lastGatewayIdNum {
		r.log.Errorf("RelayCrossRuntime.UpdateGateway invalid gateway_id: %s", gatewayId)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateGateway invalid gateway_id: %s", gatewayId)
	}

	// 保存gateway
	err = ctx.Put(contractName, parseGatewayKey(int64(gatewayIdNum)), gatewayInfoByte)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateGateway fail to save gateway info")
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateGateway fail to save gateway info")
	}
	cfg, err := common.GetChainConfigNoRecord(ctx)
	if err != nil {
		return nil, nil, err
	}
	event := []*pbcommon.ContractEvent{
		{
			Topic:           tcipcommon.EventName_CROSS_GATEWAY_UPDATE.String(),
			TxId:            ctx.GetTx().Payload.TxId,
			ContractName:    contractName,
			ContractVersion: cfg.Version,
			EventData: []string{
				string(gatewayInfoByte),
			},
		},
	}
	return []byte(gatewayId), event, nil
}

// GetGatewayNum 获取gateway个数
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
func (r *RelayCrossRuntime) GetGatewayNum(ctx protocol.TxSimContext, params map[string][]byte) ([]byte, error) {
	lastGatewayId := r.getLastGatewayId(ctx)
	return []byte(fmt.Sprintf("%d", lastGatewayId)), nil
}

// GetGateway 获取gateway
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
func (r *RelayCrossRuntime) GetGateway(ctx protocol.TxSimContext, params map[string][]byte) ([]byte, error) {
	err := checkParams(params,
		syscontract.GetGateway_GATEWAY_ID.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetGateway checkParams param error: [%v]", err)
		return nil, err
	}

	// 获取参数
	gatewayId := string(params[syscontract.GetGateway_GATEWAY_ID.String()])
	gatewayIdNum, err := strconv.Atoi(gatewayId)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetGateway invalid gateway_id: [%s]", gatewayId)
		return nil, fmt.Errorf("RelayCrossRuntime.GetGateway invalid gateway_id: [%s]", gatewayId)
	}

	gatewayInfoByte, err := ctx.Get(contractName, parseGatewayKey(int64(gatewayIdNum)))
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetGateway no such gateway id: [%s]", gatewayId)
		return nil, fmt.Errorf("RelayCrossRuntime.GetGateway no such gateway id: [%s]", gatewayId)
	}
	gatewayInfoDecode, err := base64.StdEncoding.DecodeString(string(gatewayInfoByte))
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetGateway decode gateway error: [%s]", err.Error())
		return nil, fmt.Errorf("RelayCrossRuntime.GetGateway decode gateway error: [%s]", err.Error())
	}
	return gatewayInfoDecode, nil
}

// GetGatewayByRange 批量获取gateway
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
func (r *RelayCrossRuntime) GetGatewayByRange(ctx protocol.TxSimContext, params map[string][]byte) ([]byte, error) {
	err := checkParams(params,
		syscontract.GetGatewayByRange_START_GATEWAY_ID.String(),
		syscontract.GetGatewayByRange_STOP_GATEWAY_ID.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetGatewayByRange checkParams param error: [%v]", err)
		return nil, err
	}

	// 获取参数
	startGatewayId := string(params[syscontract.GetGatewayByRange_START_GATEWAY_ID.String()])
	stopGatewayId := string(params[syscontract.GetGatewayByRange_STOP_GATEWAY_ID.String()])

	startGatewayIdNum, err := strconv.Atoi(startGatewayId)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway error:[%v]", err)
		return nil, fmt.Errorf("RelayCrossRuntime.GetGatewayByRange decode gateway error: [%v]", err)
	}
	stopGatewayIdNum, err := strconv.Atoi(stopGatewayId)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway error: [%v]", err)
		return nil, fmt.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway error: [%v]", err)
	}
	result, err := ctx.Select(contractName, parseGatewayKey(int64(startGatewayIdNum)),
		parseGatewayKey(int64(stopGatewayIdNum)))
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway by range error: [%v]", err)
		return nil, fmt.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway by range error: [%v]", err)
	}

	var gatewayInfos [][]byte

	for result.Next() {
		kv, err := result.Value()
		if err != nil {
			r.log.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway from iterator error: [%v]", err)
			return nil, fmt.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway from iterator error: [%v]",
				err)
		}
		gatewayInfo := kv.Value
		gatewayInfoDecode, err := base64.StdEncoding.DecodeString(string(gatewayInfo))
		if err != nil {
			r.log.Errorf("RelayCrossRuntime.GetGatewayByRange decode gateway error: [%v]", err)
			return nil, fmt.Errorf("RelayCrossRuntime.GetGatewayByRange decode gateway error: [%v]", err)
		}
		gatewayInfos = append(gatewayInfos, gatewayInfoDecode)
	}
	r.log.Infof("RelayCrossRuntime.GetGatewayByRange result len %d", len(gatewayInfos))
	resultByte, err := json.Marshal(gatewayInfos)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway slice byte error: [%v]", err)
		return nil, fmt.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway slice byte error: [%v]", err)
	}
	return resultByte, nil
}

// SaveSyncCrossChainInfo 保存跨链交易，一步到位
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return []*pbcommon.ContractEvent
//	@return error
func (r *RelayCrossRuntime) SaveSyncCrossChainInfo(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, []*pbcommon.ContractEvent, error) {
	isAdmin, _ := isCrossAdmin(ctx)
	if !isAdmin {
		return nil, nil, errors.New("sender not admin")
	}
	err := checkParams(params,
		syscontract.SaveSyncCrossChainInfo_CROSS_CHAIN_ID.String(),
		syscontract.SaveSyncCrossChainInfo_CROSS_CHAIN_INFO_BYTE.String(),
	)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.SaveCrossChainInfo checkParams param error: [%v]", err)
		return nil, nil, err
	}

	// 获取参数
	crossChainInfoByte := params[syscontract.SaveSyncCrossChainInfo_CROSS_CHAIN_INFO_BYTE.String()]
	crossChainInfoByte = []byte(base64.StdEncoding.EncodeToString(crossChainInfoByte))
	crossChainIdByte := params[syscontract.SaveSyncCrossChainInfo_CROSS_CHAIN_ID.String()]

	err = ctx.Put(contractName, parseCrossChainKey(string(crossChainIdByte)), crossChainInfoByte)
	if err != nil {
		msg := fmt.Sprintf("RelayCrossRuntime.SaveSyncCrossChainInfo save sync cross "+
			"chain info byte error: [%v]", err)
		r.log.Error(msg)
		return nil, nil, errors.New(msg)
	}
	cfg, err := common.GetChainConfigNoRecord(ctx)
	if err != nil {
		return nil, nil, err
	}
	event := make([]*pbcommon.ContractEvent, 0)
	event = append(event, &pbcommon.ContractEvent{
		Topic:           tcipcommon.EventName_SRC_GATEWAY_CONFIRM_END.String(),
		TxId:            ctx.GetTx().Payload.TxId,
		ContractName:    contractName,
		ContractVersion: cfg.Version,
		EventData: []string{
			string(crossChainIdByte), "",
		},
	})
	return crossChainInfoByte, event, nil
}

// SaveCrossChainInfo 保存跨链交易
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return []*pbcommon.ContractEvent
//	@return error
func (r *RelayCrossRuntime) SaveCrossChainInfo(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, []*pbcommon.ContractEvent, error) {
	isAdmin, _ := isCrossAdmin(ctx)
	if !isAdmin {
		return nil, nil, errors.New("sender not admin")
	}
	err := checkParams(params,
		syscontract.SaveCrossChainInfo_CROSS_CHAIN_INFO_BYTE.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.SaveCrossChainInfo checkParams param error: [%v]", err)
		return nil, nil, err
	}

	// 获取参数
	crossChainInfoByte := params[syscontract.SaveCrossChainInfo_CROSS_CHAIN_INFO_BYTE.String()]
	var crossChainInfo tcipcommon.CrossChainInfo
	_ = json.Unmarshal(crossChainInfoByte, &crossChainInfo)

	// 根据最后一个crossChainId生成新的crossChainId
	lastCrossChainIdNum, err := r.getNewCrossId(ctx)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.SaveCrossChainInfo get gateway slice byte error: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.SaveCrossChainInfo get gateway slice byte error: [%v]", err)
	}
	crossChainId := lastCrossChainIdNum
	crossChainStr := fmt.Sprintf("%s", lastCrossChainIdNum)

	crossChainInfo.CrossChainId = crossChainStr
	crossChainInfo.State = tcipcommon.CrossChainStateValue_WAIT_EXECUTE

	crossChainInfoByte, _ = json.Marshal(&crossChainInfo)
	crossChainInfoByte = []byte(base64.StdEncoding.EncodeToString(crossChainInfoByte))

	// 保存crossChain
	err = ctx.Put(contractName, parseCrossChainKey(crossChainId), crossChainInfoByte)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.SaveCrossChainInfo get gateway slice byte error: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.SaveCrossChainInfo get gateway slice byte error: [%v]", err)
	}

	// 获取未完成的cross chain id
	err = ctx.Put(contractName, parseNotEndCrossId(crossChainId), []byte(fmt.Sprintf("%s", crossChainId)))
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.SaveCrossChainInfo save not end cross id error: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.SaveCrossChainInfo save not end cross id error: [%v]", err)
	}

	cfg, err := common.GetChainConfigNoRecord(ctx)
	if err != nil {
		return nil, nil, err
	}
	event := []*pbcommon.ContractEvent{
		{
			Topic:           tcipcommon.EventName_NEW_CROSS_CHAIN.String(),
			TxId:            ctx.GetTx().Payload.TxId,
			ContractName:    contractName,
			ContractVersion: cfg.Version,
			EventData: []string{
				crossChainStr, crossChainInfo.FirstTxContent.TxContent.TxId,
			},
		},
	}
	return []byte(crossChainStr), event, nil
}

// UpdateCrossChainTry 更新cross chain try 信息
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return []*pbcommon.ContractEvent
//	@return error
func (r *RelayCrossRuntime) UpdateCrossChainTry(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, []*pbcommon.ContractEvent, error) {
	isAdmin, _ := isCrossAdmin(ctx)
	if !isAdmin {
		return nil, nil, errors.New("sender not admin")
	}
	event := make([]*pbcommon.ContractEvent, 0)
	err := checkParams(params,
		syscontract.UpdateCrossChainTry_CROSS_CHAIN_ID.String(),
		syscontract.UpdateCrossChainTry_CROSS_CHAIN_TX_BYTE.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainTry checkParams param error: [%v]", err)
		return nil, nil, err
	}

	// 获取参数
	crossChainId := string(params[syscontract.UpdateCrossChainTry_CROSS_CHAIN_ID.String()])
	crossChainTryByte := params[syscontract.UpdateCrossChainTry_CROSS_CHAIN_TX_BYTE.String()]

	oldCrossChainInfo, err := getCrossChainInfo(crossChainId, ctx)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainTry get cross chain info error: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateCrossChainTry get cross chain info error: [%v]",
			err)
	}

	var crossChainTxUpChain []*tcipcommon.CrossChainTxUpChain
	err = json.Unmarshal(crossChainTryByte, &crossChainTxUpChain)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainTry \"Unmarshal cross_chain_info_byte failed: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateCrossChainTry \"Unmarshal "+
			"cross_chain_info_byte failed: [%v]", err)
	}
	if len(oldCrossChainInfo.CrossChainTxContent) == 0 {
		oldCrossChainInfo.CrossChainTxContent = make([]*tcipcommon.TxContentWithVerify,
			len(oldCrossChainInfo.CrossChainMsg))
	}
	hasNil := false
	for _, crossChainTxContent := range crossChainTxUpChain {
		if crossChainTxContent != nil {
			oldCrossChainInfo.CrossChainTxContent[crossChainTxContent.Index] = crossChainTxContent.TxContentWithVerify
		}
	}
	for _, crossChainTxContent := range oldCrossChainInfo.CrossChainTxContent {
		if crossChainTxContent == nil {
			hasNil = true
		}
	}
	if !hasNil {
		cfg, err := common.GetChainConfigNoRecord(ctx)
		if err != nil {
			return nil, nil, err
		}
		firstTxId := ""
		if oldCrossChainInfo.FirstTxContent != nil && oldCrossChainInfo.FirstTxContent.TxContent != nil {
			firstTxId = oldCrossChainInfo.FirstTxContent.TxContent.TxId
		}
		oldCrossChainInfo.State = tcipcommon.CrossChainStateValue_WAIT_CONFIRM
		event = append(event, &pbcommon.ContractEvent{
			Topic:           tcipcommon.EventName_CROSS_CHAIN_TRY_END.String(),
			TxId:            ctx.GetTx().Payload.TxId,
			ContractName:    contractName,
			ContractVersion: cfg.Version,
			EventData: []string{
				oldCrossChainInfo.CrossChainId, firstTxId,
			},
		})
	}
	crossChainInfoByte, _ := json.Marshal(oldCrossChainInfo)
	crossChainInfoByte = []byte(base64.StdEncoding.EncodeToString(crossChainInfoByte))

	crossChainIdNum := oldCrossChainInfo.CrossChainId

	// 保存cross chain
	err = ctx.Put(contractName, parseCrossChainKey(crossChainIdNum), crossChainInfoByte)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainTry fail to save cross chain info: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateCrossChainTry fail to save cross chain info: [%v]",
			err)
	}
	return []byte(oldCrossChainInfo.CrossChainId), event, nil
}

// UpdateCrossChainResult 更新跨链结果
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return []*pbcommon.ContractEvent
//	@return error
func (r *RelayCrossRuntime) UpdateCrossChainResult(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, []*pbcommon.ContractEvent, error) {
	isAdmin, _ := isCrossAdmin(ctx)
	if !isAdmin {
		return nil, nil, errors.New("sender not admin")
	}
	event := make([]*pbcommon.ContractEvent, 0)
	err := checkParams(params,
		syscontract.UpdateCrossChainResult_CROSS_CHAIN_ID.String(),
		syscontract.UpdateCrossChainResult_CROSS_CHAIN_RESULT.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainResult checkParams param error: [%v]", err)
		return nil, nil, err
	}

	// 获取参数
	crossChainId := string(params[syscontract.UpdateCrossChainResult_CROSS_CHAIN_ID.String()])
	crossChainResult := string(params[syscontract.UpdateCrossChainResult_CROSS_CHAIN_RESULT.String()])

	oldCrossChainInfo, err := getCrossChainInfo(crossChainId, ctx)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainResult get cross chain info error: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateCrossChainResult get cross chain info error: [%v]",
			err)
	}

	crossChainIdNum := oldCrossChainInfo.CrossChainId

	if crossChainResult == "true" {
		oldCrossChainInfo.CrossChainResult = true
	} else {
		oldCrossChainInfo.CrossChainResult = false
		failedCrossChainIdKey := parseFailCrossChainIdKey(crossChainIdNum)
		// 保存失败的crossChainId
		err = ctx.Put(contractName, failedCrossChainIdKey, []byte(oldCrossChainInfo.CrossChainId))
		if err != nil {
			r.log.Errorf("RelayCrossRuntime.UpdateCrossChainResult fail to save cross chain info: [%v]", err)
			return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateCrossChainResult fail "+
				"to save cross chain info: [%v]", err)
		}
	}
	cfg, err := common.GetChainConfigNoRecord(ctx)
	if err != nil {
		return nil, nil, err
	}
	firstTxId := ""
	if oldCrossChainInfo.FirstTxContent != nil && oldCrossChainInfo.FirstTxContent.TxContent != nil {
		firstTxId = oldCrossChainInfo.FirstTxContent.TxContent.TxId
	}
	if oldCrossChainInfo.CrossType == tcipcommon.CrossType_INVOKE {
		event = append(event, &pbcommon.ContractEvent{
			Topic:           tcipcommon.EventName_UPADATE_RESULT_END.String(),
			TxId:            ctx.GetTx().Payload.TxId,
			ContractName:    contractName,
			ContractVersion: cfg.Version,
			EventData: []string{
				oldCrossChainInfo.CrossChainId, firstTxId,
			},
		})
	} else {
		event = append(event, &pbcommon.ContractEvent{
			Topic:           tcipcommon.EventName_GATEWAY_CONFIRM_END.String(),
			TxId:            ctx.GetTx().Payload.TxId,
			ContractName:    contractName,
			ContractVersion: cfg.Version,
			EventData: []string{
				oldCrossChainInfo.CrossChainId, firstTxId,
			},
		})
	}
	crossChainInfoByte, _ := json.Marshal(oldCrossChainInfo)
	crossChainInfoByte = []byte(base64.StdEncoding.EncodeToString(crossChainInfoByte))
	// 保存cross chain
	err = ctx.Put(contractName, parseCrossChainKey(crossChainIdNum), crossChainInfoByte)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainResult fail to save cross chain info: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateCrossChainResult fail to "+
			"save cross chain info: [%v]", err)
	}

	return []byte(oldCrossChainInfo.CrossChainId), event, nil
}

// DeleteErrorCrossChainTxList 删除错误的跨链id
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
func (r *RelayCrossRuntime) DeleteErrorCrossChainTxList(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {
	isAdmin, _ := isCrossAdmin(ctx)
	if !isAdmin {
		return nil, errors.New("sender not admin")
	}
	err := checkParams(params,
		syscontract.DeleteErrorCrossChainTxList_CROSS_CHAIN_ID.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.DeleteErrorCrossChainTxList checkParams param error: [%v]", err)
		return nil, err
	}
	crossChainId := string(params[syscontract.DeleteErrorCrossChainTxList_CROSS_CHAIN_ID.String()])
	crossChainIdNum := crossChainId
	_ = ctx.Del(contractName, parseFailCrossChainIdKey(crossChainIdNum))
	return []byte("success"), nil
}

// UpdateCrossChainConfirm 更新目标网关的confirm信息
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return []*pbcommon.ContractEvent
//	@return error
func (r *RelayCrossRuntime) UpdateCrossChainConfirm(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, []*pbcommon.ContractEvent, error) {
	isAdmin, _ := isCrossAdmin(ctx)
	if !isAdmin {
		return nil, nil, errors.New("sender not admin")
	}
	event := make([]*pbcommon.ContractEvent, 0)
	err := checkParams(params,
		syscontract.UpdateCrossChainResult_CROSS_CHAIN_ID.String(),
		syscontract.UpdateCrossChainResult_CROSS_CHAIN_RESULT.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainConfirm checkParams param error: [%v]", err)
		return nil, nil, err
	}

	// 获取参数
	crossChainId := string(params[syscontract.UpdateCrossChainResult_CROSS_CHAIN_ID.String()])
	crossChainConfirmByte := params[syscontract.UpdateCrossChainResult_CROSS_CHAIN_RESULT.String()]

	oldCrossChainInfo, err := getCrossChainInfo(crossChainId, ctx)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainConfirm get cross chain info error: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateCrossChainConfirm get cross chain info error: [%v]",
			err)
	}

	var crossChainConfirm []*tcipcommon.CrossChainConfirmUpChain
	err = json.Unmarshal(crossChainConfirmByte, &crossChainConfirm)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainConfirm Unmarshal cross_chain_info_byte failed: [%v]",
			err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateCrossChainConfirm Unmarshal "+
			"cross_chain_info_byte failed: [%v]", err)
	}
	if len(oldCrossChainInfo.GatewayConfirmResult) == 0 {
		oldCrossChainInfo.GatewayConfirmResult = make([]*tcipcommon.CrossChainConfirm,
			len(oldCrossChainInfo.CrossChainMsg))
	}
	hasNil := false
	for _, gatewayConfirmResult := range crossChainConfirm {
		if gatewayConfirmResult != nil {
			oldCrossChainInfo.GatewayConfirmResult[gatewayConfirmResult.Index] = gatewayConfirmResult.CrossChainConfirm
		}
	}
	for _, gatewayConfirmResult := range oldCrossChainInfo.GatewayConfirmResult {
		if gatewayConfirmResult == nil {
			hasNil = true
			break
		}
	}
	if !hasNil {
		firstTxId := ""
		if oldCrossChainInfo.FirstTxContent != nil && oldCrossChainInfo.FirstTxContent.TxContent != nil {
			firstTxId = oldCrossChainInfo.FirstTxContent.TxContent.TxId
		}
		cfg, err := common.GetChainConfigNoRecord(ctx)
		if err != nil {
			return nil, nil, err
		}
		event = append(event, &pbcommon.ContractEvent{
			Topic:           tcipcommon.EventName_GATEWAY_CONFIRM_END.String(),
			TxId:            ctx.GetTx().Payload.TxId,
			ContractName:    contractName,
			ContractVersion: cfg.Version,
			EventData: []string{
				oldCrossChainInfo.CrossChainId, firstTxId,
			},
		})
	}
	crossChainInfoByte, _ := json.Marshal(oldCrossChainInfo)
	crossChainInfoByte = []byte(base64.StdEncoding.EncodeToString(crossChainInfoByte))

	crossChainIdNum := oldCrossChainInfo.CrossChainId

	// 保存cross chain
	err = ctx.Put(contractName, parseCrossChainKey(crossChainIdNum), crossChainInfoByte)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateCrossChainConfirm fail to save cross chain info: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateCrossChainConfirm fail to "+
			"save cross chain info: [%v]", err)
	}
	return []byte(oldCrossChainInfo.CrossChainId), event, nil
}

// UpdateSrcGatewayConfirm 更新源网关的confirm信息
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return []*pbcommon.ContractEvent
//	@return error
func (r *RelayCrossRuntime) UpdateSrcGatewayConfirm(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, []*pbcommon.ContractEvent, error) {
	isAdmin, _ := isCrossAdmin(ctx)
	if !isAdmin {
		return nil, nil, errors.New("sender not admin")
	}
	event := make([]*pbcommon.ContractEvent, 0)
	err := checkParams(params,
		syscontract.UpdateSrcGatewayConfirm_CROSS_CHAIN_ID.String(),
		syscontract.UpdateSrcGatewayConfirm_CONFIRM_RESULT.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateSrcGatewayConfirm checkParams param error: [%v]", err)
		return nil, nil, err
	}

	// 获取参数
	crossChainId := string(params[syscontract.UpdateSrcGatewayConfirm_CROSS_CHAIN_ID.String()])
	confrimResultByte := params[syscontract.UpdateSrcGatewayConfirm_CONFIRM_RESULT.String()]

	oldCrossChainInfo, err := getCrossChainInfo(crossChainId, ctx)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateSrcGatewayConfirm get cross chain info error: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateSrcGatewayConfirm get cross chain info error: [%v]",
			err)
	}

	var confrimResult tcipcommon.CrossChainConfirm
	err = proto.Unmarshal(confrimResultByte, &confrimResult)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateSrcGatewayConfirm Unmarshal confrimeResult failed: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateSrcGatewayConfirm Unmarshal"+
			" confrimeResult failed: [%v]", err)
	}

	if oldCrossChainInfo.ConfirmResult != nil && *oldCrossChainInfo.ConfirmResult != (tcipcommon.CrossChainConfirm{}) {
		return []byte("It's been updated"), nil, nil
	}
	oldCrossChainInfo.ConfirmResult = &confrimResult
	cfg, err := common.GetChainConfigNoRecord(ctx)
	if err != nil {
		return nil, nil, err
	}
	firstTxId := ""
	if oldCrossChainInfo.FirstTxContent != nil && oldCrossChainInfo.FirstTxContent.TxContent != nil {
		firstTxId = oldCrossChainInfo.FirstTxContent.TxContent.TxId
	}
	event = append(event, &pbcommon.ContractEvent{
		Topic:           tcipcommon.EventName_SRC_GATEWAY_CONFIRM_END.String(),
		TxId:            ctx.GetTx().Payload.TxId,
		ContractName:    contractName,
		ContractVersion: cfg.Version,
		EventData: []string{
			oldCrossChainInfo.CrossChainId, firstTxId,
		},
	})
	if oldCrossChainInfo.CrossChainResult {
		oldCrossChainInfo.State = tcipcommon.CrossChainStateValue_CONFIRM_END
	} else {
		oldCrossChainInfo.State = tcipcommon.CrossChainStateValue_CANCEL_END
	}
	crossChainInfoByte, _ := json.Marshal(oldCrossChainInfo)
	crossChainInfoByte = []byte(base64.StdEncoding.EncodeToString(crossChainInfoByte))

	crossChainIdNum := oldCrossChainInfo.CrossChainId

	// 保存cross chain
	err = ctx.Put(contractName, parseCrossChainKey(crossChainIdNum), crossChainInfoByte)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateSrcGatewayConfirm fail to save cross chain info: [%v]", err)
		return nil, nil, fmt.Errorf("RelayCrossRuntime.UpdateSrcGatewayConfirm "+
			"fail to save cross chain info: [%v]", err)
	}

	// 从未完成的列表中删除
	err = ctx.Del(contractName, parseNotEndCrossId(crossChainIdNum))
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.UpdateSrcGatewayConfirm fail to delete cross chain Id: [%v]", err)
		return nil, nil,
			fmt.Errorf("RelayCrossRuntime.UpdateSrcGatewayConfirm fail to delete cross chain Id: [%v]", err)
	}
	return []byte(oldCrossChainInfo.CrossChainId), event, nil
}

// GetCrossChainNum 获取跨链交易个数
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
//func (r *RelayCrossRuntime) GetCrossChainNum(ctx protocol.TxSimContext,
//	params map[string][]byte) ([]byte, error) {
//	lastCrossChainId := r.getLastCrossId(ctx)
//	return []byte(fmt.Sprintf("%d", lastCrossChainId)), nil
//}

// GetCrossChainInfo 获取跨链交易
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
func (r *RelayCrossRuntime) GetCrossChainInfo(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {
	err := checkParams(params,
		syscontract.GetCrossChainInfo_CROSS_CHAIN_ID.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetCrossChainInfo checkParams param error: [%v]", err)
		return nil, err
	}

	// 获取参数
	crossChainId := string(params[syscontract.GetCrossChainInfo_CROSS_CHAIN_ID.String()])

	crossChainIdNum := crossChainId

	crossChainInfoByte, err := ctx.Get(contractName, parseCrossChainKey(crossChainIdNum))
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetCrossChainInfo no such cross chain id: [%s]", crossChainId)
		return nil, fmt.Errorf("RelayCrossRuntime.GetCrossChainInfo no such cross chain id: [%s]", crossChainId)
	}
	crossChainInfoDecode, err := base64.StdEncoding.DecodeString(string(crossChainInfoByte))
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetCrossChainInfo decode cross chain error: [%v]", err)
		return nil, fmt.Errorf("RelayCrossRuntime.GetCrossChainInfo decode cross chain error: [%v]", err)
	}
	return crossChainInfoDecode, nil
}

// GetCrossChainInfoByRange 批量获取跨链交易
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
func (r *RelayCrossRuntime) GetCrossChainInfoByRange(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {
	err := checkParams(params,
		syscontract.GetCrossChainInfoByRange_START_CROSS_CHAIN_ID.String(),
		syscontract.GetCrossChainInfoByRange_STOP_CROSS_CHAIN_ID.String())
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetCrossChainInfoByRange checkParams param error: [%v]", err)
		return nil, err
	}

	// 获取参数
	startCrossChainId := string(params[syscontract.GetCrossChainInfoByRange_START_CROSS_CHAIN_ID.String()])
	stopCrossChainId := string(params[syscontract.GetCrossChainInfoByRange_STOP_CROSS_CHAIN_ID.String()])

	result, err := ctx.Select(contractName, parseCrossChainKey(startCrossChainId),
		parseCrossChainKey(stopCrossChainId))
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetCrossChainInfoByRange get cross chain by range error: [%v]", err)
		return nil, fmt.Errorf("RelayCrossRuntime.GetCrossChainInfoByRange get cross chain by range error: [%v]",
			err)
	}

	crossChainInfos := make([][]byte, 0)

	for result.Next() {
		kv, err := result.Value()
		if err != nil {
			r.log.Errorf("RelayCrossRuntime.GetCrossChainInfoByRange get cross chain"+
				" from iterator error: [%v]", err)
			return nil, fmt.Errorf("RelayCrossRuntime.GetCrossChainInfoByRange get cross"+
				" chain from iterator error: [%v]", err)
		}
		crossChainInfo := kv.Value
		crossChainInfoDecode, err := base64.StdEncoding.DecodeString(string(crossChainInfo))
		if err != nil {
			r.log.Errorf("RelayCrossRuntime.GetCrossChainInfoByRange decode cross chain error: [%v]", err)
			return nil, fmt.Errorf("RelayCrossRuntime.GetCrossChainInfoByRange decode cross "+
				"chain error: [%v]", err)
		}
		crossChainInfos = append(crossChainInfos, crossChainInfoDecode)
	}
	resultByte, err := json.Marshal(crossChainInfos)
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetCrossChainInfoByRange get cross chain slice byte error: [%v]", err)
		return nil, fmt.Errorf("RelayCrossRuntime.GetCrossChainInfoByRange get cross "+
			"chain slice byte error: [%v]", err)
	}
	return resultByte, nil
}

// GetNotEndCrossChainIdList 获取未完成的跨链交易id
//
//	@receiver r
//	@param ctx
//	@param params
//	@return []byte
//	@return error
func (r *RelayCrossRuntime) GetNotEndCrossChainIdList(ctx protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {
	keyPre := notEndCrossChainId
	limitKey := make([]byte, len(notEndCrossChainId))
	copy(limitKey, notEndCrossChainId)
	limitKey[len(limitKey)-1] = limitKey[len(limitKey)-1] + 1
	result, err := ctx.Select(contractName, keyPre, limitKey)

	if err != nil {
		r.log.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway by range error: [%v]", err)
		return nil, fmt.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway by range error: [%v]", err)
	}

	var crossIds []string

	for result.Next() {
		kv, err := result.Value()
		if err != nil {
			r.log.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway from iterator error: [%v]", err)
			return nil, fmt.Errorf("RelayCrossRuntime.GetGatewayByRange get gateway from iterator error: [%v]",
				err)
		}
		if kv.Value != nil {
			crossIds = append(crossIds, string(kv.Value))
		}
	}

	return json.Marshal(crossIds)
}

// getNewGatewayId 获取新的gateway id
//
//	@receiver r
//	@param ctx
//	@return int64
//	@return error
func (r *RelayCrossRuntime) getNewGatewayId(ctx protocol.TxSimContext) (int64, error) {
	lastGatewayIdNum := r.getLastGatewayId(ctx)
	err := ctx.Put(contractName, lastGatewayIdKey, []byte(fmt.Sprintf("%d", lastGatewayIdNum+1)))
	if err != nil {
		r.log.Errorf("RelayCrossRuntime.getNewGatewayId put last gateway id error: [%v]", err)
		return 0, err
	}
	return lastGatewayIdNum, nil
}

// getLastGatewayId 获取最后一个gateway id
//
//	@receiver r
//	@param ctx
//	@return int64
func (r *RelayCrossRuntime) getLastGatewayId(ctx protocol.TxSimContext) int64 {
	lastGatewayIdByte, err := r.get(ctx, lastGatewayIdKey)
	if err != nil {
		r.log.Warnf("RelayCrossRuntime.getLastGatewayId get last gateway id error: [%v]", err)
		lastGatewayIdByte = []byte("0")
	}
	lastGatewayIdNum, err := strconv.Atoi(string(lastGatewayIdByte))
	if err != nil {
		lastGatewayIdNum = 0
	}
	return int64(lastGatewayIdNum)
}

// getNewCrossId 获取新的跨链交易id
//
//	@receiver r
//	@param ctx
//	@return int64
//	@return error
func (r *RelayCrossRuntime) getNewCrossId(ctx protocol.TxSimContext) (string, error) {
	return ctx.GetTx().Payload.TxId, nil
	//lastCrossChainIdNum := r.getLastCrossId(ctx)
	//err := ctx.Put(contractName, lastCrossChainIdKey, []byte(fmt.Sprintf("%d", lastCrossChainIdNum+1)))
	//if err != nil {
	//	r.log.Errorf("RelayCrossRuntime.getNewCrossId put last cross chain id error: [%v]", err)
	//	return 0, err
	//}
	//return lastCrossChainIdNum, nil
}

// getLastCrossId 获取最后一个跨链交易id
//
//	@receiver r
//	@param ctx
//	@return int64
//func (r *RelayCrossRuntime) getLastCrossId(ctx protocol.TxSimContext) int64 {
//	lastCrossChainIdByte, err := r.get(ctx, lastCrossChainIdKey)
//	if err != nil {
//		r.log.Warnf("RelayCrossRuntime.getLastCrossId get last cross chain id error: [%v]", err)
//		lastCrossChainIdByte = []byte("0")
//	}
//	lastCrossChainIdNum, err := strconv.Atoi(string(lastCrossChainIdByte))
//	if err != nil {
//		lastCrossChainIdNum = 0
//	}
//	return int64(lastCrossChainIdNum)
//}

// getCrossChainInfo 获取跨链交易内容
//
//	@param crossChainId
//	@param ctx
//	@return *tcipcommon.CrossChainInfo
//	@return error
func getCrossChainInfo(crossChainId string, ctx protocol.TxSimContext) (*tcipcommon.CrossChainInfo, error) {
	crossChainIdNum := crossChainId

	crossChainInfoByte, err := ctx.Get(contractName, parseCrossChainKey(crossChainIdNum))
	if err != nil {
		return nil, errors.New("no such cross chain id:" + crossChainId)
	}
	crossChainInfoDecode, err := base64.StdEncoding.DecodeString(string(crossChainInfoByte))
	if err != nil {
		return nil, errors.New("decode cross chain error: " + err.Error())
	}
	var crossChainInfo tcipcommon.CrossChainInfo
	err = json.Unmarshal(crossChainInfoDecode, &crossChainInfo)
	if err != nil {
		return nil, errors.New("unmarshal cross chain error: " + err.Error())
	}
	return &crossChainInfo, nil
}

// getAdminAddr 获取管理员列表
//
//	@param ctx
//	@return map[string]bool
//	@return error
func getAdminAddr(ctx protocol.TxSimContext) (map[string]bool, error) {
	res, err := ctx.Get(contractName, admin)
	if err != nil {
		return nil, err
	}
	addressMap := make(map[string]bool)
	if res != nil {
		err1 := json.Unmarshal(res, &addressMap)
		if err1 != nil {
			return nil, err
		}
	}
	return addressMap, nil
}

// putAdminAddr 存储管理员列表
//
//	@param ctx
//	@return []byte
//	@return error
func putAdminAddr(ctx protocol.TxSimContext, addressMap map[string]bool) ([]byte, error) {
	addressString, _ := json.Marshal(addressMap)
	err := ctx.Put(contractName, admin, addressString)
	if err != nil {
		return addressString, err
	}
	return addressString, nil
}

func isCrossAdmin(ctx protocol.TxSimContext) (bool, error) {
	addressMap, err := getAdminAddr(ctx)
	if err != nil {
		return false, fmt.Errorf("getAdminAddr: [%v]", err)
	}
	senderPk, err := common.GetSenderPublicKey(ctx)
	if err != nil {
		return false, fmt.Errorf(" GetSenderPublicKey: [%v]", err)
	}
	cfg, err := common.GetChainConfigNoRecord(ctx)
	if err != nil {
		return false, err
	}
	senderAddress, err := common.PublicKeyToAddress(senderPk, cfg)
	if err != nil {
		return false, fmt.Errorf("PublicKeyToAddress: [%v]", err)
	}
	if _, ok := addressMap[senderAddress]; ok {
		return true, nil
	}
	return false, nil
}

func (r *RelayCrossRuntime) get(ctx protocol.TxSimContext, key []byte) ([]byte, error) {
	return ctx.Get(contractName, key)
}

func checkParams(params map[string][]byte, keys ...string) error {
	if params == nil {
		return fmt.Errorf("params is nil")
	}
	for _, key := range keys {
		if v, ok := params[key]; !ok {
			return fmt.Errorf("params has no such key: [%s]", key)
		} else if len(v) == 0 {
			return fmt.Errorf("param [%s] is invalid: value is nil", key)
		}
	}
	return nil
}

func parseGatewayKey(gatewayId int64) []byte {
	return []byte(fmt.Sprintf("g%019d", gatewayId))
}

func parseCrossChainKey(crossChainId string) []byte {
	if crossChainIdNum, err := strconv.Atoi(crossChainId); err == nil {
		return []byte(fmt.Sprintf("c%019d", crossChainIdNum))
	}
	var key []byte
	key = append(key, crossChainIdKey...)
	key = append(key, []byte(crossChainId)...)
	return key
}

func parseFailCrossChainIdKey(crossChainId string) []byte {
	return append(failedCrossChainId, parseCrossChainKey(crossChainId)...)
}

func parseNotEndCrossId(crossChainId string) []byte {
	var key []byte
	key = append(key, notEndCrossChainId...)
	key = append(key, []byte(crossChainId)...)
	return key
}
