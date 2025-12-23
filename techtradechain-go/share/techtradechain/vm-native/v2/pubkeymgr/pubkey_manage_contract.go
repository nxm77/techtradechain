/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Package pubkeymgr is package for pubkeymgr
package pubkeymgr

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"techtradechain.com/techtradechain/common/v2/msgbus"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"

	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
	"github.com/gogo/protobuf/proto"
	"github.com/mr-tron/base58"
)

const (
	paramNameRole   = "role"
	paramNamePubkey = "pubkey"
)

// PubkeyManageContract 公钥模式管理合约
type PubkeyManageContract struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewPubkeyManageContract 新建公钥管理合约
// @param log
// @return *PubkeyManageContract
func NewPubkeyManageContract(log protocol.Logger) *PubkeyManageContract {
	return &PubkeyManageContract{
		log:     log,
		methods: registerPubkeyManageContractMethods(log),
	}
}

// GetMethod get register method by name
func (c *PubkeyManageContract) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

func registerPubkeyManageContractMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	// pubkey manager
	pubkeyManageRuntime := &PubkeyManageRuntime{log: log}

	methodMap[syscontract.PubkeyManageFunction_PUBKEY_ADD.String()] = common.WrapEventResult(
		pubkeyManageRuntime.AddPubkey)
	methodMap[syscontract.PubkeyManageFunction_PUBKEY_DELETE.String()] = common.WrapEventResult(
		pubkeyManageRuntime.DeletePubkey)
	methodMap[syscontract.PubkeyManageFunction_PUBKEY_QUERY.String()] = common.WrapResultFunc(
		pubkeyManageRuntime.QueryPubkey)

	return methodMap
}

// PubkeyManageRuntime 公钥合约的运行时
type PubkeyManageRuntime struct {
	log protocol.Logger
}

// NewPubkeyManageRuntime 新建公钥合约运行时对象
// @param log
// @return *PubkeyManageRuntime
func NewPubkeyManageRuntime(log protocol.Logger) *PubkeyManageRuntime {
	return &PubkeyManageRuntime{log: log}
}

func pubkeyHash(pubkey []byte) string {
	pkHash := sha256.Sum256(pubkey)
	strPkHash := base58.Encode(pkHash[:])
	return strPkHash
}

// AddPubkey Add public key
func (r *PubkeyManageRuntime) AddPubkey(context protocol.TxSimContext, params map[string][]byte) (
	[]byte, []*commonPb.ContractEvent, error) {
	orgId := string(params[protocol.ConfigNameOrgId])
	if utils.IsAnyBlank(orgId) {
		err := fmt.Errorf("%s, param[org_id] of AddPubkey not found", common.ErrParams.Error())
		r.log.Errorf(err.Error())
		return nil, nil, err
	}

	role := string(params[paramNameRole])
	if utils.IsAnyBlank(role) {
		err := fmt.Errorf("%s, param[role] of AddPubkey not found", common.ErrParams.Error())
		r.log.Errorf(err.Error())
		return nil, nil, err
	}
	// check role
	upperRole := strings.ToUpper(role)
	if protocol.Role(upperRole) != protocol.RoleClient && protocol.Role(upperRole) != protocol.RoleLight &&
		protocol.Role(upperRole) != protocol.RoleCommonNode {
		err := fmt.Errorf("%s, illegal param[role] of AddPubkey: %s", common.ErrParams.Error(), role)
		r.log.Errorf(err.Error())
		return nil, nil, err
	}

	pubkey := string(params[paramNamePubkey])
	if utils.IsAnyBlank(pubkey) {
		err := fmt.Errorf("%s, param[pubkey] of AddPubkey not found", common.ErrParams.Error())
		r.log.Errorf(err.Error())
		return nil, nil, err
	}

	pk, err := asym.PublicKeyFromPEM([]byte(pubkey))
	if err != nil {
		r.log.Errorf(err.Error())
		return nil, nil, err
	}

	pkBytes, err := pk.Bytes()
	if err != nil {
		r.log.Errorf(err.Error())
		return nil, nil, err
	}

	pkHashKey := pubkeyHash(pkBytes)

	pkInfo := &accesscontrol.PKInfo{
		OrgId:   orgId,
		Role:    upperRole,
		PkBytes: pkBytes,
	}
	value, err := proto.Marshal(pkInfo)
	if err != nil {
		err = fmt.Errorf("marshal error in AddPubkey")
		r.log.Errorf(err.Error())
		return nil, nil, err
	}
	if err = context.Put(syscontract.SystemContract_PUBKEY_MANAGE.String(), []byte(pkHashKey), value); err != nil {
		r.log.Errorf("Put failed in AddPubkey, err: %s", err.Error())
		return nil, nil, err
	}
	cfg, err := common.GetChainConfigNoRecord(context)
	if err != nil {
		return nil, nil, err
	}
	event := []*commonPb.ContractEvent{
		{
			Topic:           strconv.Itoa(int(msgbus.PubkeyManageAdd)),
			TxId:            context.GetTx().Payload.TxId,
			ContractName:    syscontract.SystemContract_PUBKEY_MANAGE.String(),
			ContractVersion: cfg.Version,
			EventData:       []string{orgId, role, pubkey},
		},
	}
	r.log.Infof("pubkey add success")
	return []byte("Success"), event, nil
}

// DeletePubkey Delete pubkey
func (r *PubkeyManageRuntime) DeletePubkey(context protocol.TxSimContext, params map[string][]byte) (
	[]byte, []*commonPb.ContractEvent, error) {
	orgId := string(params[protocol.ConfigNameOrgId])
	if utils.IsAnyBlank(orgId) {
		err := fmt.Errorf("%s, param[org_id] of DeletePubkey not found", common.ErrParams.Error())
		r.log.Errorf(err.Error())
		return nil, nil, err
	}
	pubkey := string(params[paramNamePubkey])
	if utils.IsAnyBlank(pubkey) {
		err := fmt.Errorf("%s, param[pubkey] of DeletePubkey not found", common.ErrParams.Error())
		r.log.Errorf(err.Error())
		return nil, nil, err
	}
	pk, err := asym.PublicKeyFromPEM([]byte(pubkey))
	if err != nil {
		r.log.Errorf(err.Error())
		return nil, nil, err
	}

	pkBytes, err := pk.Bytes()
	if err != nil {
		r.log.Errorf(err.Error())
		return nil, nil, err
	}

	pkHashKey := pubkeyHash(pkBytes)
	bytes, err := context.Get(syscontract.SystemContract_PUBKEY_MANAGE.String(), []byte(pkHashKey))
	if err != nil {
		r.log.Errorf("DeletePubkey get pubkey failed, pubkey[%s], err: %s", pubkey, err.Error())
		return nil, nil, err
	}

	if len(bytes) == 0 {
		msg := fmt.Sprintf("DeletePubkey get pubkey failed, pubkey[%s], err: not exist", pubkey)
		r.log.Error(msg)
		return nil, nil, errors.New(msg)
	}
	r.log.Infof("pubkey exists")

	err = context.Del(syscontract.SystemContract_PUBKEY_MANAGE.String(), []byte(pkHashKey))
	if err != nil {
		r.log.Errorf("DeletePubkey for pubkey failed, pubkey[%s], err: %s", pubkey, err.Error())
		return nil, nil, err
	}
	cfg, err := common.GetChainConfigNoRecord(context)
	if err != nil {
		return nil, nil, err
	}
	event := []*commonPb.ContractEvent{
		{
			Topic:           strconv.Itoa(int(msgbus.PubkeyManageDelete)),
			TxId:            context.GetTx().Payload.TxId,
			ContractName:    syscontract.SystemContract_PUBKEY_MANAGE.String(),
			ContractVersion: cfg.Version,
			EventData:       []string{orgId, pubkey},
		},
	}
	r.log.Infof("pubkey delete success")
	return []byte("Success"), event, nil
}

// QueryPubkey Query public key
func (r *PubkeyManageRuntime) QueryPubkey(context protocol.TxSimContext, params map[string][]byte) ([]byte, error) {
	pubkey := string(params[paramNamePubkey])
	if utils.IsAnyBlank(pubkey) {
		err := fmt.Errorf("%s, param[pubkey] of QueryPubkey not found", common.ErrParams.Error())
		r.log.Errorf(err.Error())
		return nil, err
	}
	pk, err := asym.PublicKeyFromPEM([]byte(pubkey))
	if err != nil {
		r.log.Errorf(err.Error())
		return nil, err
	}

	pkBytes, err := pk.Bytes()
	if err != nil {
		r.log.Errorf(err.Error())
		return nil, err
	}

	pkHashKey := pubkeyHash(pkBytes)
	bytes, err := context.Get(syscontract.SystemContract_PUBKEY_MANAGE.String(), []byte(pkHashKey))
	if err != nil {
		r.log.Errorf("QueryPubkey get pubkey failed, pubkey[%s], err: %s", pubkey, err.Error())
		return nil, err
	}

	return bytes, nil
}
