/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package accountmgr220

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
	"techtradechain.com/techtradechain/vm-native/v2/v220/chainconfigmgr220"
)

const (
	// AccountPrefix comment at next version
	AccountPrefix = "__account_prefix__"
	// FrozenPrefix comment at next version
	FrozenPrefix = "__frozen_account__"
	// AddressKey comment at next version
	AddressKey = "address_key"
	// BatchRecharge comment at next version
	BatchRecharge = "batch_recharge"
	// RechargeKey comment at next version
	RechargeKey = "recharge_key"
	// RechargeAmountKey comment at next version
	RechargeAmountKey = "recharge_amount_key"
	// ChargePublicKey comment at next version
	ChargePublicKey = "charge_public_key"
	// ChargeGasAmount comment at next version
	ChargeGasAmount = "charge_gas_amount"
	// Success comment at next version
	Success = "success"
	// AccountPrefix comment at next version
	emptyString    = ""
	addressIllegal = "account address is illegal"
	int64OverFlow  = "int64 is overflow"
	unlock         = "0"
	locked         = "1"
)

// AccountManager comment at next version
type AccountManager struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewAccountManager comment at next version
func NewAccountManager(log protocol.Logger) *AccountManager {
	return &AccountManager{
		log:     log,
		methods: registerGasAccountContractMethods(log),
	}
}

// GetMethod comment at next version
func (g *AccountManager) GetMethod(methodName string) common.ContractFunc {
	return g.methods[methodName]
}

func registerGasAccountContractMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	gasAccountRuntime := &AccountManagerRuntime{log: log}

	methodMap[syscontract.GasAccountFunction_SET_ADMIN.String()] = common.WrapResultFunc(
		gasAccountRuntime.SetAdmin)
	methodMap[syscontract.GasAccountFunction_GET_ADMIN.String()] = common.WrapResultFunc(
		gasAccountRuntime.GetAdmin)
	methodMap[syscontract.GasAccountFunction_RECHARGE_GAS.String()] = common.WrapResultFunc(
		gasAccountRuntime.RechargeGas)
	methodMap[syscontract.GasAccountFunction_CHARGE_GAS.String()] = common.WrapResultFunc(
		gasAccountRuntime.ChargeGasVm)
	methodMap[syscontract.GasAccountFunction_GET_BALANCE.String()] = common.WrapResultFunc(
		gasAccountRuntime.GetBalance)
	methodMap[syscontract.GasAccountFunction_REFUND_GAS.String()] = common.WrapResultFunc(
		gasAccountRuntime.RefundGas)
	methodMap[syscontract.GasAccountFunction_REFUND_GAS_VM.String()] = common.WrapResultFunc(
		gasAccountRuntime.RefundGasVm)
	methodMap[syscontract.GasAccountFunction_FROZEN_ACCOUNT.String()] = common.WrapResultFunc(
		gasAccountRuntime.FrozenAccount)
	methodMap[syscontract.GasAccountFunction_UNFROZEN_ACCOUNT.String()] = common.WrapResultFunc(
		gasAccountRuntime.UnFrozenAccount)
	methodMap[syscontract.GasAccountFunction_ACCOUNT_STATUS.String()] = common.WrapResultFunc(
		gasAccountRuntime.GetAccountStatus)
	methodMap[syscontract.GasAccountFunction_CHARGE_GAS_FOR_MULTI_ACCOUNT.String()] = common.WrapResultFunc(
		gasAccountRuntime.ChargeGasVmForMultiAccount)

	return methodMap
}

// AccountManagerRuntime comment at next version
type AccountManagerRuntime struct {
	log protocol.Logger
}

// SetAdmin comment at next version
func (g *AccountManagerRuntime) SetAdmin(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	addressBytes, ok := params[AddressKey]
	if !ok {
		err := fmt.Errorf(" params key %s not exist ", AddressKey)
		g.log.Error(err.Error())
		return nil, err
	}

	if utils.IsAnyBlank(addressBytes) {
		err := fmt.Errorf(" %s, param[public_key]=%s,", common.ErrParams.Error(), addressBytes)
		g.log.Errorf(err.Error())
		return nil, err
	}

	address := string(addressBytes)
	if address, ok = g.verifyAddress(address); !ok {
		err := errors.New(addressIllegal)
		g.log.Error(err.Error())
		return nil, err
	}

	return g.setAdmin(txSimContext, address)
}

func (g *AccountManagerRuntime) setAdmin(txSimContext protocol.TxSimContext, address string) ([]byte, error) {
	var err error
	var chainConfig *configPb.ChainConfig
	var result []byte

	chainConfig, err = chainconfigmgr220.GetChainConfigEmptyParams(txSimContext)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if chainConfig.AccountConfig != nil {
		chainConfig.AccountConfig.GasAdminAddress = address
	} else {
		chainConfig.AccountConfig = &configPb.GasAccountConfig{
			GasAdminAddress: address,
		}
	}

	result, err = chainconfigmgr220.SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}
	return result, nil
}

// GetAdmin comment at next version
func (g *AccountManagerRuntime) GetAdmin(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	adminPublicKey, err := g.getAdmin(txSimContext)
	if err != nil {
		g.log.Error(err.Error())
		return nil, errors.New(" not set gas admin account")
	}
	return adminPublicKey, nil
}

func (g *AccountManagerRuntime) getAdmin(txSimContext protocol.TxSimContext) ([]byte, error) {
	var err error
	var chainConfig *configPb.ChainConfig
	chainConfig, err = chainconfigmgr220.GetChainConfigEmptyParams(txSimContext)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if chainConfig.AccountConfig == nil {
		return nil, errors.New("chain config account config is empty ")
	}

	if _, ok := g.verifyAddress(chainConfig.AccountConfig.GasAdminAddress); !ok {
		return nil, errors.New(" gas admin address is illegal")
	}

	return []byte(chainConfig.AccountConfig.GasAdminAddress), nil
}

// RechargeGas comment at next version
func (g *AccountManagerRuntime) RechargeGas(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	var (
		gasBalance int64
		err        error
		publicKey  []byte
	)
	publicKey, err = g.getSenderPublicKey(txSimContext)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if !g.checkAdmin(txSimContext, publicKey) {
		err = errors.New(" verify admin failed ")
		g.log.Debug(err.Error())
		return nil, err
	}

	batchRechargeBytes, ok := params[BatchRecharge]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", BatchRecharge)
		g.log.Error(err.Error())
		return nil, err
	}

	if utils.IsAnyBlank(batchRechargeBytes) {
		err = fmt.Errorf(" %s, param[%s]=%s",
			common.ErrParams.Error(),
			BatchRecharge,
			batchRechargeBytes,
		)
		g.log.Errorf(err.Error())
		return nil, err
	}

	rechargeGasReq := &syscontract.RechargeGasReq{}
	if err = rechargeGasReq.Unmarshal(batchRechargeBytes); err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	for i, recharge := range rechargeGasReq.BatchRechargeGas {
		address := recharge.Address
		if address, ok = g.verifyAddress(address); !ok {
			err = errors.New(addressIllegal)
			g.log.Error(err.Error())
			return nil, err
		}

		if err = g.checkAmount(recharge.GasAmount); err != nil {
			g.log.Error(err.Error())
			return nil, err
		}

		accountKey := AccountPrefix + address
		gasBalance, err = g.getAccountBalance(txSimContext, accountKey)
		if err != nil {
			err = fmt.Errorf(" batch index [%v]  error is: %s", i, err.Error())
			g.log.Error(err.Error())
			return nil, err
		}

		updateAmount := gasBalance + recharge.GasAmount
		if err = g.checkOverFlow(updateAmount); err != nil {
			g.log.Error(err.Error())
			return nil, err
		}

		if err = txSimContext.Put(syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(accountKey), []byte(
			strconv.FormatInt(updateAmount, 10))); err != nil {
			err = fmt.Errorf(" batch charge index [%v]  error is: %s", i, err.Error())
			g.log.Error(err.Error())
			return nil, err
		}
	}

	return []byte(Success), nil
}

// RefundGasVm comment at next version
func (g *AccountManagerRuntime) RefundGasVm(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	var (
		gasBalance        int64
		rechargeGasAmount int64
		err               error
		address           string
	)

	rechargeKeyBytes, ok := params[RechargeKey]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", BatchRecharge)
		g.log.Error(err.Error())
		return nil, err
	}

	rechargeAmountKeyByte, ok := params[RechargeAmountKey]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", RechargeAmountKey)
		g.log.Error(err.Error())
		return nil, err
	}

	if utils.IsAnyBlank(rechargeKeyBytes, rechargeAmountKeyByte) {
		err = fmt.Errorf(" %s, param[%s]=%s,param[%s]=%s",
			common.ErrParams.Error(),
			RechargeKey,
			rechargeKeyBytes,
			RechargeAmountKey,
			rechargeAmountKeyByte,
		)
		g.log.Errorf(err.Error())
		return nil, err
	}

	rechargeGasAmount, err = strconv.ParseInt(string(rechargeAmountKeyByte), 10, 0)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if err = g.checkAmount(rechargeGasAmount); err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	address, err = publicKeyToAddress(rechargeKeyBytes)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	accountKey := AccountPrefix + address
	gasBalance, err = g.getAccountBalance(txSimContext, accountKey)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	updateAmount := gasBalance + rechargeGasAmount
	if err = g.checkOverFlow(updateAmount); err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if err = txSimContext.Put(syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(accountKey), []byte(
		strconv.FormatInt(updateAmount, 10))); err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	return []byte(Success), nil
}

// ChargeGasVm charge gas for vm and  must set config auth multi sign
func (g *AccountManagerRuntime) ChargeGasVm(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	var err error
	var address string
	chargeGasPublicKey, ok := params[ChargePublicKey]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", ChargePublicKey)
		g.log.Error(err.Error())
		return nil, err
	}

	chargeGasAmountBytes, ok := params[ChargeGasAmount]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", ChargeGasAmount)
		g.log.Error(err.Error())
		return nil, err
	}

	if utils.IsAnyBlank(chargeGasPublicKey, chargeGasAmountBytes) {
		err = fmt.Errorf(" %s, param[%s]=%s,param[%s]=%s",
			common.ErrParams.Error(),
			ChargePublicKey,
			chargeGasPublicKey,
			ChargeGasAmount,
			chargeGasAmountBytes,
		)
		g.log.Errorf(err.Error())
		return nil, err
	}

	address, err = publicKeyToAddress(chargeGasPublicKey)
	if err != nil {
		g.log.Error(err)
		return nil, err
	}
	return g.chargeGas(txSimContext, address, chargeGasAmountBytes)
}

// ChargeGasVmForMultiAccount comment at next version
func (g *AccountManagerRuntime) ChargeGasVmForMultiAccount(
	txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	hasError := false
	errAddresses := make([]string, 0)
	for address, chargeGasAmountBytes := range params {
		chargeGasAmount, err := strconv.ParseInt(string(chargeGasAmountBytes), 10, 64)
		if err != nil {
			hasError = true
			errAddresses = append(errAddresses, address)
			g.log.Error(err.Error())
		} else if _, err := g.chargeGasForMultiAccount(txSimContext, address, chargeGasAmount); err != nil {
			hasError = true
			errAddresses = append(errAddresses, address)
			g.log.Error(err.Error())
		}
	}

	if hasError {
		return nil, fmt.Errorf("charge accounts error: %v", errAddresses)
	}

	return []byte(Success), nil
}

func (g *AccountManagerRuntime) chargeGasForMultiAccount(
	txSimContext protocol.TxSimContext,
	address string,
	chargeGasAmount int64) ([]byte, error) {
	var (
		gasBalance   int64
		err          error
		updateAmount int64
	)

	accountKey := AccountPrefix + address
	gasBalance, err = g.getAccountBalance(txSimContext, accountKey)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}
	if gasBalance < int64(0) {
		err = fmt.Errorf("the balance of `%s` is less than 0", address)
		g.log.Error(err)
		return nil, err
	}

	if err = g.checkAmount(chargeGasAmount); err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if updateAmount = gasBalance - chargeGasAmount; updateAmount < 0 {
		err = fmt.Errorf("please check [addr:%s] balance, gasBalance[%d] < chargeGasAmount[%d]",
			address, gasBalance, chargeGasAmount)
		g.log.Error(err.Error())
		return nil, err
	}

	if err = txSimContext.Put(syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(accountKey), []byte(
		strconv.FormatInt(updateAmount, 10))); err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	return []byte(Success), nil
}

// GetBalance comment at next version
func (g *AccountManagerRuntime) GetBalance(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	var err error
	var gasBalance []byte
	addressBytes, ok := params[AddressKey]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", AddressKey)
		g.log.Error(err.Error())
		return nil, err
	}

	if utils.IsAnyBlank(addressBytes) {
		err = fmt.Errorf(" %s, param[%s]=%s", common.ErrParams.Error(), AddressKey, addressBytes)
		g.log.Errorf(err.Error())
		return nil, err
	}

	address := string(addressBytes)
	if address, ok = g.verifyAddress(address); !ok {
		err = errors.New(addressIllegal)
		g.log.Error(err.Error())
		return nil, err
	}

	gasBalance, err = txSimContext.Get(syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		[]byte(AccountPrefix+address))
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if len(gasBalance) == 0 {
		return []byte("0"), err
	}

	return gasBalance, nil
}

// RefundGas refund gas for sdk
func (g *AccountManagerRuntime) RefundGas(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	var publicKey []byte
	var err error
	addressBytes, ok := params[AddressKey]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", AddressKey)
		g.log.Error(err.Error())
		return nil, err
	}

	address := string(addressBytes)
	if address, ok = g.verifyAddress(address); !ok {
		err = errors.New(addressIllegal)
		g.log.Error(err.Error())
		return nil, err
	}

	chargeGasAmountBytes, ok := params[ChargeGasAmount]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", ChargeGasAmount)
		g.log.Error(err.Error())
		return nil, err
	}

	if utils.IsAnyBlank(address, chargeGasAmountBytes) {
		err = fmt.Errorf(" %s, param[%s]=%s,param[%s]=%s",
			common.ErrParams.Error(),
			AddressKey,
			addressBytes,
			ChargeGasAmount,
			chargeGasAmountBytes,
		)
		g.log.Errorf(err.Error())
		return nil, err
	}

	publicKey, err = g.getSenderPublicKey(txSimContext)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if !g.checkAdmin(txSimContext, publicKey) {
		err = errors.New(" verify admin failed ")
		g.log.Error(err.Error())
		return nil, err
	}
	return g.chargeGas(txSimContext, address, chargeGasAmountBytes)

}

func (g *AccountManagerRuntime) chargeGas(txSimContext protocol.TxSimContext, address string,
	chargeGasAmountBytes []byte) ([]byte, error) {
	var (
		gasBalance      int64
		err             error
		chargeGasAmount int64
		updateAmount    int64
	)

	if g.checkFrozen(txSimContext, address) {
		err = fmt.Errorf(" %s accout is frozened", address)
		g.log.Error(err.Error())
		return nil, err
	}

	accountKey := AccountPrefix + address
	gasBalance, err = g.getAccountBalance(txSimContext, accountKey)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	chargeGasAmount, err = strconv.ParseInt(string(chargeGasAmountBytes), 10, 64)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if err = g.checkAmount(chargeGasAmount); err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if updateAmount = gasBalance - chargeGasAmount; updateAmount < 0 {
		err = fmt.Errorf("please check [addr:%s] balance, gasBalance[%d] < chargeGasAmount[%d]",
			address, gasBalance, chargeGasAmount)
		g.log.Error(err.Error())
		return nil, err
	}

	if err = txSimContext.Put(syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(accountKey), []byte(
		strconv.FormatInt(updateAmount, 10))); err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	return []byte(Success), nil
}

// FrozenAccount comment at next version
func (g *AccountManagerRuntime) FrozenAccount(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	var err error
	var publicKey []byte
	var address string
	publicKey, err = g.getSenderPublicKey(txSimContext)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if !g.checkAdmin(txSimContext, publicKey) {
		err = errors.New(" verify admin failed ")
		g.log.Error(err.Error())
		return nil, err
	}

	addressBytes, ok := params[AddressKey]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", AddressKey)
		g.log.Error(err.Error())
		return nil, err
	}

	if utils.IsAnyBlank(addressBytes) {
		err = fmt.Errorf(" %s, param[%s]=%s", common.ErrParams.Error(), AddressKey, addressBytes)
		g.log.Errorf(err.Error())
		return nil, err
	}

	address = string(addressBytes)
	if address, ok = g.verifyAddress(address); !ok {
		err = errors.New(addressIllegal)
		g.log.Error(err.Error())
		return nil, err
	}

	frozenPublicKey := FrozenPrefix + address
	if err = txSimContext.Put(syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(frozenPublicKey),
		[]byte(locked)); err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	return []byte(Success), nil

}

// UnFrozenAccount comment at next version
func (g *AccountManagerRuntime) UnFrozenAccount(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	var err error
	var publicKey []byte
	var address string
	publicKey, err = g.getSenderPublicKey(txSimContext)
	if err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	if !g.checkAdmin(txSimContext, publicKey) {
		err = errors.New(" verify admin failed ")
		g.log.Error(err.Error())
		return nil, err
	}

	addressBytes, ok := params[AddressKey]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", AddressKey)
		g.log.Error(err.Error())
		return nil, err
	}

	if utils.IsAnyBlank(addressBytes) {
		err = fmt.Errorf(" %s, param[%s]=%s", common.ErrParams.Error(), AddressKey, addressBytes)
		g.log.Errorf(err.Error())
		return nil, err
	}

	address = string(addressBytes)
	if address, ok = g.verifyAddress(address); !ok {
		err = errors.New(addressIllegal)
		g.log.Error(err.Error())
		return nil, err
	}

	if frozen := g.checkFrozen(txSimContext, address); !frozen {
		err = fmt.Errorf("account %v not frozen", address)
		g.log.Errorf(err.Error())
		return nil, err
	}

	frozenPublicKey := FrozenPrefix + address
	if err = txSimContext.Put(syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(frozenPublicKey),
		[]byte(unlock)); err != nil {
		g.log.Error(err.Error())
		return nil, err
	}

	return []byte(Success), nil

}

// GetAccountStatus comment at next version
func (g *AccountManagerRuntime) GetAccountStatus(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	var err error
	var address string
	addressBytes, ok := params[AddressKey]
	if !ok {
		err = fmt.Errorf(" params key %s not exist ", AddressKey)
		g.log.Error(err.Error())
		return nil, err
	}

	if utils.IsAnyBlank(addressBytes) {
		err = fmt.Errorf(" %s, param[%s]=%s", common.ErrParams.Error(), AddressKey, addressBytes)
		g.log.Errorf(err.Error())
		return nil, err
	}

	address = string(addressBytes)
	if address, ok = g.verifyAddress(address); !ok {
		err = errors.New(addressIllegal)
		g.log.Error(err.Error())
		return nil, err
	}

	if g.checkFrozen(txSimContext, address) {
		return []byte(locked), nil
	}

	return []byte(unlock), nil

}

func (g *AccountManagerRuntime) checkFrozen(txSimContext protocol.TxSimContext, address string) bool {
	var (
		err         error
		frozenBytes []byte
	)

	frozenBytes, err = txSimContext.Get(syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(FrozenPrefix+address))
	if err != nil {
		g.log.Error(err.Error())
		return false
	}

	if string(frozenBytes) != locked {
		return false
	}
	return true
}

func (g *AccountManagerRuntime) checkAdmin(txSimContext protocol.TxSimContext, userPublicKey []byte) bool {
	adminPublicKeyBytes, err := g.getAdmin(txSimContext)
	if err != nil {
		g.log.Error(err.Error())
		return false
	}

	publicKeyString, err := publicKeyToAddress(userPublicKey)
	if err != nil {
		g.log.Error(err.Error())
		return false
	}

	g.log.Debugf("【220】adminPublicKeyBytes = %s, publicKeyString = %s", string(adminPublicKeyBytes), publicKeyString)
	if string(adminPublicKeyBytes) != publicKeyString {
		return false
	}

	return true
}

func (g *AccountManagerRuntime) getSenderPublicKey(txSimContext protocol.TxSimContext) ([]byte, error) {
	var err error
	var pk []byte
	sender := txSimContext.GetSender()
	if sender == nil {
		err = errors.New(" can not find sender from tx ")
		g.log.Error(err.Error())
		return nil, err
	}

	switch sender.MemberType {
	case accesscontrol.MemberType_CERT:
		pk, err = publicKeyFromCert(sender.MemberInfo)
		if err != nil {
			g.log.Error(err.Error())
			return nil, err
		}
	case accesscontrol.MemberType_CERT_HASH:
		var certInfo *commonPb.CertInfo
		infoHex := hex.EncodeToString(sender.MemberInfo)
		if certInfo, err = wholeCertInfo(txSimContext, infoHex); err != nil {
			g.log.Error(err.Error())
			return nil, fmt.Errorf(" can not load the whole cert info,member[%s],reason: %s", infoHex, err)
		}

		if pk, err = publicKeyFromCert(certInfo.Cert); err != nil {
			g.log.Error(err.Error())
			return nil, err
		}

	case accesscontrol.MemberType_PUBLIC_KEY:
		pk = sender.MemberInfo
	default:
		err = fmt.Errorf("invalid member type: %s", sender.MemberType)
		g.log.Error(err.Error())
		return nil, err
	}

	return pk, nil
}

func (g *AccountManagerRuntime) getAccountBalance(txSimContext protocol.TxSimContext,
	accountKey string) (int64, error) {

	var gasBalance int64
	gas, err := txSimContext.Get(syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(accountKey))
	if err != nil {
		g.log.Error(err.Error())
		return 0, nil
	}

	if len(gas) == 0 {
		return 0, nil
	}

	gasBalance, err = strconv.ParseInt(string(gas), 10, 64)
	if err != nil {
		g.log.Error(err.Error())
		return 0, err
	}

	if gasBalance < 0 {
		err = errors.New(" gas balance less than zero ")
		g.log.Error(err.Error())
		return 0, err
	}
	return gasBalance, nil
}

func (g *AccountManagerRuntime) verifyAddress(address string) (string, bool) {
	g.log.Infof("verify account address is:%v", address)
	if len(address) != 42 {
		return emptyString, false
	}

	if address[:2] != "ZX" {
		return emptyString, false
	}

	return address[:2] + strings.ToLower(address[2:]), true
}

func (g *AccountManagerRuntime) checkAmount(amount int64) error {
	if amount < 0 {
		g.log.Errorf("amount is %v", amount)
		return errors.New("amount must >= 0")
	}
	return nil
}

func (g *AccountManagerRuntime) checkOverFlow(amount int64) error {
	if amount < 0 {
		g.log.Error(int64OverFlow)
		return errors.New(int64OverFlow)
	}
	return nil
}

func publicKeyToAddress(publicKey []byte) (string, error) {
	pk, err := asym.PublicKeyFromPEM(publicKey)
	if err != nil {
		return "", err
	}

	publicKeyString, err := evmutils.ZXAddressFromPublicKey(pk)
	if err != nil {
		return emptyString, err
	}
	return publicKeyString, nil
}

func publicKeyFromCert(member []byte) ([]byte, error) {
	certificate, err := utils.ParseCert(member)
	if err != nil {
		return nil, err
	}
	pubKeyBytes, err := certificate.PublicKey.String()
	if err != nil {
		return nil, err
	}
	return []byte(pubKeyBytes), nil
}

func wholeCertInfo(txSimContext protocol.TxSimContext, certHash string) (*commonPb.CertInfo, error) {
	certBytes, err := txSimContext.Get(syscontract.SystemContract_CERT_MANAGE.String(), []byte(certHash))
	if err != nil {
		return nil, err
	}

	return &commonPb.CertInfo{
		Hash: certHash,
		Cert: certBytes,
	}, nil
}
