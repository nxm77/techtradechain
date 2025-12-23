/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
)

const (
	emptyString  = ""
	zxAddrPrefix = "ZX"
)

// GetSenderPublicKey get tx sender public key
// @param txSimContext
// @return []byte public key
// @return error
func GetSenderPublicKey(txSimContext protocol.TxSimContext) ([]byte, error) {
	var err error
	var pk []byte
	sender := txSimContext.GetSender()
	if sender == nil {
		err = errors.New(" can not find sender from tx ")
		return nil, err
	}

	switch sender.MemberType {
	case accesscontrol.MemberType_CERT:
		pk, err = publicKeyFromCert(sender.MemberInfo)
		if err != nil {
			return nil, err
		}
	case accesscontrol.MemberType_CERT_HASH:
		var certInfo *commonPb.CertInfo
		infoHex := hex.EncodeToString(sender.MemberInfo)
		if certInfo, err = wholeCertInfo(txSimContext, infoHex); err != nil {
			return nil, fmt.Errorf(" can not load the whole cert info,member[%s],reason: %s", infoHex, err)
		}

		if pk, err = publicKeyFromCert(certInfo.Cert); err != nil {
			return nil, err
		}

	case accesscontrol.MemberType_PUBLIC_KEY:
		pk = sender.MemberInfo
	default:
		err = fmt.Errorf("invalid member type: %s", sender.MemberType)
		return nil, err
	}

	return pk, nil
}

// publicKeyFromCert get public byte from cert
// @param crtPEM
// @return []byte
// @return error
func publicKeyFromCert(crtPEM []byte) ([]byte, error) {
	certificate, err := utils.ParseCert(crtPEM)
	if err != nil {
		return nil, err
	}
	pubKeyBytes, err := certificate.PublicKey.String()
	if err != nil {
		return nil, err
	}
	return []byte(pubKeyBytes), nil
}

// wholeCertInfo get cert all info
// @param txSimContext
// @param certHash
// @return *commonPb.CertInfo
// @return error
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

// PublicKeyToAddress publicKey to address
// @param publicKey
// @param chainCfg chain config
// @return string  as "760352ae678c378a86e03ccad56c3d2d712134ed" "0X760352ae678c378a86e03ccad56c3d2d712134ed"
// @error
func PublicKeyToAddress(publicKey []byte, chainCfg *configPb.ChainConfig) (string, error) {
	pk, err := asym.PublicKeyFromPEM(publicKey)
	if err != nil {
		return "", err
	}

	publicKeyString, err := utils.PkToAddrStr(pk, chainCfg.Vm.AddrType, crypto.HashAlgoMap[chainCfg.Crypto.Hash])
	if err != nil {
		return emptyString, err
	}

	if chainCfg.Vm.AddrType == configPb.AddrType_ZXL {
		publicKeyString = "ZX" + publicKeyString
	}
	return publicKeyString, nil
}

// VerifyAndToLowerAddress by TxSimContext
// @param context
// @param address as "A60352ae678c378a86e03ccad56c3d2d712134ed" "0X760352ae678c378a86e03ccad56c3d2d712134ed"
// @return string  as "a60352ae678c378a86e03ccad56c3d2d712134ed" "0x760352ae678c378a86e03ccad56c3d2d712134ed"
// @error
func VerifyAndToLowerAddress(context protocol.TxSimContext, address string) (string, bool) {

	chainCfg, _ := context.GetBlockchainStore().GetLastChainConfig()
	if chainCfg.Vm.AddrType == configPb.AddrType_ZXL || context.GetBlockVersion() < 2218 {
		if len(address) != 42 || address[:2] != zxAddrPrefix {
			return emptyString, false
		}

		return address[:2] + strings.ToLower(address[2:]), true
	}

	if !utils.CheckEvmAddressFormat(address) {
		return emptyString, false
	}

	return strings.ToLower(address), true
}

// VerifyAndToLowerAddress2 by blockVersion addrType
// @param blockVersion
// @param addrType as "A60352ae678c378a86e03ccad56c3d2d712134ed" "760352ae678c378a86e03ccad56c3d2d712134ed"
// @return address  as "a60352ae678c378a86e03ccad56c3d2d712134ed" "760352ae678c378a86e03ccad56c3d2d712134ed"
// @bool is valid address
func VerifyAndToLowerAddress2(blockVersion uint32, addrType configPb.AddrType, address string) (string, bool) {
	if blockVersion < 2300 {
		if len(address) != 42 || address[:2] != zxAddrPrefix {
			return emptyString, false
		}

		return address[:2] + strings.ToLower(address[2:]), true
	}
	if addrType == configPb.AddrType_ZXL {
		if len(address) != 42 || address[:2] != zxAddrPrefix {
			return emptyString, false
		}

		return address[:2] + strings.ToLower(address[2:]), true
	}
	if !utils.CheckEvmAddressFormat(address) {
		return emptyString, false
	}

	return strings.ToLower(address), true
}
