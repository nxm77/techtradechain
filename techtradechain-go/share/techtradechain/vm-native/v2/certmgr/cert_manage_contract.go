/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Package certmgr is package for cert
package certmgr

import (
	"bytes"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"techtradechain.com/techtradechain/common/v2/msgbus"

	bcx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	pbac "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
	"github.com/gogo/protobuf/proto"
)

const (
	paramNameCertHashes = "cert_hashes"
	paramNameCerts      = "certs"
	paramNameCertCrl    = "cert_crl"

	paramNameAlias   = "alias"
	paramNameCert    = "cert"
	paramNameAliases = "aliases"
	maxHisCertsLen   = 10
)

var (
	certManageContractName = syscontract.SystemContract_CERT_MANAGE.String()
	certAliasKey           = "cert_alias#"
	aliasNameRegStr        = "^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$"
	aliasNameReg           = regexp.MustCompile(aliasNameRegStr)
)

// CertManageContract 证书管理合约
// 包括：证书哈希管理、证书别名管理
type CertManageContract struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewCertManageContract create a new instance
// @param log
// @return *CertManageContract
func NewCertManageContract(log protocol.Logger) *CertManageContract {
	return &CertManageContract{
		log:     log,
		methods: registerCertManageContractMethods(log),
	}
}

// GetMethod get register method by name
func (c *CertManageContract) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

// registerCertManageContractMethods all of the cert management method are here
func registerCertManageContractMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	// cert manager
	certManageRuntime := &CertManageRuntime{log: log}

	// cert hash manage
	methodMap[syscontract.CertManageFunction_CERT_ADD.String()] = common.WrapResultFunc(
		certManageRuntime.Add)
	methodMap[syscontract.CertManageFunction_CERTS_DELETE.String()] = common.WrapEventResult(
		certManageRuntime.Delete)
	methodMap[syscontract.CertManageFunction_CERTS_FREEZE.String()] = common.WrapEventResult(
		certManageRuntime.Freeze)
	methodMap[syscontract.CertManageFunction_CERTS_UNFREEZE.String()] = common.WrapEventResult(
		certManageRuntime.Unfreeze)
	methodMap[syscontract.CertManageFunction_CERTS_REVOKE.String()] = common.WrapEventResult(
		certManageRuntime.Revoke)

	// alias manage
	methodMap[syscontract.CertManageFunction_CERT_ALIAS_ADD.String()] = common.WrapResultFunc(
		certManageRuntime.AddAlias)
	methodMap[syscontract.CertManageFunction_CERT_ALIAS_UPDATE.String()] = common.WrapEventResult(
		certManageRuntime.UpdateAlias)
	methodMap[syscontract.CertManageFunction_CERTS_ALIAS_DELETE.String()] = common.WrapEventResult(
		certManageRuntime.DeleteAlias)

	// query
	methodMap[syscontract.CertManageFunction_CERTS_QUERY.String()] = common.WrapResultFunc(
		certManageRuntime.Query)
	methodMap[syscontract.CertManageFunction_CERTS_ALIAS_QUERY.String()] = common.WrapResultFunc(
		certManageRuntime.QueryAlias)
	return methodMap
}

// CertManageRuntime runtime for contract
type CertManageRuntime struct {
	log protocol.Logger
}

// Add cert add
// @param none param
// one is the certificate itself
// the other is hash
// @return certHash
func (r *CertManageRuntime) Add(txSimContext protocol.TxSimContext, _ map[string][]byte) (
	result []byte, err error) {

	memberInfo := txSimContext.GetTx().Sender.Signer.GetMemberInfo()

	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		r.log.Warnf("txSimContext.GetAccessControl failed, err: %s", err.Error())
		return nil, err
	}

	hashType := ac.GetHashAlg()
	certHash, err := utils.GetCertificateIdHex(memberInfo, hashType)
	if err != nil {
		r.log.Warnf("get certHash failed, err: %s", err.Error())
		return nil, err
	}

	err = txSimContext.Put(certManageContractName, []byte(certHash), memberInfo)
	if err != nil {
		r.log.Warnf("certManage add cert failed, err: %s", err.Error())
		return nil, err
	}

	r.log.Infof("certManage add cert success."+
		" certHash[%s] memberInfo[%s] hashType[%s]", certHash, string(memberInfo), hashType)
	return []byte(certHash), nil
}

// Delete cert delete
// @param cert_hashes is cert hash list string, separate by comma
// @return string for "Success"
func (r *CertManageRuntime) Delete(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, event []*commonPb.ContractEvent, err error) {

	// verify params
	certHashesStr := string(params[paramNameCertHashes])

	if utils.IsAnyBlank(certHashesStr) {
		err = fmt.Errorf("%s, delete cert require param [%s] not found", common.ErrParams.Error(), paramNameCertHashes)
		r.log.Warn(err)
		return nil, nil, err
	}

	certHashes := strings.Split(certHashesStr, ",")
	for _, certHash := range certHashes {
		bytes, err1 := txSimContext.Get(certManageContractName, []byte(certHash))
		if err1 != nil {
			r.log.Warnf("certManage delete the certHash failed, certHash[%s], err: %s", certHash, err1.Error())
			return nil, nil, err1
		}

		if len(bytes) == 0 {
			msg := fmt.Sprintf(
				"certManage delete the certHash failed, certHash[%s], err: certHash is not exist", certHash)
			r.log.Warnf(msg)
			return nil, nil, errors.New(msg)
		}

		err = txSimContext.Del(certManageContractName, []byte(certHash))
		if err != nil {
			r.log.Warnf("certManage txSimContext.Del failed, certHash[%s] err: %s", certHash, err.Error())
			return nil, nil, err
		}
	}
	cfg, err := common.GetChainConfigNoRecord(txSimContext)
	if err != nil {
		return nil, nil, err
	}
	event = append(event, &commonPb.ContractEvent{
		Topic:           strconv.Itoa(int(msgbus.CertManageCertsDelete)),
		TxId:            txSimContext.GetTx().Payload.TxId,
		ContractName:    syscontract.SystemContract_CERT_MANAGE.String(),
		ContractVersion: cfg.Version,
		EventData:       []string{certHashesStr},
	})

	r.log.Infof("certManage delete success certHashes[%s]", certHashesStr)
	return []byte("Success"), event, nil
}

// Query certs query
// @param cert_hashes is cert hash list string, separate by comma
// @return cert_hash seeing the name of a thing one thinks of its function
// @return cert itself
func (r *CertManageRuntime) Query(txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte, error) {

	// verify params
	certHashesStr := string(params[paramNameCertHashes])

	if utils.IsAnyBlank(certHashesStr) {
		err := fmt.Errorf("%s, query cert require param [%s] not found", common.ErrParams.Error(), paramNameCertHashes)
		r.log.Warn(err)
		return nil, err
	}

	certHashes := strings.Split(certHashesStr, ",")
	certInfos := make([]*commonPb.CertInfo, len(certHashes))
	for i, certHash := range certHashes {
		certBytes, err := txSimContext.Get(certManageContractName, []byte(certHash))
		if err != nil {
			r.log.Warnf("certManage delete the certHash failed, certHash[%s] err: %s", certHash, err.Error())
			return nil, err
		}

		certInfos[i] = &commonPb.CertInfo{
			Hash: certHash,
			Cert: certBytes,
		}
	}

	c := &commonPb.CertInfos{}
	c.CertInfos = certInfos
	certBytes, err := proto.Marshal(c)
	if err != nil {
		r.log.Warnf("certManage query proto.Marshal(c) err certHash[%s] err", certHashesStr, err)
		return nil, err
	}

	r.log.Infof("certManage query success certHashes[%s]", certHashesStr)
	return certBytes, nil
}

// Freeze certs
// @param certs is original certificate list string, separate by comma
// @result cert_hash
func (r *CertManageRuntime) Freeze(txSimContext protocol.TxSimContext, params map[string][]byte) (
	[]byte, []*commonPb.ContractEvent, error) {
	// verify params
	changed := false

	hashType, freezeKeyArray, err := r.getFreezeKeyArray(txSimContext)
	if err != nil {
		return nil, nil, err
	}

	// the full cert
	var certFullHashes bytes.Buffer
	certsStr := string(params[paramNameCerts])

	if utils.IsAnyBlank(certsStr) {
		err = fmt.Errorf("%s, freeze cert require param [%s] not found", common.ErrParams.Error(), paramNameCerts)
		r.log.Warn(err)
		return nil, nil, err
	}

	config, _ := common.GetChainConfig(txSimContext)

	certs := strings.Split(certsStr, ",")

	for _, cert := range certs {
		if msg := r.checkCert(cert, config.TrustRoots); msg != nil {
			r.log.Warnf("checkCert failed, err: %s", msg)
			return nil, nil, msg
		}
		certHash, err1 := utils.GetCertificateIdHex([]byte(cert), hashType)
		if err1 != nil {
			r.log.Warnf("utils.GetCertificateIdHex failed, err: %s", err1.Error())
			return nil, nil, err1
		}
		certHashKey := protocol.CertFreezeKeyPrefix + certHash
		certHashBytes, err1 := txSimContext.Get(certManageContractName, []byte(certHashKey))
		if err1 != nil {
			r.log.Warnf("txSimContext get certHashKey certHashKey[%s], err:", certHashKey, err1.Error())
			return nil, nil, err1
		}

		if len(certHashBytes) > 0 {
			// the certHashKey is exist
			msg := fmt.Errorf("the certHashKey is exist certHashKey[%s]", certHashKey)
			r.log.Warn(msg)
			return nil, nil, msg
		}

		err = txSimContext.Put(certManageContractName, []byte(certHashKey), []byte(cert))
		if err != nil {
			r.log.Warnf("txSimContext.Put err, err: %s", err.Error())
			return nil, nil, err
		}

		// add the certHashKey
		freezeKeyArray = append(freezeKeyArray, certHashKey)
		certFullHashes.WriteString(certHash)
		certFullHashes.WriteString(",")
		changed = true
	}

	if !changed {
		r.log.Warn(common.ErrParams)
		return nil, nil, common.ErrParams
	}

	marshal, err := json.Marshal(freezeKeyArray)
	if err != nil {
		r.log.Warnf("freezeKeyArray err: ", err.Error())
		return nil, nil, err
	}
	err = txSimContext.Put(certManageContractName, []byte(protocol.CertFreezeKey), marshal)
	if err != nil {
		r.log.Warnf("txSimContext put CERT_FREEZE_KEY err ", err.Error())
		return nil, nil, err
	}

	certHashes := strings.TrimRight(certFullHashes.String(), ",")
	cfg, err := common.GetChainConfigNoRecord(txSimContext)
	if err != nil {
		return nil, nil, err
	}
	event := []*commonPb.ContractEvent{
		{
			Topic:           strconv.Itoa(int(msgbus.CertManageCertsFreeze)),
			TxId:            txSimContext.GetTx().Payload.TxId,
			ContractName:    syscontract.SystemContract_CERT_MANAGE.String(),
			ContractVersion: cfg.Version,
			EventData:       []string{certsStr},
		},
	}

	r.log.Infof("certManage freeze success certHashes[%s]", certHashes)
	return []byte(certHashes), event, nil
}

// Unfreeze certs unfreeze
// @param certs is original certificate list string, separate by comma
// @param cert_hashes is cert hash list string, separate by comma
// either certs or cert_hashes
// @result string for "Success"
func (r *CertManageRuntime) Unfreeze(txSimContext protocol.TxSimContext, params map[string][]byte) (
	[]byte, []*commonPb.ContractEvent, error) {
	// verify params
	changed := false

	hashType, freezeKeyArray, err := r.getFreezeKeyArray(txSimContext)
	if err != nil {
		return nil, nil, err
	}

	if len(freezeKeyArray) == 0 {
		r.log.Warnf("no cert need to unfreeze")
		return nil, nil, errors.New("no cert need to unfreeze")
	}

	// the full cert
	certFullHashes := &bytes.Buffer{}
	certsStr := string(params[paramNameCerts])
	certHashesStr := string(params[paramNameCertHashes])

	if utils.IsAllBlank(certsStr, certHashesStr) {
		err = fmt.Errorf("%s, unfreeze cert require param [%s or %s] not found",
			common.ErrParams.Error(), paramNameCerts, paramNameCertHashes)
		r.log.Warn(err)
		return nil, nil, err
	}

	config, _ := common.GetChainConfig(txSimContext)
	certs := strings.Split(certsStr, ",")
	for _, cert := range certs {
		if msg := r.checkCert(cert, config.TrustRoots); msg != nil {
			return nil, nil, msg
		}
		if len(cert) == 0 {
			continue
		}
		certHash, err1 := utils.GetCertificateIdHex([]byte(cert), hashType)
		if err1 != nil {
			r.log.Warnf("GetCertificateIdHex failed, err: ", err1.Error())
			continue
		}
		freezeKeyArray, changed = r.recoverFrozenCert(txSimContext, certHash, freezeKeyArray, certFullHashes, changed)
	}

	certHashes := strings.Split(certHashesStr, ",")
	for _, certHash := range certHashes {
		if len(certHash) == 0 {
			continue
		}
		freezeKeyArray, changed = r.recoverFrozenCert(txSimContext, certHash, freezeKeyArray, certFullHashes, changed)
	}

	if !changed {
		r.log.Warn(common.ErrParams)
		return nil, nil, common.ErrParams
	}

	marshal, err := json.Marshal(freezeKeyArray)
	if err != nil {
		r.log.Warnf("freezeKeyArray err: ", err.Error())
		return nil, nil, err
	}
	err = txSimContext.Put(certManageContractName, []byte(protocol.CertFreezeKey), marshal)
	if err != nil {
		r.log.Warnf("txSimContext put CERT_FREEZE_KEY err: ", err.Error())
		return nil, nil, err
	}
	cfg, err := common.GetChainConfigNoRecord(txSimContext)
	if err != nil {
		return nil, nil, err
	}
	event := []*commonPb.ContractEvent{
		{
			Topic:           strconv.Itoa(int(msgbus.CertManageCertsUnfreeze)),
			TxId:            txSimContext.GetTx().Payload.TxId,
			ContractName:    syscontract.SystemContract_CERT_MANAGE.String(),
			ContractVersion: cfg.Version,
			EventData:       []string{certsStr, certHashesStr},
		},
	}

	certHasheStr := strings.TrimRight(certFullHashes.String(), ",")
	r.log.Infof("certManage unfreeze success certHashes[%s]", certHasheStr)
	return []byte("Success"), event, nil
}

// Revoke certs revocation
// @param cert_crl
// @return cert_crl
func (r *CertManageRuntime) Revoke(txSimContext protocol.TxSimContext, params map[string][]byte) (
	[]byte, []*commonPb.ContractEvent, error) {

	// verify params
	changed := false

	crlStr, ok := params[paramNameCertCrl]
	if !ok {
		err := fmt.Errorf("certManage cert revocation params err,cert_cerl is empty")
		r.log.Warn(err.Error())
		return nil, nil, err
	}
	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		r.log.Warnf("certManage txSimContext.GetOrganization failed, err: ", err.Error())
		return nil, nil, err
	}
	_, err = ac.VerifyRelatedMaterial(pbac.VerifyType_CRL, crlStr)
	if err != nil {
		r.log.Warnf("certManage validate crl failed err: ", err.Error())
		return nil, nil, err
	}

	var crls []*pkix.CertificateList

	crlPEM, rest := pem.Decode(crlStr)
	for crlPEM != nil {
		crl, err1 := x509.ParseCRL(crlPEM.Bytes)
		if err1 != nil {
			r.log.Warnf("certManage parse crl failed err: ", err1.Error())
			return nil, nil, err1
		}

		crlPEM, rest = pem.Decode(rest)
		crls = append(crls, crl)
	}

	crlBytes, err := txSimContext.Get(certManageContractName, []byte(protocol.CertRevokeKey))
	if err != nil {
		r.log.Warnf("get certManage crlList fail err: ", err.Error())
		return nil, nil, fmt.Errorf("get certManage crlList failed, err: %s", err)
	}

	crlKeyList := make([]string, 0)
	if len(crlBytes) > 0 {
		err = json.Unmarshal(crlBytes, &crlKeyList)
		if err != nil {
			r.log.Warnf("certManage unmarshal crl list err: ", err.Error())
			return nil, nil, errors.New("unmarshal crl list err")
		}
	}

	var crlResult bytes.Buffer
	for _, crtList := range crls {
		aki, err1 := getAKI(crtList)
		if err1 != nil {
			r.log.Warnf("certManage getAKI err: ", err1.Error())
			continue
		}

		key := fmt.Sprintf("%s%s", protocol.CertRevokeKeyPrefix, hex.EncodeToString(aki))
		crtListBytes, err1 := asn1.Marshal(*crtList)
		if err1 != nil {
			r.log.Warnf("certManage marshal crt list err: ", err1.Error())
			continue
		}

		existed := false
		crtListBytes1, err1 := txSimContext.Get(certManageContractName, []byte(key))
		if err1 != nil {
			r.log.Warnf("certManage txSimContext crtList err: ", err1.Error())
			continue
		}

		if len(crtListBytes1) > 0 {
			existed = true
		}

		// to pem bytes
		toMemory := pem.EncodeToMemory(&pem.Block{
			Type:    "crl",
			Headers: nil,
			Bytes:   crtListBytes,
		})

		err = txSimContext.Put(certManageContractName, []byte(key), toMemory)
		if err != nil {
			r.log.Warnf("certManage save crl certs err: ", err.Error())
			return nil, nil, err
		}

		if !existed {
			// add key to array
			crlKeyList = append(crlKeyList, key)
		}

		crlResult.WriteString(key + ",")
		changed = true
	}

	if !changed {
		r.log.Warn(common.ErrParams)
		return nil, nil, common.ErrParams
	}

	crlBytesResult, err := json.Marshal(crlKeyList)
	if err != nil {
		r.log.Warnf("certManage marshal crlKeyList err: ", err.Error())
		return nil, nil, err
	}
	err = txSimContext.Put(certManageContractName, []byte(protocol.CertRevokeKey), crlBytesResult)
	if err != nil {
		r.log.Warnf("certManage txSimContext put CertRevokeKey err: ", err.Error())
		return nil, nil, err
	}
	cfg, err := common.GetChainConfigNoRecord(txSimContext)
	if err != nil {
		return nil, nil, err
	}
	event := []*commonPb.ContractEvent{
		{
			Topic:           strconv.Itoa(int(msgbus.CertManageCertsRevoke)),
			TxId:            txSimContext.GetTx().Payload.TxId,
			ContractName:    syscontract.SystemContract_CERT_MANAGE.String(),
			ContractVersion: cfg.Version,
			EventData:       []string{string(crlStr)},
		},
	}

	crlResultStr := strings.TrimRight(crlResult.String(), ",")
	r.log.Infof("certManage revocation success crlResult[%s]", crlResultStr)
	return []byte(crlResultStr), event, nil
}

func getAKI(crl *pkix.CertificateList) (aki []byte, err error) {
	aki, _, err = bcx509.GetAKIFromExtensions(crl.TBSCertList.Extensions)
	if err != nil {
		return nil, fmt.Errorf("fail to get AKI of CRL [%s]: %v", crl.TBSCertList.Issuer.String(), err)
	}
	return aki, nil
}

func (r *CertManageRuntime) getFreezeKeyArray(txSimContext protocol.TxSimContext) (string, []string, error) {
	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		r.log.Warnf("txSimContext.GetAccessControl failed, err: ", err.Error())
		return "", nil, err
	}
	hashType := ac.GetHashAlg()

	// the freeze key array
	freezeKeyArray := make([]string, 0)
	freezeKeyArrayBytes, err := txSimContext.Get(certManageContractName,
		[]byte(protocol.CertFreezeKey))
	if err != nil {
		r.log.Warnf("txSimContext get CERT_FREEZE_KEY err: ", err.Error())
		return "", nil, err
	}

	if len(freezeKeyArrayBytes) > 0 {
		err := json.Unmarshal(freezeKeyArrayBytes, &freezeKeyArray)
		if err != nil {
			r.log.Warnf("unmarshal freeze key array err: ", err.Error())
			return "", nil, err
		}
	}
	return hashType, freezeKeyArray, nil
}

func (r *CertManageRuntime) recoverFrozenCert(txSimContext protocol.TxSimContext, certHash string,
	freezeKeyArray []string, certFullHashes *bytes.Buffer, changed bool) ([]string, bool) {
	certHashKey := protocol.CertFreezeKeyPrefix + certHash
	certHashBytes, err := txSimContext.Get(certManageContractName, []byte(certHashKey))
	if err != nil {
		r.log.Warnf("txSimContext get certHashKey err certHashKey[%s] err: ", certHashKey, err.Error())
		return freezeKeyArray, changed
	}

	if len(certHashBytes) == 0 {
		// the certHashKey is not exist
		r.log.Debugf("the certHashKey is not exist certHashKey[%s]", certHashKey)
		return freezeKeyArray, changed
	}

	err = txSimContext.Del(certManageContractName, []byte(certHashKey))
	if err != nil {
		r.log.Warnf("certManage unfreeze txSimContext.Del failed, certHash[%s] err:%s", certHash, err.Error())
		return freezeKeyArray, changed
	}

	for i := 0; i < len(freezeKeyArray); i++ {
		if strings.EqualFold(freezeKeyArray[i], certHashKey) {
			freezeKeyArray = append(freezeKeyArray[:i], freezeKeyArray[i+1:]...)
			certFullHashes.WriteString(certHash)
			certFullHashes.WriteString(",")
			changed = true
			break
		}
	}
	return freezeKeyArray, changed
}

// checkCert must not ca
func (r *CertManageRuntime) checkCert(cert string, trustRoots []*configPb.TrustRootConfig) error {
	c, err := utils.ParseCert([]byte(cert))
	if err != nil {
		return err
	}
	if c.IsCA {
		return errors.New("can not freeze/unfreeze root certificate")
	}

	caCerts := make([][]byte, 0)
	for _, root := range trustRoots {
		for _, certTmp := range root.Root {
			caCerts = append(caCerts, []byte(certTmp))
		}
	}
	//return nil
	return utils.VerifyCertIssue(caCerts, nil, []byte(cert))
}
