/*
 * Copyright (C) BABEC. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
/**
证书管理：链运行在证书模式下可用
包括：
	证书哈希上链、删除、查询；
	别名上链、更新、删除、查询
证书别名就是给证书设置一个别名，上链的时候可用此别名代替。以减少证书实际体积。
证书哈希是一种特殊的别名。

当前文件是证书别名的管理。
*/
package certmgr220

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	accesscontrolPb "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
)

// AddAlias add alias
//
// 别名和证书是多对一关系，一个证书可以有多个别名
// 别名唯一不可重复
// 其中证书哈希是别名的一个特殊格式
// 无需传入证书，因为是直接使用交易的签名者，即 sender
// @param alias 别名
func (r *CertManageRuntime) AddAlias(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// 1. get params and verify fmt
	// 2. verify exist
	// 3. save alias info
	// 4. return
	name := string(params[paramNameAlias])
	name = strings.TrimSpace(name)
	if utils.IsAnyBlank(name) {
		err = fmt.Errorf("add alias failed, alias name is nil")
		r.log.Warn(err)
		return nil, err
	}
	if !aliasNameReg.MatchString(name) {
		err = fmt.Errorf("add alias failed, alias name[%s] must match %s", name, aliasNameRegStr)
		r.log.Warn(err)
		return nil, err
	}
	sender := txSimContext.GetTx().Sender.Signer
	if sender.MemberType != accesscontrolPb.MemberType_CERT {
		err = fmt.Errorf("add alias failed, MemberType must be MemberType_CERT ")
		r.log.Warn(err)
		return nil, err
	}

	certAliasInfo, err := r.getAliasFromDb(txSimContext, name)
	if err != nil {
		err = fmt.Errorf("add alias failed, %s", err.Error())
		r.log.Warn(err)
		return nil, err
	}
	if certAliasInfo != nil && len(certAliasInfo.NowCert.Cert) > 0 {
		err = fmt.Errorf("add alias failed, alias[%s] already exist", name)
		r.log.Warn(err)
		return nil, err
	}

	err = r.addAliasCore(txSimContext, name, sender.GetMemberInfo(), certAliasInfo)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}
	return []byte("ok"), nil
}

// UpdateAlias 别名更新，本组织管理员
//
// 别名自身不可更新，需组织管理员更新
// @param cert 更新的证书
// @param alias 别名
func (r *CertManageRuntime) UpdateAlias(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// 1. get params and verify fmt
	// 2. verify sender org
	// 3. replace now cert
	// 4. append to history cert
	// 5. save alias info
	// 6. return
	cert := string(params[paramNameCert])
	name := string(params[paramNameAlias])
	cert = strings.TrimSpace(cert)
	name = strings.TrimSpace(name)
	if utils.IsAnyBlank(name, cert) {
		err = fmt.Errorf("update alias failed, alias name or cert is nil")
		r.log.Warn(err)
		return nil, err
	}
	if !aliasNameReg.MatchString(name) {
		err = fmt.Errorf("update alias failed, alias name[%s] must match %s", name, aliasNameRegStr)
		r.log.Warn(err)
		return nil, err
	}

	certAliasInfo, err := r.getAliasFromDb(txSimContext, name)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}
	if certAliasInfo == nil || len(certAliasInfo.NowCert.Cert) == 0 {
		// if non-existent return
		err = fmt.Errorf("update alias fail, alias[%s] not exist", name)
		r.log.Warn(err)
		return nil, err

		// if non-existent insert
		//err = r.addAliasCore(txSimContext, name, []byte(cert))
		//if err != nil {
		//	r.log.Warn(err)
		//	return nil, err
		//}
		//return []byte("ok"), nil
	}

	// verify org
	certificate, err := utils.ParseCert([]byte(cert))
	if err != nil || certificate == nil || certificate.Subject.Organization == nil {
		err = fmt.Errorf("update alias fail, params[%s] format error, err:%s", paramNameCert, err)
		r.log.Warn(err)
		return nil, err
	}

	nowCertificate, err := utils.ParseCert(certAliasInfo.NowCert.Cert)
	if err != nil || nowCertificate == nil || nowCertificate.Subject.Organization == nil {
		err = fmt.Errorf("update alias fail, params[%s] format error, err:%s", paramNameCert, err)
		r.log.Warn(err)
		return nil, err
	}

	if !containOrgId(txSimContext, certificate.Subject.Organization[0]) {
		err = fmt.Errorf("update alias fail, "+
			"you can't change to other organization[%s] cert. ",
			certificate.Subject.Organization[0])
		r.log.Warn(err)
		return nil, err
	}

	if !containOrgId(txSimContext, nowCertificate.Subject.Organization[0]) {
		err = fmt.Errorf("update alias fail, "+
			"you can't change other organization[%s] cert. ",
			certificate.Subject.Organization[0])
		r.log.Warn(err)
		return nil, err
	}

	nowCert := &commonPb.AliasCertInfo{
		Cert:        []byte(cert),
		BlockHeight: txSimContext.GetBlockHeight(),
	}

	err = r.setCertHash(txSimContext, nowCert)
	if err != nil {
		return nil, err
	}

	if bytes.Equal(certAliasInfo.NowCert.Cert, nowCert.Cert) {
		return nil, fmt.Errorf("update alias fail, alias[%s] is already the cert", name)
	}
	certAliasInfo.NowCert = nowCert
	h := certAliasInfo.HisCerts
	h = append(h, nowCert)
	if len(h) > maxHisCertsLen {
		certAliasInfo.HisCerts = h[len(h)-maxHisCertsLen:]
	} else {
		certAliasInfo.HisCerts = h
	}
	err = r.setAliasToDb(txSimContext, certAliasInfo)
	if err != nil {
		return nil, err
	}
	r.log.Infof("update alias success, alias[%s] cert[%s]", name, cert)
	return []byte("ok"), nil
}

// DeleteAlias 本组织管理员
//
// 别名自身不可删除，需组织管理员更新
// @param aliases 别名列表，以逗号分隔，如："alias1,alias2,alias3"
func (r *CertManageRuntime) DeleteAlias(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// 1. get params and verify fmt
	// 2. verify sender org
	// 3. replace now cert
	// 4. append to history cert
	// 5. save none alias
	// 6. return
	names := string(params[paramNameAliases])
	names = strings.TrimSpace(names)
	if utils.IsAnyBlank(names) {
		err = fmt.Errorf("delete alias failed, alias name is nil")
		r.log.Warn(err)
		return nil, err
	}
	nameList := strings.Split(names, ",")
	for _, name := range nameList {
		certAliasInfo, err := r.getAliasFromDb(txSimContext, name)
		if err != nil {
			r.log.Warn(err)
			return nil, err
		}

		if certAliasInfo == nil || len(certAliasInfo.NowCert.Cert) == 0 {
			err = fmt.Errorf("delete alias fail, alias[%s] not exist", name)
			r.log.Warn(err)
			return nil, err
		}

		// verify org
		certificate, err := utils.ParseCert(certAliasInfo.NowCert.Cert)
		if err != nil || certificate == nil || certificate.Subject.Organization == nil {
			err = fmt.Errorf("delete alias fail, params[%s] format error, err:%s", paramNameCert, err)
			r.log.Warn(err)
			return nil, err
		}
		if !containOrgId(txSimContext, certificate.Subject.Organization[0]) {
			err = fmt.Errorf("delete alias fail, "+
				"you can't delete other organization[%s] cert. ",
				certificate.Subject.Organization[0])
			r.log.Warn(err)
			return nil, err
		}

		blankCert := &commonPb.AliasCertInfo{BlockHeight: txSimContext.GetBlockHeight()}
		certAliasInfo.NowCert = blankCert
		h := certAliasInfo.HisCerts
		h = append(h, blankCert)
		if len(h) > maxHisCertsLen {
			certAliasInfo.HisCerts = h[len(h)-maxHisCertsLen:]
		} else {
			certAliasInfo.HisCerts = h
		}
		err = r.setAliasToDb(txSimContext, certAliasInfo)
		if err != nil {
			return nil, err
		}
		r.log.Infof("delete alias[%s] success", name)
	}
	return []byte("ok"), nil
}

// QueryAlias 查询别名信息
//
// @param aliases 别名列表，以逗号分隔，如："alias1,alias2,alias3"
// @return HisCerts 返回该别名从最近一次添加后最近10次的的历史变更记录
// @return NowCert 当前别名代表的证书、开始生效的高度、证书的哈希
// @return Alias 别名
func (r *CertManageRuntime) QueryAlias(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	names := string(params[paramNameAliases])
	names = strings.TrimSpace(names)
	if utils.IsAnyBlank(names) {
		err = fmt.Errorf("query alias failed, alias name is nil")
		r.log.Warn(err)
		return nil, err
	}

	nameList := strings.Split(names, ",")
	infos := commonPb.AliasInfos{}
	for _, name := range nameList {
		certAliasInfo, err := r.getAliasFromDb(txSimContext, name)
		if err != nil {
			r.log.Error(err)
			return nil, err
		}
		if certAliasInfo == nil {
			err = fmt.Errorf("query alias failed, not found alias[%s], you should add alias first", name)
			r.log.Warn(err)
			return nil, err
		}
		infos.AliasInfos = append(infos.AliasInfos, certAliasInfo)
	}
	return infos.Marshal()
}

func (r *CertManageRuntime) addAliasCore(txSimContext protocol.TxSimContext, aliasName string,
	certBytes []byte, certAliasInfo *commonPb.AliasInfo) error {

	nowCert := &commonPb.AliasCertInfo{
		Cert:        certBytes,
		BlockHeight: txSimContext.GetBlockHeight(),
	}
	err := r.setCertHash(txSimContext, nowCert)
	if err != nil {
		return err
	}

	if certAliasInfo == nil {
		certAliasInfo = &commonPb.AliasInfo{Alias: aliasName}
	}
	err = r.saveAllAlias(txSimContext, aliasName)
	if err != nil {
		return err
	}
	certAliasInfo.NowCert = nowCert
	certAliasInfo.HisCerts = append(certAliasInfo.HisCerts, nowCert)
	err = r.setAliasToDb(txSimContext, certAliasInfo)
	if err != nil {
		return err
	}
	r.log.Infof("add alias success, alias[%s] cert[%s]", aliasName, certBytes)
	return err
}

func (r *CertManageRuntime) saveAllAlias(txSimContext protocol.TxSimContext, aliasStr string) error {
	allAliasBytes, err := txSimContext.Get(certManageContractName, []byte(certAliasKey+"all"))
	if err != nil {
		r.log.Warnf("save all alias failed, err: ", err.Error())
		return err
	}
	allAlias := string(allAliasBytes)
	if len(allAlias) > 0 {
		allAlias += ","
	}
	allAlias += aliasStr
	_ = txSimContext.Put(certManageContractName, []byte(certAliasKey+"all"), []byte(allAlias))
	return nil
}

func (r *CertManageRuntime) setAliasToDb(txSimContext protocol.TxSimContext, certAliasInfo *commonPb.AliasInfo) error {
	certAliasBytes, err := certAliasInfo.Marshal()
	if err != nil {
		r.log.Warnf("inner error, proto marshal fail, err: ", err.Error())
		return err
	}
	alias := hex.EncodeToString([]byte(certAliasInfo.Alias))
	_ = txSimContext.Put(certManageContractName, []byte(certAliasKey+certAliasInfo.Alias), certAliasBytes)
	_ = txSimContext.Put(certManageContractName, []byte(alias), certAliasInfo.NowCert.Cert)

	return nil
}

func (r *CertManageRuntime) getAliasFromDb(txSimContext protocol.TxSimContext, aliasStr string) (
	*commonPb.AliasInfo, error) {
	if utils.IsAnyBlank(aliasStr) {
		err := fmt.Errorf("params[%s] is empty", paramNameAlias)
		r.log.Warn(err)
		return nil, err
	}
	if err := protocol.CheckKeyFieldStr(aliasStr, ""); err != nil {
		r.log.Warn(err)
		return nil, err
	}

	certAliasBytes, err := txSimContext.Get(certManageContractName, []byte(certAliasKey+aliasStr))
	if err != nil {
		r.log.Errorf("inner error, get alias from db failed, err: ", err.Error())
		return nil, err
	}

	if certAliasBytes == nil {
		return nil, nil
	}
	certAliasInfo := &commonPb.AliasInfo{}
	err = proto.Unmarshal(certAliasBytes, certAliasInfo)
	if err != nil {
		r.log.Errorf("inner error, proto unmarshal AliasInfo fail, err: ", err.Error())
		return nil, err
	}
	return certAliasInfo, nil
}

func (r *CertManageRuntime) setCertHash(txSimContext protocol.TxSimContext, nowCert *commonPb.AliasCertInfo) error {
	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		r.log.Errorf("txSimContext.GetAccessControl failed, err: %s", err.Error())
		return err
	}
	hashType := ac.GetHashAlg()
	certHash, err := utils.GetCertificateIdHex(nowCert.Cert, hashType)
	if err != nil {
		r.log.Errorf("get certHash failed, err: %s", err.Error())
		return err
	}
	nowCert.Hash = certHash
	return nil
}

func containOrgId(ctx protocol.TxSimContext, orgId string) bool {
	endorses := ctx.GetTx().Endorsers
	if len(endorses) == 0 {
		return ctx.GetSender().OrgId == orgId
	}
	for _, endorse := range endorses {
		return endorse.Signer.OrgId == orgId
	}
	return false
}

// Stay new function
//
//func (r *CertManageRuntime) FreezeAlias(txSimContext protocol.TxSimContext, params map[string][]byte) (
//	result []byte, err error) {
//	// get alias
//	names := string(params[paramNameAliases])
//	names = strings.TrimSpace(names)
//	if utils.IsAnyBlank(names) {
//		err = fmt.Errorf("freeze alias failed, alias name is nil")
//		r.log.Warn(err)
//		return nil, err
//	}
//
//	nameList := strings.Split(names, ",")
//	for _, name := range nameList {
//		certAliasInfo, err := r.getAliasFromDb(txSimContext, name)
//		if err != nil {
//			r.log.Error(err)
//			return nil, err
//		}
//		if certAliasInfo == nil {
//			err = fmt.Errorf("freeze alias failed, not found alias[%s], you should add alias first", name)
//			r.log.Warn(err)
//			return nil, err
//		}
//		//if certAliasInfo.Status != AliasStatusNormal.String() {
//		//	err = fmt.Errorf("freeze alias failed, alias[%s] status[%s] is not normal", name, certAliasInfo.Status)
//		//	r.log.Warn(err)
//		//	return nil, err
//		//}
//		//// save status
//		//certAliasInfo.Status = AliasStatusFrozen.String()
//
//		err = r.setAliasToDb(txSimContext, certAliasInfo)
//		if err != nil {
//			return nil, err
//		}
//		err = r.addFreezeAliasArray(txSimContext, name)
//		if err != nil {
//			return nil, err
//		}
//		r.log.Infof("freeze alias success, alias[%s]", name)
//	}
//	return []byte("ok"), nil
//}
//
//func (r *CertManageRuntime) UnfreezeAlias(txSimContext protocol.TxSimContext, params map[string][]byte) (
//	result []byte, err error) {
//	// get alias
//	names := string(params[paramNameAliases])
//	names = strings.TrimSpace(names)
//	if utils.IsAnyBlank(names) {
//		err = fmt.Errorf("unfreeze alias failed, alias name is nil")
//		r.log.Warn(err)
//		return nil, err
//	}
//	nameList := strings.Split(names, ",")
//	for _, name := range nameList {
//		certAliasInfo, err := r.getAliasFromDb(txSimContext, name)
//		if err != nil {
//			r.log.Error(err)
//			return nil, err
//		}
//		if certAliasInfo == nil {
//			err = fmt.Errorf("unfreeze alias failed, not found alias[%s], you should add alias first", name)
//			r.log.Warn(err)
//			return nil, err
//		}
//		//if certAliasInfo.Status == AliasStatusDeleted.String() {
//		//	err = fmt.Errorf("freeze alias failed, alias[%s] has been deleted", name)
//		//	r.log.Warn(err)
//		//	return nil, err
//		//}
//		//if certAliasInfo.Status != AliasStatusFrozen.String() {
//		//	err = fmt.Errorf("freeze alias failed, alias[%s] status[%s] is not frozen", name, certAliasInfo.Status)
//		//	r.log.Warn(err)
//		//	return nil, err
//		//}
//		//// save status
//		//certAliasInfo.Status = AliasStatusNormal.String()
//		err = r.setAliasToDb(txSimContext, certAliasInfo)
//		if err != nil {
//			return nil, err
//		}
//
//		err = r.removeFreezeAliasArray(txSimContext, name)
//		if err != nil {
//			return nil, err
//		}
//		r.log.Infof("unfreeze alias success, alias[%s]", name)
//	}
//	return []byte("ok"), nil
//}
//func (r *CertManageRuntime) addFreezeAliasArray(txSimContext protocol.TxSimContext, alias string) error {
//	aliases, err := r.getFreezeAliasArray(txSimContext)
//	if err != nil {
//		return err
//	}
//	aliases = append(aliases, alias)
//
//	marshal, err := json.Marshal(aliases)
//	if err != nil {
//		r.log.Warnf("freezeKeyArray err: ", err.Error())
//		return err
//	}
//	return txSimContext.Put(certManageContractName, []byte(protocol.CertFreezeKey), marshal)
//}
//
//func (r *CertManageRuntime) removeFreezeAliasArray(txSimContext protocol.TxSimContext, alias string) error {
//	aliases, err := r.getFreezeAliasArray(txSimContext)
//	if err != nil {
//		return err
//	}
//	idx := -1
//	for i, a := range aliases {
//		if a == alias {
//			idx = i
//			break
//		}
//	}
//	if idx >= 0 {
//		aliases = append(aliases[:idx], aliases[idx+1:]...)
//	}
//
//	marshal, err := json.Marshal(aliases)
//	if err != nil {
//		r.log.Warnf("freezeKeyArray err: ", err.Error())
//		return err
//	}
//	return txSimContext.Put(certManageContractName, []byte(protocol.CertFreezeKey), marshal)
//}
//
//func (r *CertManageRuntime) getFreezeAliasArray(txSimContext protocol.TxSimContext) ([]string, error) {
//	freezeKeyArray := make([]string, 0)
//	freezeKeyArrayBytes, err := txSimContext.Get(certManageContractName,
//		[]byte(protocol.CertFreezeKey))
//	if err != nil {
//		r.log.Warnf("txSimContext get CERT_ALIAS_FREEZE err: ", err.Error())
//		return nil, err
//	}
//
//	if len(freezeKeyArrayBytes) > 0 {
//		err := json.Unmarshal(freezeKeyArrayBytes, &freezeKeyArray)
//		if err != nil {
//			r.log.Warnf("unmarshal freeze alias array err: ", err.Error())
//			return nil, err
//		}
//	}
//	return freezeKeyArray, nil
//}
//
//
//func (r *CertManageRuntime) QueryCertByAliasAndBlockHeight(txSimContext protocol.TxSimContext,
//	params map[string][]byte) (result []byte, err error) {
//	name := string(params[paramNameAlias])
//	blockHeight := string(params[paramBlockHeight])
//	name = strings.TrimSpace(name)
//	height, err := strconv.Atoi(blockHeight)
//	if err != nil {
//		err = fmt.Errorf("query alias failed, params[%s] not number", paramBlockHeight)
//		r.log.Warn(err)
//		return nil, err
//	}
//	if utils.IsAnyBlank(name) {
//		err = fmt.Errorf("query alias failed, alias name is nil")
//		r.log.Warn(err)
//		return nil, err
//	}
//
//	certAliasInfo, err := r.getAliasFromDb(txSimContext, name)
//	if err != nil {
//		r.log.Warn(err)
//		return nil, err
//	}
//	if certAliasInfo == nil {
//		err = fmt.Errorf("query alias failed, not found alias[%s], you should add alias first", name)
//		r.log.Warn(err)
//		return nil, err
//	}
//	hisCert := &commonPb.AliasCertInfo{}
//	for _, cert := range certAliasInfo.HisCerts {
//		if cert.BlockHeight <= uint64(height) {
//			hisCert = cert
//		}
//	}
//	return hisCert.Marshal()
//}
