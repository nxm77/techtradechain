/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtlssupport

import (
	"errors"
	"fmt"
	"sync"

	cmx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/common/v2/helper"
	"techtradechain.com/techtradechain/net-common/common"
	pbac "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
)

// DerivedInfoWithCert contains infos loaded from tls cert when verifying peer certificate.
type DerivedInfoWithCert struct {
	TlsCertBytes []byte
	PubKeyBytes  []byte
	ChainIds     []string
	PeerId       string
	CertId       string
}

// CertValidator wraps a ChainTrustRoots instance and a common.MemberStatusValidator.
// It provides a function for verifying peer certificate when tls handshaking.
// In handshaking process, the function will load remote tls certificate and verify it
// by the access control module of each blockchain,
// also load remote peer id and cert id. All these infos will stored in validator.
// These infos could be queried with QueryDerivedInfoWithPeerId method,
// and could be removed with CleanDerivedInfoWithPeerId method.
type CertValidator struct {
	pkMode                bool
	memberStatusValidator *common.MemberStatusValidator
	infoStore             map[string]*DerivedInfoWithCert // map[peer.ID]*DerivedInfoWithCert
	mu                    sync.RWMutex

	chainTrustRoots *ChainTrustRoots
}

// NewCertValidator create a new CertValidator instance.
func NewCertValidator(pkMod bool, memberStatusValidator *common.MemberStatusValidator,
	roots *ChainTrustRoots) *CertValidator {
	return &CertValidator{
		pkMode:                pkMod,
		memberStatusValidator: memberStatusValidator,
		infoStore:             make(map[string]*DerivedInfoWithCert),
		mu:                    sync.RWMutex{},
		chainTrustRoots:       roots,
	}
}

// VerifyPeerCertificateFunc provides a function for verify peer certificate in tls config.
// In handshaking process, the function will load remote tls certificate and verify it
// by the access control module of each blockchain,
// also load remote peer id and cert id. All these infos will stored in validator.
func (v *CertValidator) VerifyPeerCertificateFunc() func(rawCerts [][]byte, _ [][]*cmx509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*cmx509.Certificate) error {
		validateRes := v.memberStatusValidate(rawCerts)
		if validateRes.err != nil {
			return validateRes.err
		}
		if !validateRes.passed {
			v.DeleteDerivedInfoWithPeerId(validateRes.peerIdStr)
			return errors.New("member status verify failed")
		}
		info := &DerivedInfoWithCert{
			TlsCertBytes: rawCerts[0],
			ChainIds:     validateRes.chainIds,
			PeerId:       validateRes.peerIdStr,
			PubKeyBytes:  validateRes.pkBytes,
			CertId:       validateRes.certId,
		}

		v.mu.Lock()
		defer v.mu.Unlock()
		v.infoStore[info.PeerId] = info
		return nil
	}
}

// QueryDerivedInfoWithPeerId return all infos that loaded with VerifyPeerCertificateFunc and stored in validator.
func (v *CertValidator) QueryDerivedInfoWithPeerId(peerId string) *DerivedInfoWithCert {
	v.mu.RLock()
	defer v.mu.RUnlock()
	res, ok := v.infoStore[peerId]
	if !ok {
		return nil
	}
	return res
}

// DeleteDerivedInfoWithPeerId if the certificate verify failed, delete the DerivedInfo
func (v *CertValidator) DeleteDerivedInfoWithPeerId(peerId string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.infoStore, peerId)
}

type memberStatusValidateResult struct {
	chainIds  []string
	passed    bool
	cert      *cmx509.Certificate
	peerIdStr string
	certId    string
	pkBytes   []byte
	err       error
}

func (v *CertValidator) memberStatusValidate(
	rawCerts [][]byte) *memberStatusValidateResult {
	// result
	res := &memberStatusValidateResult{}
	// members waited for validating
	members := make([]*pbac.Member, 0)
	for idx := range rawCerts {
		m := &pbac.Member{
			OrgId:      "",
			MemberType: pbac.MemberType_CERT,
			MemberInfo: rawCerts[idx],
		}
		if v.pkMode {
			m.MemberType = pbac.MemberType_PUBLIC_KEY
			cert, err := cmx509.ParseCertificate(m.MemberInfo)
			if err != nil {
				res.err = fmt.Errorf("parse certificate failed, %s", err.Error())
				return res
			}
			var pkPem string
			pkPem, err = cert.PublicKey.String()
			if err != nil {
				res.err = fmt.Errorf("get public key pem string failed, %s", err.Error())
				return res
			}
			m.MemberInfo = []byte(pkPem)
		}
		if idx == 0 {
			var err error
			res.cert, err = cmx509.ParseCertificate(rawCerts[idx])
			if err != nil {
				res.err = fmt.Errorf("parse certificate failed, %s", err.Error())
				return res
			}
			var pkPem string
			pkPem, err = res.cert.PublicKey.String()
			if err != nil {
				res.err = fmt.Errorf("get public key pem string failed, %s", err.Error())
				return res
			}
			res.pkBytes = []byte(pkPem)
			res.peerIdStr, err = helper.CreateLibp2pPeerIdWithPublicKey(res.cert.PublicKey)
			if err != nil {
				res.err = fmt.Errorf("parse pid from pubkey failed, %s", err.Error())
				return res
			}
			if v.pkMode {
				res.certId = pkPem
			} else {
				certIdBytes, err2 := cmx509.GetNodeIdFromSm2Certificate(cmx509.OidNodeId, *res.cert)
				if err2 != nil {
					res.err = fmt.Errorf("get certid failed, %s", err2.Error())
					return res
				}
				res.certId = string(certIdBytes)
			}
		}
		members = append(members, m)
	}
	res.chainIds, res.passed, res.err = v.memberStatusValidator.ValidateMemberStatus(members)
	if res.err == nil && !res.passed && !v.pkMode && v.chainTrustRoots != nil {
		// verify with custom trust root
		cert, err := cmx509.ParseCertificate(rawCerts[0])
		if err != nil {
			res.err = fmt.Errorf("parse certificate failed, %s", err.Error())
			return res
		}
		res.chainIds, res.err = v.chainTrustRoots.VerifyCert(cert)
		if len(res.chainIds) > 0 {
			res.passed = true
		}
	}
	return res
}
