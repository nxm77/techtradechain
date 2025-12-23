/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"encoding/pem"

	"techtradechain.com/techtradechain/common/v2/crypto"
	bcx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
)

// Signer define interface of all kinds of signer in different mode
type Signer interface {
	// Sign sign payload
	Sign(payload *common.Payload) (signature []byte, err error)
	// NewMember new *accesscontrol.Member
	NewMember() (*accesscontrol.Member, error)
}

// CertModeSigner define a classic cert signer in PermissionedWithCert mode
type CertModeSigner struct {
	PrivateKey crypto.PrivateKey
	Cert       *bcx509.Certificate
	OrgId      string
}

// Sign sign payload
func (signer *CertModeSigner) Sign(payload *common.Payload) ([]byte, error) {
	hashType, err := bcx509.GetHashFromSignatureAlgorithm(signer.Cert.SignatureAlgorithm)
	if err != nil {
		return nil, err
	}
	return utils.SignPayloadWithHashType(signer.PrivateKey, hashType, payload)
}

// NewMember new *accesscontrol.Member
func (signer *CertModeSigner) NewMember() (*accesscontrol.Member, error) {
	certPem := pem.EncodeToMemory(&pem.Block{Bytes: signer.Cert.Raw, Type: "CERTIFICATE"})
	return &accesscontrol.Member{
		OrgId:      signer.OrgId,
		MemberInfo: certPem,
		MemberType: accesscontrol.MemberType_CERT,
	}, nil
}

// PublicModeSigner define a signer in Public mode
type PublicModeSigner struct {
	PrivateKey crypto.PrivateKey
	HashType   crypto.HashType
}

// Sign sign payload
func (signer *PublicModeSigner) Sign(payload *common.Payload) ([]byte, error) {
	return utils.SignPayloadWithHashType(signer.PrivateKey, signer.HashType, payload)
}

// NewMember new *accesscontrol.Member
func (signer *PublicModeSigner) NewMember() (*accesscontrol.Member, error) {
	pkPem, err := signer.PrivateKey.PublicKey().String()
	if err != nil {
		return nil, err
	}
	return &accesscontrol.Member{
		MemberInfo: []byte(pkPem),
		MemberType: accesscontrol.MemberType_PUBLIC_KEY,
	}, nil
}

// PermissionedWithKeyModeSigner define a signer in PermissionedWithKey mode
type PermissionedWithKeyModeSigner struct {
	PrivateKey crypto.PrivateKey
	HashType   crypto.HashType
	OrgId      string
}

// Sign sign payload
func (signer *PermissionedWithKeyModeSigner) Sign(payload *common.Payload) ([]byte, error) {
	return utils.SignPayloadWithHashType(signer.PrivateKey, signer.HashType, payload)
}

// NewMember new *accesscontrol.Member
func (signer *PermissionedWithKeyModeSigner) NewMember() (*accesscontrol.Member, error) {
	pkPem, err := signer.PrivateKey.PublicKey().String()
	if err != nil {
		return nil, err
	}
	return &accesscontrol.Member{
		OrgId:      signer.OrgId,
		MemberInfo: []byte(pkPem),
		MemberType: accesscontrol.MemberType_PUBLIC_KEY,
	}, nil
}

// CertAliasSigner define a cert alias signer in PermissionedWithCert mode
type CertAliasSigner struct {
	PrivateKey crypto.PrivateKey
	Cert       *bcx509.Certificate
	OrgId      string
	CertAlias  string
}

// Sign sign payload
func (signer *CertAliasSigner) Sign(payload *common.Payload) ([]byte, error) {
	hashType, err := bcx509.GetHashFromSignatureAlgorithm(signer.Cert.SignatureAlgorithm)
	if err != nil {
		return nil, err
	}
	return utils.SignPayloadWithHashType(signer.PrivateKey, hashType, payload)
}

// NewMember new *accesscontrol.Member
func (signer *CertAliasSigner) NewMember() (*accesscontrol.Member, error) {
	return &accesscontrol.Member{
		OrgId:      signer.OrgId,
		MemberInfo: []byte(signer.CertAlias),
		MemberType: accesscontrol.MemberType_ALIAS,
	}, nil
}

// CertHashSigner define a cert hash signer in PermissionedWithCert mode
type CertHashSigner struct {
	PrivateKey crypto.PrivateKey
	Cert       *bcx509.Certificate
	OrgId      string
	CertHash   []byte
}

// Sign sign payload
func (signer *CertHashSigner) Sign(payload *common.Payload) ([]byte, error) {
	hashType, err := bcx509.GetHashFromSignatureAlgorithm(signer.Cert.SignatureAlgorithm)
	if err != nil {
		return nil, err
	}
	return utils.SignPayloadWithHashType(signer.PrivateKey, hashType, payload)
}

// NewMember new *accesscontrol.Member
func (signer *CertHashSigner) NewMember() (*accesscontrol.Member, error) {
	return &accesscontrol.Member{
		OrgId:      signer.OrgId,
		MemberInfo: signer.CertHash,
		MemberType: accesscontrol.MemberType_CERT_HASH,
	}, nil
}
