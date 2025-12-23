/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"errors"
	"fmt"
	"io/ioutil"

	"techtradechain.com/techtradechain/common/v2/kmsutils"

	"techtradechain.com/techtradechain/common/v2/cert"
	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	bcx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"github.com/gogo/protobuf/proto"
)

// SignPayload sign payload
// Deprecated: This function will be deleted when appropriate. Please use SignPayloadWithHashType
func SignPayload(privateKey crypto.PrivateKey, cert *bcx509.Certificate, payload *common.Payload) ([]byte, error) {
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return SignPayloadBytes(privateKey, cert, payloadBytes)
}

// SignPayloadBytes sign payload bytes
// Deprecated: This function will be deleted when appropriate. Please use SignPayloadBytesWithHashType
func SignPayloadBytes(privateKey crypto.PrivateKey, cert *bcx509.Certificate, payloadBytes []byte) ([]byte, error) {
	var opts crypto.SignOpts
	hashalgo, err := bcx509.GetHashFromSignatureAlgorithm(cert.SignatureAlgorithm)
	if err != nil {
		return nil, fmt.Errorf("invalid algorithm: %v", err)
	}

	opts.Hash = hashalgo
	opts.UID = crypto.CRYPTO_DEFAULT_UID

	return privateKey.SignWithOpts(payloadBytes, &opts)
}

// SignPayloadWithHashType sign payload with specified hash type
func SignPayloadWithHashType(privateKey crypto.PrivateKey,
	hashType crypto.HashType, payload *common.Payload) ([]byte, error) {
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return SignPayloadBytesWithHashType(privateKey, hashType, payloadBytes)
}

// SignPayloadBytesWithHashType sign payload bytes with specified hash type
func SignPayloadBytesWithHashType(privateKey crypto.PrivateKey,
	hashType crypto.HashType, payloadBytes []byte) ([]byte, error) {
	var opts crypto.SignOpts
	opts.Hash = hashType
	opts.UID = crypto.CRYPTO_DEFAULT_UID

	return privateKey.SignWithOpts(payloadBytes, &opts)
}

// SignPayloadWithPath sign payload with specified key/cert file path
func SignPayloadWithPath(keyFilePath, crtFilePath string, payload *common.Payload) ([]byte, error) {
	// 读取私钥
	keyPem, err := ioutil.ReadFile(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("read key file failed, %s", err)
	}

	// 读取证书
	certPem, err := ioutil.ReadFile(crtFilePath)
	if err != nil {
		return nil, fmt.Errorf("read cert file failed, %s", err)
	}

	key, err := asym.PrivateKeyFromPEM(keyPem, nil)
	if err != nil {
		return nil, err
	}

	cert, err := ParseCert(certPem)
	if err != nil {
		return nil, err
	}

	hashAlgo, err := bcx509.GetHashFromSignatureAlgorithm(cert.SignatureAlgorithm)
	if err != nil {
		return nil, err
	}

	return SignPayloadWithHashType(key, hashAlgo, payload)
}

// SignPayloadWithPkPath sign payload with specified key file path
func SignPayloadWithPkPath(keyFilePath, hashType string, payload *common.Payload) ([]byte, error) {
	keyPem, err := ioutil.ReadFile(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("read key file failed, %s", err)
	}

	key, err := asym.PrivateKeyFromPEM(keyPem, nil)
	if err != nil {
		return nil, err
	}

	return SignPayloadWithHashType(key, crypto.HashAlgoMap[hashType], payload)
}

// NewEndorser returns *common.EndorsementEntry
// Deprecated: This function will be deleted when appropriate. Please use NewEndorserWithMemberType
func NewEndorser(orgId string, certPem []byte, sig []byte) *common.EndorsementEntry {
	return &common.EndorsementEntry{
		Signer: &accesscontrol.Member{
			OrgId:      orgId,
			MemberInfo: certPem,
			MemberType: accesscontrol.MemberType_CERT,
		},
		Signature: sig,
	}
}

// NewPkEndorser returns *common.EndorsementEntry
func NewPkEndorser(orgId string, pk []byte, sig []byte) *common.EndorsementEntry {
	return &common.EndorsementEntry{
		Signer: &accesscontrol.Member{
			OrgId:      orgId,
			MemberInfo: pk,
			MemberType: accesscontrol.MemberType_PUBLIC_KEY,
		},
		Signature: sig,
	}
}

// NewEndorserWithMemberType new endorser with member type
func NewEndorserWithMemberType(orgId string, memberInfo []byte, memberType accesscontrol.MemberType,
	sig []byte) *common.EndorsementEntry {
	return &common.EndorsementEntry{
		Signer: &accesscontrol.Member{
			OrgId:      orgId,
			MemberInfo: memberInfo,
			MemberType: memberType,
		},
		Signature: sig,
	}
}

// MakeEndorserWithPem make endorser with pem
// Deprecated: This function will be deleted when appropriate. Please use MakeEndorser
func MakeEndorserWithPem(keyPem, certPem []byte, payload *common.Payload) (*common.EndorsementEntry, error) {
	key, err := asym.PrivateKeyFromPEM(keyPem, nil)
	if err != nil {
		return nil, err
	}

	cert, err := ParseCert(certPem)
	if err != nil {
		return nil, err
	}

	hashAlgo, err := bcx509.GetHashFromSignatureAlgorithm(cert.SignatureAlgorithm)
	if err != nil {
		return nil, err
	}

	signature, err := SignPayloadWithHashType(key, hashAlgo, payload)
	if err != nil {
		return nil, err
	}

	var orgId string
	if len(cert.Subject.Organization) != 0 {
		orgId = cert.Subject.Organization[0]
	}

	return NewEndorserWithMemberType(orgId, certPem, accesscontrol.MemberType_CERT, signature), nil
}

// MakePkEndorserWithPem make public mode endorser with pem
// Deprecated: This function will be deleted when appropriate. Please use MakeEndorser
func MakePkEndorserWithPem(keyPem []byte, hashType crypto.HashType, orgId string,
	payload *common.Payload) (*common.EndorsementEntry, error) {
	key, err := asym.PrivateKeyFromPEM(keyPem, nil)
	if err != nil {
		return nil, err
	}

	signature, err := SignPayloadWithHashType(key, hashType, payload)
	if err != nil {
		return nil, err
	}

	return NewEndorserWithMemberType(orgId, keyPem, accesscontrol.MemberType_PUBLIC_KEY, signature), nil
}

// MakeEndorser returns *common.EndorsementEntry
func MakeEndorser(orgId string, hashType crypto.HashType, memberType accesscontrol.MemberType, keyPem,
	memberInfo []byte, payload *common.Payload) (*common.EndorsementEntry, error) {
	var (
		err       error
		key       crypto.PrivateKey
		signature []byte
	)

	key, err = asym.PrivateKeyFromPEM(keyPem, nil)
	if err != nil {
		return nil, err
	}

	signature, err = SignPayloadWithHashType(key, hashType, payload)
	if err != nil {
		return nil, err
	}

	return NewEndorserWithMemberType(orgId, memberInfo, memberType, signature), nil
}

// MakeEndorserWithPath make endorser with key/cert file path
func MakeEndorserWithPath(keyFilePath, crtFilePath string, payload *common.Payload) (*common.EndorsementEntry, error) {
	// 读取私钥
	keyPem, err := ioutil.ReadFile(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("read key file failed, %s", err)
	}

	// 读取证书
	certPem, err := ioutil.ReadFile(crtFilePath)
	if err != nil {
		return nil, fmt.Errorf("read cert file failed, %s", err)
	}

	cert, err := ParseCert(certPem)
	if err != nil {
		return nil, err
	}

	hashAlgo, err := bcx509.GetHashFromSignatureAlgorithm(cert.SignatureAlgorithm)
	if err != nil {
		return nil, err
	}

	var orgId string
	if len(cert.Subject.Organization) != 0 {
		orgId = cert.Subject.Organization[0]
	}

	return MakeEndorser(orgId, hashAlgo, accesscontrol.MemberType_CERT, keyPem, certPem, payload)
}

// MakePkEndorserWithPath make public mode endorser with key file path
func MakePkEndorserWithPath(keyFilePath string, hashType crypto.HashType, orgId string,
	payload *common.Payload) (*common.EndorsementEntry, error) {
	keyPem, err := ioutil.ReadFile(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("read key file failed, %s", err)
	}

	key, err := asym.PrivateKeyFromPEM(keyPem, nil)
	if err != nil {
		return nil, fmt.Errorf("")
	}

	pubKey := key.PublicKey()
	memberInfo, err := pubKey.String()
	if err != nil {
		return nil, err
	}

	return MakeEndorser(orgId, hashType, accesscontrol.MemberType_PUBLIC_KEY, keyPem,
		[]byte(memberInfo), payload)
}

// MakeEndorserWithPathAndP11Handle make endorser with key/cert file path and P11Handle
func MakeEndorserWithPathAndP11Handle(keyFilePath, crtFilePath string, p11Handle interface{}, kmsEnabled bool,
	payload *common.Payload) (*common.EndorsementEntry, error) {
	if p11Handle == nil && !kmsEnabled {
		return nil, errors.New("p11Handle is not nil or kmsEnabled is true")
	}

	// 读取私钥
	keySpec, err := ioutil.ReadFile(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("read key file failed, %s", err)
	}

	// 读取证书
	certPem, err := ioutil.ReadFile(crtFilePath)
	if err != nil {
		return nil, fmt.Errorf("read cert file failed, %s", err)
	}

	var key crypto.PrivateKey
	if p11Handle != nil {
		key, err = cert.ParseP11PrivKey(p11Handle, keySpec)
	} else if kmsEnabled {
		key, err = kmsutils.ParseKMSPrivKey(keySpec)
	}
	if err != nil {
		return nil, fmt.Errorf("cert.ParseP11PrivKey failed, %s", err)
	}

	cert, err := ParseCert(certPem)
	if err != nil {
		return nil, err
	}

	signature, err := SignPayload(key, cert, payload)
	if err != nil {
		return nil, err
	}

	var orgId string
	if len(cert.Subject.Organization) != 0 {
		orgId = cert.Subject.Organization[0]
	}

	e := NewEndorser(orgId, certPem, signature)
	return e, nil
}

// MakePkEndorserWithPathAndP11Handle make public mode endorser with key file path
func MakePkEndorserWithPathAndP11Handle(keyFilePath string, hashType crypto.HashType, kmsEnabled bool, orgId string,
	payload *common.Payload) (*common.EndorsementEntry, error) {
	keyPem, err := ioutil.ReadFile(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("read key file failed, %s", err)
	}

	if !kmsEnabled {
		return nil, fmt.Errorf("use wrong method")
	}

	key, err := kmsutils.ParseKMSPrivKey(keyPem)
	if err != nil {
		return nil, fmt.Errorf("")
	}

	pubKey := key.PublicKey()
	memberInfo, err := pubKey.String()
	if err != nil {
		return nil, err
	}
	signature, err := SignPayloadWithHashType(key, hashType, payload)
	if err != nil {
		return nil, err
	}

	e := NewPkEndorser(orgId, []byte(memberInfo), signature)
	return e, nil
}
