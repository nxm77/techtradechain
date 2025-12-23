/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"bytes"
	"encoding/asn1"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/common/v2/crypto/asym/rsa"
	bcx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/pb-go/v2/tee"
)

const (
	// KLV_LENGTH_SIZE 4
	KLV_LENGTH_SIZE = 4
)

// TEEProof TEE proof
type TEEProof struct {
	VerificationKey    bccrypto.PublicKey
	VerificationKeyPEM []byte
	EncryptionKey      bccrypto.EncryptKey
	EncryptionKeyPEM   []byte
	Certificate        *bcx509.Certificate
	CertificateDER     []byte
	Report             []byte
	Challenge          []byte
	Signature          []byte
}

// GrapheneAttestationVerify  graphene attestation verify
func GrapheneAttestationVerify(proof *tee.RemoteAttestationResponse, certOpts bcx509.VerifyOptions,
	reportFromChain []byte) (bool, *TEEProof, error) {
	challenge := proof.RemoteAttestationPayload.Challenge
	report := proof.RemoteAttestationPayload.Report
	cert := proof.RemoteAttestationPayload.TeeCert
	sig := proof.Signature
	if challenge == "" || report == nil || cert == nil || sig == nil {
		err := errors.New("paramers not be empty")
		return false, nil, fmt.Errorf("fail to get proof: %v", err)
	}

	certificate, err := bcx509.ParseCertificate(cert)
	if err != nil {
		return false, nil, fmt.Errorf("fail to parse TEE certificate: %v", err)
	}

	verificationKey := certificate.PublicKey

	encryptionKeyPEM, err := bcx509.GetExtByOid(OidKeyBag, certificate.Extensions)
	if err != nil {
		encryptionKeyPEM, err = bcx509.GetExtByOid(OidKeyBag, certificate.ExtraExtensions)
		if err != nil {
			return false, nil, fmt.Errorf("fail to get encryption key: %v", err)
		}
	}
	encryptionKeyInterface, err := asym.PublicKeyFromPEM(encryptionKeyPEM)
	if err != nil {
		return false, nil, fmt.Errorf("fail to parse TEE encryption key: %v", err)
	}

	var encryptionKey bccrypto.EncryptKey
	switch k := encryptionKeyInterface.(type) {
	case *rsa.PublicKey:
		encryptionKey = k
	default:
		return false, nil, fmt.Errorf("unrecognized encryption key type")
	}

	msg, err := json.Marshal(proof.RemoteAttestationPayload)
	if err != nil {
		return false, nil, fmt.Errorf("fail to marshal remote msg")
	}

	algo := certificate.SignatureAlgorithm
	hashType, err := bcx509.GetHashFromSignatureAlgorithm(algo)
	if err != nil {
		return false, nil, fmt.Errorf("fail to get hash from signature algorithm ")
	}

	pss := ""
	switch algo {
	case bcx509.SHA256WithRSAPSS, bcx509.SHA384WithRSAPSS, bcx509.SHA1WithRSA:
		pss = rsa.RSA_PSS
	default:
		pss = ""
	}

	isValid, err := verificationKey.VerifyWithOpts(msg, sig, &bccrypto.SignOpts{
		Hash:         hashType,
		UID:          "",
		EncodingType: pss,
	})
	if err != nil {
		return false, nil, fmt.Errorf("invalid signature: %v", err)
	}
	if !isValid {
		return false, nil, fmt.Errorf("invalid signature")
	}

	certChains, err := certificate.Verify(certOpts)
	if err != nil || certChains == nil {
		return false, nil, fmt.Errorf("untrusted certificate: %v", err)
	}

	if !bytes.Equal(report, reportFromChain) {
		return false, nil, fmt.Errorf("report does not match, reportFromChain: %s, report: %s",
			reportFromChain, report)
	}

	verificationKeyPEM, err := verificationKey.String()
	if err != nil {
		return false, nil, fmt.Errorf("fail to serialize verification key")
	}

	teeProof := &TEEProof{
		VerificationKey:    verificationKey,
		VerificationKeyPEM: []byte(verificationKeyPEM),
		EncryptionKey:      encryptionKey,
		EncryptionKeyPEM:   encryptionKeyPEM,
		Certificate:        certificate,
		CertificateDER:     cert,
		Report:             report,
		Challenge:          []byte(challenge),
		Signature:          sig,
	}
	return true, teeProof, nil
}

// AttestationVerify attestation verify
func AttestationVerify(proof []byte, certOpts bcx509.VerifyOptions, reportFromChain []byte) (bool, *TEEProof, error) {
	challengeLen, err := BinaryToUint32(proof[0:KLV_LENGTH_SIZE])
	if err != nil {
		return false, nil, fmt.Errorf("invalid input: %v", err)
	}
	challenge := proof[KLV_LENGTH_SIZE : challengeLen+KLV_LENGTH_SIZE]

	reportLen, err := BinaryToUint32(proof[challengeLen+KLV_LENGTH_SIZE : challengeLen+KLV_LENGTH_SIZE*2])
	if err != nil {
		return false, nil, fmt.Errorf("invalid input: %v", err)
	}
	report := proof[challengeLen+KLV_LENGTH_SIZE*2 : challengeLen+reportLen+KLV_LENGTH_SIZE*2]

	certLen, err := BinaryToUint32(
		proof[challengeLen+reportLen+KLV_LENGTH_SIZE*2 : challengeLen+reportLen+KLV_LENGTH_SIZE*3],
	)
	if err != nil {
		return false, nil, fmt.Errorf("invalid input: %v", err)
	}
	certDER := proof[challengeLen+reportLen+KLV_LENGTH_SIZE*3 : challengeLen+reportLen+certLen+KLV_LENGTH_SIZE*3]

	sigLen, err := BinaryToUint32(
		proof[challengeLen+reportLen+certLen+KLV_LENGTH_SIZE*3 : challengeLen+reportLen+certLen+KLV_LENGTH_SIZE*4],
	)
	if err != nil {
		return false, nil, fmt.Errorf("invalid input: %v", err)
	}
	sig := proof[challengeLen+reportLen+certLen+KLV_LENGTH_SIZE*4 : challengeLen+reportLen+certLen+sigLen+KLV_LENGTH_SIZE*4] // nolint: lll

	certificate, err := bcx509.ParseCertificate(certDER)
	if err != nil {
		return false, nil, fmt.Errorf("fail to parse TEE certificate: %v", err)
	}

	verificationKey := certificate.PublicKey

	encryptionKeyPEM, err := bcx509.GetExtByOid(OidKeyBag, certificate.Extensions)
	if err != nil {
		encryptionKeyPEM, err = bcx509.GetExtByOid(OidKeyBag, certificate.ExtraExtensions)
		if err != nil {
			return false, nil, fmt.Errorf("fail to get encryption key: %v", err)
		}
	}

	//encryptionKeyBlock, _ := pem.Decode(encryptionKeyPEM)
	//if encryptionKeyBlock == nil {
	//	return false, nil, fmt.Errorf("fail to decode encryption key")
	//}
	encryptionKeyInterface, err := asym.PublicKeyFromPEM(encryptionKeyPEM)
	if err != nil {
		return false, nil, fmt.Errorf("fail to parse TEE encryption key: %v", err)
	}

	var encryptionKey bccrypto.EncryptKey
	switch k := encryptionKeyInterface.(type) {
	case *rsa.PublicKey:
		encryptionKey = k
	default:
		return false, nil, fmt.Errorf("unrecognized encryption key type")
	}

	msg := proof[0 : challengeLen+reportLen+certLen+KLV_LENGTH_SIZE*3]
	isValid, err := verificationKey.VerifyWithOpts(msg, sig, &bccrypto.SignOpts{
		Hash:         bccrypto.HASH_TYPE_SHA256,
		UID:          "",
		EncodingType: rsa.RSA_PSS,
	})
	if err != nil {
		return false, nil, fmt.Errorf("invalid signature: %v", err)
	}
	if !isValid {
		return false, nil, fmt.Errorf("invalid signature")
	}

	certChains, err := certificate.Verify(certOpts)
	if err != nil || certChains == nil {
		return false, nil, fmt.Errorf("untrusted certificate: %v", err)
	}

	fmt.Printf("###### report = %s\n", string(report))
	fmt.Printf("###### report from chain = %s\n", string(reportFromChain))
	if !bytes.Equal(report, reportFromChain) {
		return false, nil, fmt.Errorf("report does not match, reportFromChain: %s, report: %s",
			reportFromChain, report)
	}

	verificationKeyPEM, err := verificationKey.String()
	if err != nil {
		return false, nil, fmt.Errorf("fail to serialize verification key")
	}

	teeProof := &TEEProof{
		VerificationKey:    verificationKey,
		VerificationKeyPEM: []byte(verificationKeyPEM),
		EncryptionKey:      encryptionKey,
		EncryptionKeyPEM:   encryptionKeyPEM,
		Certificate:        certificate,
		CertificateDER:     certDER,
		Report:             report,
		Challenge:          challenge,
		Signature:          sig,
	}

	return true, teeProof, nil
}

// AttestationVerifyComponents attestation verify components
func AttestationVerifyComponents(challenge, signature, report []byte,
	certificate *bcx509.Certificate, verificationKey bccrypto.PublicKey,
	encryptionKey bccrypto.EncryptKey, certOpts bcx509.VerifyOptions) (bool, *TEEProof, error) {
	challengeLen := Uint32ToBinary(uint32(len(challenge)))
	reportLen := Uint32ToBinary(uint32(len(report)))
	certLen := Uint32ToBinary(uint32(len(certificate.Raw)))
	msg := append(challengeLen, challenge...)
	msg = append(msg, reportLen...)
	msg = append(msg, report...)
	msg = append(msg, certLen...)
	msg = append(msg, certificate.Raw...)

	isValid, err := verificationKey.VerifyWithOpts(msg, signature, &bccrypto.SignOpts{
		Hash:         bccrypto.HASH_TYPE_SHA256,
		UID:          "",
		EncodingType: rsa.RSA_PSS,
	})
	if err != nil {
		return false, nil, fmt.Errorf("invalid signature: %v", err)
	}
	if !isValid {
		return false, nil, fmt.Errorf("invalid signature")
	}

	certChains, err := certificate.Verify(certOpts)
	if err != nil || certChains == nil {
		return false, nil, fmt.Errorf("untrusted certificate: %v", err)
	}

	verificationKeyPEM, err := verificationKey.String()
	if err != nil {
		return false, nil, fmt.Errorf("fail to serialize verification key")
	}
	verificationKeyDER, err := verificationKey.Bytes()
	if err != nil {
		return false, nil, fmt.Errorf("fail to serialize verification key")
	}

	verificationKeyDERFromCert, err := certificate.PublicKey.Bytes()
	if err != nil {
		return false, nil, fmt.Errorf("fail to serialize verification key in certificate")
	}

	if !bytes.Equal(verificationKeyDER, verificationKeyDERFromCert) {
		return false, nil, fmt.Errorf("verification key do not match")
	}

	encryptionKeyPEM, err := encryptionKey.String()
	if err != nil {
		return false, nil, fmt.Errorf("fail to serialize encryption key")
	}
	encryptionKeyDER, err := encryptionKey.Bytes()
	if err != nil {
		return false, nil, fmt.Errorf("fail to serialize encryption key")
	}

	encryptionKeyPEMFromCert, err := bcx509.GetExtByOid(OidKeyBag, certificate.Extensions)
	if err != nil {
		encryptionKeyPEMFromCert, err = bcx509.GetExtByOid(OidKeyBag, certificate.ExtraExtensions)
		if err != nil {
			return false, nil, fmt.Errorf("fail to get encryption key: %v", err)
		}
	}

	encryptionKeyBlockFromCert, _ := pem.Decode(encryptionKeyPEMFromCert)
	if encryptionKeyBlockFromCert == nil {
		return false, nil, fmt.Errorf("fail to decode encryption key")
	}

	if !bytes.Equal(encryptionKeyDER, encryptionKeyBlockFromCert.Bytes) {
		return false, nil, fmt.Errorf("encryption key do not match")
	}

	teeProof := &TEEProof{
		VerificationKey:    verificationKey,
		VerificationKeyPEM: []byte(verificationKeyPEM),
		EncryptionKey:      encryptionKey,
		EncryptionKeyPEM:   []byte(encryptionKeyPEM),
		Certificate:        certificate,
		CertificateDER:     certificate.Raw,
		Report:             report,
		Challenge:          challenge,
		Signature:          signature,
	}

	return true, teeProof, nil
}

var (
	// OidKeyBag oid key bag
	OidKeyBag = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 12, 10, 1, 1}
)

// BinaryToUint32 binary to uint32
func BinaryToUint32(in []byte) (uint32, error) {
	if len(in) != KLV_LENGTH_SIZE {
		return 0, fmt.Errorf("input is not an uint32: %v", in)
	}

	result := uint32(in[0])*(1<<24) + uint32(in[1])*(1<<16) + uint32(in[2])*(1<<8) + uint32(in[3])
	return result, nil
}

// Uint32ToBinary uint32 to binary
func Uint32ToBinary(in uint32) []byte {
	out := make([]byte, 4)
	out[0] = byte(in / (1 << 24))
	out[1] = byte((in % (1 << 24)) / (1 << 16))
	out[2] = byte((in % (1 << 16)) / (1 << 8))
	out[3] = byte(in % (1 << 8))
	return out
}
