/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtlssupport

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"

	"math/big"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto"
	cmTls "techtradechain.com/techtradechain/common/v2/crypto/tls"
	cmx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/common/v2/helper"
	"techtradechain.com/techtradechain/net-common/common"
	"github.com/lucas-clemente/quic-go"
)

const (
	certValidityPeriod = 100 * 365 * 24 * time.Hour // ~100 years
)

// NewTlsConfigWithCertMode create a new tls config with tls certificates for tls handshake.
func NewTlsConfigWithCertMode(
	certificates []cmTls.Certificate,
	certValidator *CertValidator,
) (*cmTls.Config, error) {
	if certValidator.pkMode {
		return nil, errors.New("cert validator in public key mode, but tls config with cert mode creating")
	}
	tlsConfig := &cmTls.Config{
		Certificates:          certificates,
		InsecureSkipVerify:    true,
		ClientAuth:            cmTls.RequireAnyClientCert,
		VerifyPeerCertificate: certValidator.VerifyPeerCertificateFunc(),
	}
	//len(certificates) == 2 means enc cert is set, use gmtls
	if len(certificates) == 2 {
		tlsConfig.GMSupport = cmTls.NewGMSupport()
	}
	return tlsConfig, nil
}

// NewTlsConfigWithPubKeyMode create a new tls config with a tls certificate
// wrapped the public key of the private key for tls handshake.
func NewTlsConfigWithPubKeyMode(
	sk crypto.PrivateKey,
	certValidator *CertValidator,
) (*cmTls.Config, error) {
	if !certValidator.pkMode {
		return nil, errors.New("cert validator in cert mode, but tls config with public key mode creating")
	}
	cert, err := PrivateKeyToCertificate(sk)
	if err != nil {
		return nil, err
	}
	tlsConfig := &cmTls.Config{
		//MinVersion:               cmTls.VersionTLS13,
		PreferServerCipherSuites: common.PreferServerCipherSuites(),
		InsecureSkipVerify:       true, // This is not insecure here. We will verify the cert chain ourselves.
		ClientAuth:               cmTls.RequireAnyClientCert,
		Certificates:             []cmTls.Certificate{*cert},
		VerifyPeerCertificate:    certValidator.VerifyPeerCertificateFunc(),
		SessionTicketsDisabled:   true,
	}
	return tlsConfig, nil
}

// NewTlsConfigWithPubKeyMode4Quic create a new tls config with a tls certificate
// wrapped the public key of the private key for tls handshake. Just for quic network.
func NewTlsConfigWithPubKeyMode4Quic(
	sk crypto.PrivateKey,
	certValidator *CertValidator,
) (*cmTls.Config, error) {
	if !certValidator.pkMode {
		return nil, errors.New("cert validator in cert mode, but tls config with public key mode creating")
	}
	cert, err := PrivateKeyToCertificate4Quic(sk)
	if err != nil {
		return nil, err
	}
	tlsConfig := &cmTls.Config{
		//MinVersion:               cmTls.VersionTLS13,
		PreferServerCipherSuites: common.PreferServerCipherSuites(),
		InsecureSkipVerify:       true, // This is not insecure here. We will verify the cert chain ourselves.
		ClientAuth:               cmTls.RequireAnyClientCert,
		Certificates:             []cmTls.Certificate{*cert},
		VerifyPeerCertificate:    certValidator.VerifyPeerCertificateFunc(),
		SessionTicketsDisabled:   true,
	}
	return tlsConfig, nil
}

// GetCertAndPeerIdWithKeyPair will create a tls cert with x509 key pair and load the peer id from cert.
func GetCertAndPeerIdWithKeyPair(certPEMBlock []byte, keyPEMBlock []byte) (*cmTls.Certificate, string, error) {
	certificate, err := cmTls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return nil, "", err
	}
	peerID, err2 := helper.GetLibp2pPeerIdFromCert(certPEMBlock)
	if err2 != nil {
		return nil, "", err2
	}
	return &certificate, peerID, nil
}

// GetCertAndPeerIdWithKeyPair4Quic will create a tls cert with qx509 key pair and load the peer id from cert.
func GetCertAndPeerIdWithKeyPair4Quic(certPEMBlock []byte, keyPEMBlock []byte) (*cmTls.Certificate, string, error) {
	certificateQ, err := quic.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return nil, "", err
	}
	peerID, err2 := helper.GetLibp2pPeerIdFromCert(certPEMBlock)
	if err2 != nil {
		return nil, "", err2
	}
	certificate := ParseQTLSCertToCMTLSCert(certificateQ)
	return &certificate, peerID, nil
}

// ParseQTLSCertToCMTLSCert parse the tls cert to techtradechain tls cert
func ParseQTLSCertToCMTLSCert(cert tls.Certificate) cmTls.Certificate {
	cmCert := cmTls.Certificate{
		Certificate:                  cert.Certificate,
		PrivateKey:                   cert.PrivateKey,
		SupportedSignatureAlgorithms: parseSignatureScheme(cert.SupportedSignatureAlgorithms),
		OCSPStaple:                   cert.OCSPStaple,
		SignedCertificateTimestamps:  cert.SignedCertificateTimestamps,
		Leaf:                         cert.Leaf,
	}
	return cmCert
}

func parseSignatureScheme(ss []tls.SignatureScheme) []cmTls.SignatureScheme {
	if ss == nil {
		return nil
	}
	res := make([]cmTls.SignatureScheme, 0, 16)
	for _, s := range ss {
		res = append(res, cmTls.SignatureScheme(s))
	}
	return res
}

// PrivateKeyToCertificate create a certificate simply with a private key.
func PrivateKeyToCertificate(privateKey crypto.PrivateKey) (*cmTls.Certificate, error) {
	sn, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: sn,
		NotBefore:    time.Time{},
		NotAfter:     time.Now().Add(certValidityPeriod),
	}
	certDER, err := cmx509.CreateCertificate(rand.Reader, tmpl, tmpl,
		privateKey.PublicKey().ToStandardKey(), privateKey.ToStandardKey())
	if err != nil {
		return nil, err
	}
	return &cmTls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  privateKey.ToStandardKey(),
	}, nil
}

// PrivateKeyToCertificate4Quic create a certificate simply with a private key. Just for quic network.
func PrivateKeyToCertificate4Quic(privateKey crypto.PrivateKey) (*cmTls.Certificate, error) {
	sn, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: sn,
		NotBefore:    time.Time{},
		NotAfter:     time.Now().Add(certValidityPeriod),
	}
	certDER, err := cmx509.CreateCertificate(rand.Reader, tmpl, tmpl, privateKey.PublicKey().
		ToStandardKey(), privateKey.ToStandardKey())
	if err != nil {
		return nil, err
	}
	return &cmTls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  privateKey.ToStandardKey(),
	}, nil
}
