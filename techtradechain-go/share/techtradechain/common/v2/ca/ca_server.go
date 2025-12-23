/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ca

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	cmtls "techtradechain.com/techtradechain/common/v2/crypto/tls"
	cmcred "techtradechain.com/techtradechain/common/v2/crypto/tls/credentials"
	cmx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/common/v2/log"

	"google.golang.org/grpc/credentials"
)

// CAServer CA服务端对象
type CAServer struct {
	CaPaths  []string
	CaCerts  []string
	CertFile string
	KeyFile  string
	Logger   log.LoggerInterface
}

// CustomVerify 自定义验证模式
type CustomVerify struct {
	VerifyPeerCertificate   func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error
	GMVerifyPeerCertificate func(rawCerts [][]byte, verifiedChains [][]*cmx509.Certificate) error
}

// GetCredentialsByCA 获得服务端的TransportCredentials对象
// @param checkClientAuth
// @param customVerify
// @return cert
// @return err
func (s *CAServer) GetCredentialsByCA(checkClientAuth bool, customVerify CustomVerify) (
	*credentials.TransportCredentials, error) {

	cert, err := tls.LoadX509KeyPair(s.CertFile, s.KeyFile)
	if err == nil {
		return s.getCredentialsByCA(checkClientAuth, &cert, customVerify.VerifyPeerCertificate)
	}

	gmCert, err := cmtls.LoadX509KeyPair(s.CertFile, s.KeyFile)
	if err == nil {
		return s.getGMCredentialsByCA(checkClientAuth, &gmCert, customVerify.GMVerifyPeerCertificate)
	}

	return nil, fmt.Errorf("load X509 key pair failed, %s", err.Error())
}

func (s *CAServer) getCredentialsByCA(checkClientAuth bool,
	cert *tls.Certificate,
	customVerifyFunc func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error) (
	*credentials.TransportCredentials, error) {

	var (
		clientAuth tls.ClientAuthType
		clientCAs  *x509.CertPool
	)

	if checkClientAuth {

		certPool := x509.NewCertPool()

		if len(s.CaCerts) > 0 {
			if err := s.addCertsToCertPool(certPool); err != nil {
				return nil, err
			}
		} else {
			if err := s.addTrustCertsToCertPool(certPool); err != nil {
				return nil, err
			}
		}

		clientAuth = tls.RequireAndVerifyClientCert
		clientCAs = certPool
	} else {
		clientAuth = tls.NoClientCert
		clientCAs = nil
	}

	// nolint: gosec
	c := credentials.NewTLS(&tls.Config{
		Certificates:          []tls.Certificate{*cert},
		ClientAuth:            clientAuth,
		ClientCAs:             clientCAs,
		InsecureSkipVerify:    false,
		VerifyPeerCertificate: customVerifyFunc,
	})

	return &c, nil
}

func (s *CAServer) addCertsToCertPool(certPool *x509.CertPool) error {
	for _, caCert := range s.CaCerts {
		if caCert != "" {
			err := addCertPool(certPool, caCert)
			if err != nil {
				s.Logger.Warnf("ignore invalid cert [%s], %s", caCert, err.Error())
				continue
			}
		}
	}
	return nil
}

func (s *CAServer) addTrustCertsToCertPool(certPool *x509.CertPool) error {
	caCerts, err := loadCerts(s.CaPaths)
	if err != nil {
		errMsg := fmt.Sprintf("load trust certs failed, %s", err.Error())
		return errors.New(errMsg)
	}

	if len(caCerts) == 0 {
		return ErrTrustCrtsDirEmpty
	}

	for _, caCert := range caCerts {
		err := addTrust(certPool, caCert)
		if err != nil {
			s.Logger.Warnf("ignore invalid cert [%s], %s", caCert, err.Error())
			continue
		}
	}
	return nil
}

func (s *CAServer) getGMCredentialsByCA(checkClientAuth bool,
	cert *cmtls.Certificate,
	customVerifyFunc func(rawCerts [][]byte, verifiedChains [][]*cmx509.Certificate) error) (
	*credentials.TransportCredentials, error) {

	var clientAuth cmtls.ClientAuthType
	var clientCAs *cmx509.CertPool

	if checkClientAuth {

		certPool := cmx509.NewCertPool()

		if len(s.CaCerts) > 0 {
			if err := s.addCertsToSM2CertPool(certPool); err != nil {
				return nil, err
			}
		} else {
			if err := s.addTrustCertsToSM2CertPool(certPool); err != nil {
				return nil, err
			}
		}

		clientAuth = cmtls.RequireAndVerifyClientCert
		clientCAs = certPool
	} else {
		clientAuth = cmtls.NoClientCert
		clientCAs = nil
	}

	c := cmcred.NewTLS(&cmtls.Config{
		Certificates:          []cmtls.Certificate{*cert},
		ClientAuth:            clientAuth,
		ClientCAs:             clientCAs,
		InsecureSkipVerify:    false,
		VerifyPeerCertificate: customVerifyFunc,
	})

	return &c, nil
}

func (s *CAServer) addCertsToSM2CertPool(certPool *cmx509.CertPool) error {
	for _, caCert := range s.CaCerts {
		if caCert != "" {
			err := addSM2CertPool(certPool, caCert)
			if err != nil {
				s.Logger.Warnf("ignore invalid cert [%s], %s", caCert, err.Error())
				continue
			}
		}
	}
	return nil
}

func (s *CAServer) addTrustCertsToSM2CertPool(certPool *cmx509.CertPool) error {
	caCerts, err := loadCerts(s.CaPaths)
	if err != nil {
		errMsg := fmt.Sprintf("load trust certs failed, %s", err.Error())
		return errors.New(errMsg)
	}

	if len(caCerts) == 0 {
		return ErrTrustCrtsDirEmpty
	}

	for _, caCert := range caCerts {
		err := addGMTrust(certPool, caCert)
		if err != nil {
			s.Logger.Warnf("ignore invalid cert [%s], %s", caCert, err.Error())
			continue
		}
	}
	return nil
}
