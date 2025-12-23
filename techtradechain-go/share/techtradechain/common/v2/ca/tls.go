/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ca

import (
	"errors"
	"fmt"
	"net"
	"os"

	cmtls "techtradechain.com/techtradechain/common/v2/crypto/tls"
	cmx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"

	"golang.org/x/net/http2"
)

// GetTLSConfig 根据传入的参数，构建tls.Config对象
// @param certPemPath
// @param certKeyPath
// @param caPaths
// @param caCerts
// @param encCertPemPath
// @param encCertKeyPath
// @return *cmtls.Config
// @return error
func GetTLSConfig(certPemPath, certKeyPath string, caPaths, caCerts []string,
	encCertPemPath, encCertKeyPath string) (*cmtls.Config, error) {
	//single cert mode
	_, err1 := os.Stat(encCertPemPath)
	_, err2 := os.Stat(encCertKeyPath)
	if errors.Is(err1, os.ErrNotExist) || errors.Is(err2, os.ErrNotExist) {
		return getTlsConfig(certPemPath, certKeyPath, caPaths, caCerts)
	}

	// double cert mode (gmtls1.1)
	return getGMTlsConfig(certPemPath, certKeyPath, encCertPemPath, encCertKeyPath, caPaths, caCerts)
}

func getTlsConfig(certPemPath, certKeyPath string, caPaths, caCerts []string) (*cmtls.Config, error) {
	certKeyPair, err := cmtls.LoadX509KeyPair(certPemPath, certKeyPath)
	if err != nil {
		return nil, err
	}
	certPool, err := getCertPool(caPaths, caCerts)
	if err != nil {
		return nil, err
	}

	cfg := &cmtls.Config{
		Certificates: []cmtls.Certificate{certKeyPair},
		NextProtos:   []string{http2.NextProtoTLS},
		ClientCAs:    certPool,
	}
	//set clientAuth if caCert exists
	if certPool != nil {
		cfg.ClientAuth = cmtls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func getGMTlsConfig(certPemPath, certKeyPath, encCertPemPath, encCertKeyPath string,
	caPaths, caCerts []string) (*cmtls.Config, error) {
	sigCert, err := cmtls.LoadX509KeyPair(certPemPath, certKeyPath)
	if err != nil {
		return nil, err
	}
	encCert, err := cmtls.LoadX509KeyPair(encCertPemPath, encCertKeyPath)
	if err != nil {
		return nil, err
	}
	certPool, err := getCertPool(caPaths, caCerts)
	if err != nil {
		return nil, err
	}

	cfg := &cmtls.Config{
		GMSupport:    cmtls.NewGMSupport(),
		Certificates: []cmtls.Certificate{sigCert, encCert},
		NextProtos:   []string{http2.NextProtoTLS},
		ClientCAs:    certPool,
	}
	//set clientAuth if caCert exists
	if certPool != nil {
		cfg.ClientAuth = cmtls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func getCertPool(caPaths, caCerts []string) (*cmx509.CertPool, error) {
	if len(caPaths) == 0 && len(caCerts) == 0 {
		return nil, nil
	}

	certPool := cmx509.NewCertPool()
	if len(caPaths) > 0 {
		caCertPaths, err := loadCerts(caPaths)
		if err != nil {
			return nil, fmt.Errorf("load trust certs failed, %s", err.Error())
		}

		if len(caCertPaths) == 0 {
			return nil, errors.New("trust certs dir is empty")
		}

		for _, caCertPath := range caCertPaths {
			err := addGMTrust(certPool, caCertPath)
			if err != nil {
				return nil, err
			}
		}
	}

	for _, caCert := range caCerts {
		//err := addTrust(certPool, caCert)
		err := addSM2CertPool(certPool, caCert)
		if err != nil {
			return nil, err
		}
	}

	return certPool, nil
}

// NewTLSListener 根据tls配置，新建net.Listener对象
// @param inner
// @param config
// @return net.Listener
func NewTLSListener(inner net.Listener, config *cmtls.Config) net.Listener {
	return cmtls.NewListener(inner, config)
}
