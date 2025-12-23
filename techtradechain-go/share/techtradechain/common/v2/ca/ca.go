/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ca

// nolint:staticcheck
import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	cmx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
)

func loadCerts(caPaths []string) ([]string, error) {
	var filepaths []string

	for _, caPath := range caPaths {
		if caPath == "" {
			continue
		}

		dir, err := ioutil.ReadDir(caPath)
		if err != nil {
			return nil, err
		}

		pathSep := string(os.PathSeparator)

		for _, fi := range dir {
			if !fi.IsDir() {
				ok := strings.HasSuffix(fi.Name(), ".crt")
				if ok {
					filepaths = append(filepaths, caPath+pathSep+fi.Name())
				}
			}
		}
	}

	return filepaths, nil
}

func addTrust(pool *x509.CertPool, path string) error {
	aCrt, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read cert file failed, %s", err.Error())
	}

	//pool.AppendCertsFromPEM(aCrt)

	err = addCertPool(pool, string(aCrt))
	if err != nil {
		return fmt.Errorf("add cert pool failed, %s", err.Error())
	}

	return nil
}

func addGMTrust(pool *cmx509.CertPool, path string) error {
	aCrt, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read cert file failed, %s", err.Error())
	}

	//pool.AppendCertsFromPEM(aCrt)
	err = addSM2CertPool(pool, string(aCrt))
	if err != nil {
		return fmt.Errorf("add sm2 cert pool failed, %s", err.Error())
	}

	return nil
}

func getCertificates(trustRoot string) ([]*x509.Certificate, error) {
	var certificates []*x509.Certificate

	pemBlock, rest := pem.Decode([]byte(trustRoot))
	for pemBlock != nil {
		cert, err := x509.ParseCertificate(pemBlock.Bytes)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted root cert list")
		}

		certificates = append(certificates, cert)
		pemBlock, rest = pem.Decode(rest)
	}

	return certificates, nil
}

func addCertPool(certPool *x509.CertPool, trustRoot string) error {
	certificates, err := getCertificates(trustRoot)
	if err != nil {
		return fmt.Errorf("get certificates failed, %s", err.Error())
	}

	for _, certificate := range certificates {
		certPool.AddCert(certificate)
	}

	return nil
}

func getSM2Certificates(trustRoot string) ([]*cmx509.Certificate, error) {
	var certificates []*cmx509.Certificate

	pemBlock, rest := pem.Decode([]byte(trustRoot))
	for pemBlock != nil {
		cert, err := cmx509.ParseCertificate(pemBlock.Bytes)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted root cert list")
		}

		certificates = append(certificates, cert)
		pemBlock, rest = pem.Decode(rest)
	}

	return certificates, nil
}

func addSM2CertPool(certPool *cmx509.CertPool, trustRoot string) error {
	certificates, err := getSM2Certificates(trustRoot)
	if err != nil {
		return fmt.Errorf("get sm2 certificates failed, %s", err.Error())
	}

	for _, certificate := range certificates {
		certPool.AddCert(certificate)
	}

	return nil
}
