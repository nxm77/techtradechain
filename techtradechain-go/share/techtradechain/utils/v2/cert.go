/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	bcx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
)

// GetCertHash get certificate hash
func GetCertHash(_ string, userCrtPEM []byte, hashType string) ([]byte, error) {
	certHash, err := getCertificateId(userCrtPEM, hashType)
	if err != nil {
		return nil, fmt.Errorf("calc cert hash failed, %s", err.Error())
	}
	return certHash, nil
}

func getCertificateId(certPEM []byte, hashType string) ([]byte, error) {
	if certPEM == nil {
		return nil, fmt.Errorf("get cert certPEM == nil")
	}

	certDer, _ := pem.Decode(certPEM)
	if certDer == nil {
		return nil, fmt.Errorf("invalid certificate")
	}

	return getCertificateIdFromDER(certDer.Bytes, hashType)
}

func getCertificateIdFromDER(certDER []byte, hashType string) ([]byte, error) {
	if certDER == nil {
		return nil, fmt.Errorf("get cert from der certDER == nil")
	}

	id, err := hash.GetByStrType(hashType, certDER)
	if err != nil {
		return nil, err
	}

	return id, nil
}

// ParseCert convert bytearray to certificate
func ParseCert(crtPEM []byte) (*bcx509.Certificate, error) {
	certBlock, _ := pem.Decode(crtPEM)
	if certBlock == nil {
		return nil, fmt.Errorf("decode pem failed, invalid certificate")
	}

	cert, err := bcx509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("x509 parse cert failed, %s", err)
	}

	return cert, nil
}

// VerifyCertIssue verify that the certificate is issued by the root/intermediate certificate
// caCerts caCert list, caCert means cert []byte(string)
func VerifyCertIssue(caCerts [][]byte, intermediateCerts [][]byte, cert []byte) error {
	c, err := ParseCert(cert)
	if err != nil {
		return err
	}

	caPool := bcx509.NewCertPool()
	for _, caCert := range caCerts {
		pemBlock, rest := pem.Decode(caCert)
		for pemBlock != nil {
			crt, err2 := bcx509.ParseCertificate(pemBlock.Bytes)
			if err2 != nil {
				return fmt.Errorf("x509 parse cert failed, %s", err2)
			}
			caPool.AddCert(crt)
			pemBlock, rest = pem.Decode(rest)
		}
	}
	intermediatePool := bcx509.NewCertPool()
	for _, intermediateCert := range intermediateCerts {
		pemBlock, rest := pem.Decode(intermediateCert)
		for pemBlock != nil {
			crt, err3 := bcx509.ParseCertificate(pemBlock.Bytes)
			if err3 != nil {
				return fmt.Errorf("x509 parse cert failed, %s", err3)
			}
			intermediatePool.AddCert(crt)
			pemBlock, rest = pem.Decode(rest)
		}
	}
	if len(intermediateCerts) == 0 {
		intermediatePool = caPool
	}

	certChain, err := c.Verify(bcx509.VerifyOptions{
		Intermediates:             caPool,
		Roots:                     intermediatePool,
		CurrentTime:               time.Time{},
		KeyUsages:                 []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		MaxConstraintComparisions: 0,
	})
	if err != nil {
		return err
	}
	if len(certChain) > 0 && len(certChain[0]) > 0 {
		return nil
	}
	return errors.New("the cert is not in trust root")
}
