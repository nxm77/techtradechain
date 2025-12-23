/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package x509

import (
	"encoding/pem"
	"fmt"
)

// GetOUFromPEM get ou from cert pem
func GetOUFromPEM(certPEM []byte) ([]string, error) {
	pemBlock, _ := pem.Decode(certPEM)
	if pemBlock == nil {
		return nil, fmt.Errorf("fail to parse certificate")
	}
	cert, err := ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("fail to parse certificate: [%v]", err)
	}
	return cert.Subject.OrganizationalUnit, nil
}
