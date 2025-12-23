/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkcs11

import (
	"bytes"
	"crypto/elliptic"
	"encoding/asn1"
	"math/big"

	"github.com/tjfoc/gmsm/sm2"

	"github.com/pkg/errors"
)

var errUnsupportedEllipticCurve = errors.New("unsupported elliptic curve")

// Information about an Elliptic Curve
type curveInfo struct {
	oid   []byte
	curve elliptic.Curve
}

var knownCurves = map[string]curveInfo{
	"P-224": {
		mustMarshal(asn1.ObjectIdentifier{1, 3, 132, 0, 33}),
		elliptic.P224(),
	},
	"P-256": {
		mustMarshal(asn1.ObjectIdentifier{1, 2, 840, 10045, 3, 1, 7}),
		elliptic.P256(),
	},
	"P-384": {
		mustMarshal(asn1.ObjectIdentifier{1, 3, 132, 0, 34}),
		elliptic.P384(),
	},
	"P-521": {
		mustMarshal(asn1.ObjectIdentifier{1, 3, 132, 0, 35}),
		elliptic.P521(),
	},

	"SM2": {
		mustMarshal(asn1.ObjectIdentifier{1, 2, 156, 10197, 1, 301}),
		sm2.P256Sm2(),
	},
}

func mustMarshal(val interface{}) []byte {
	b, _ := asn1.Marshal(val)
	return b
}

func unmarshalEcParams(oid []byte) (elliptic.Curve, error) {
	for _, ci := range knownCurves {
		if bytes.Equal(oid, ci.oid) {
			if ci.curve != nil {
				return ci.curve, nil
			}
			return nil, errUnsupportedEllipticCurve
		}
	}
	return nil, errUnsupportedEllipticCurve
}

func unmarshalEcPoint(curve elliptic.Curve, ecPoint []byte) (*big.Int, *big.Int, error) {
	if len(ecPoint) < 64 {
		return nil, nil, errors.New("invalid ecPoint length")
	}
	z, x, y := big.NewInt(0), big.NewInt(0), big.NewInt(0)
	if len(ecPoint) != 64 {
		x, y = elliptic.Unmarshal(curve, ecPoint)
		if x == nil {
			// http://docs.oasis-open.org/pkcs11/pkcs11-curr/v2.40/os/pkcs11-curr-v2.40-os.html#_ftn1
			// PKCS#11 v2.20 specified that the CKA_EC_POINT was to be store in a DER-encoded
			// OCTET STRING.
			var rawEcPoint asn1.RawValue
			if _, err := asn1.Unmarshal(ecPoint, &rawEcPoint); err == nil {
				if len(rawEcPoint.Bytes) > 0 {
					x, y = elliptic.Unmarshal(curve, rawEcPoint.Bytes)
				}
			}
		}
		if x == nil {
			if byte(0x04) == ecPoint[0] && byte(0x04) == ecPoint[2] {
				x, y = elliptic.Unmarshal(curve, ecPoint[2:])
				if x == nil {
					var point = ecPoint[3:]
					x.SetBytes(point[:len(point)/2])
					y.SetBytes(point[len(point)/2:])
				}
			}
		}
	} else {
		x.SetBytes(ecPoint[:len(ecPoint)/2])
		y.SetBytes(ecPoint[len(ecPoint)/2:])
	}

	if x.Cmp(z) == 0 || y.Cmp(z) == 0 {
		return nil, nil, errors.New("ecPoint not a valid ecdsa or sm2 public key point")
	}
	return x, y, nil
}
