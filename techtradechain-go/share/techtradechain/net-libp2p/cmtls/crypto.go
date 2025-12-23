/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"crypto/ecdsa"
	"crypto/rand"
	"errors"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/crypto/kms/tencentcloudkms"

	goCrypro "crypto"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym/rsa"
	"github.com/tjfoc/gmsm/sm2"
)

//func encrypt(publicKey interface{}, msg []byte) ([]byte, error) {
//	switch publicKey.(type) {
//	//case *sm2.PublicKey, *ecdsa.PublicKey, *rsa.PublicKey:
//	case *rsa.PublicKey:
//		k, _ := publicKey.(*rsa.PublicKey)
//		return k.Encrypt(msg)
//	case *sm2.PublicKey:
//		k, _ := publicKey.(*sm2.PublicKey)
//		return k.EncryptAsn1(msg, rand.Reader)
//	case crypto.EncryptKey:
//		k, _ := publicKey.(crypto.EncryptKey)
//		return k.Encrypt(msg)
//	case *ecdsa.PublicKey:
//		k, _ := publicKey.(*ecdsa.PublicKey)
//		_ = k.X
//		return nil, nil
//	default:
//		return nil, fmt.Errorf("tls: server's certificate contains an unsupported type of public key: %T", publicKey)
//	}
//}

//func decrypt(privateKey interface{}, msg []byte) ([]byte, error) {
//	switch privateKey.(type) {
//	case *rsa.PrivateKey:
//		k := privateKey.(*rsa.PrivateKey)
//		return k.Decrypt(msg)
//	case *sm2.PrivateKey:
//		k := privateKey.(*sm2.PrivateKey)
//		return k.DecryptAsn1(msg)
//	case crypto.EncryptKey:
//		k := privateKey.(crypto.DecryptKey)
//		return k.Decrypt(msg)
//
//	default:
//		return nil, fmt.Errorf("tls: server's certificate contains an unsupported type of ivate key: %T", privateKey)
//	}
//}

//  sign .
//  @Description:
//  @param privateKey
//  @param msg
//  @return []byte
//  @return error
func sign(privateKey interface{}, msg []byte) ([]byte, error) {
	switch p := privateKey.(type) {
	case *rsa.PrivateKey:
		return p.Sign(msg)
	case *sm2.PrivateKey:
		return p.Sign(rand.Reader, msg, nil)
	case *ecdsa.PrivateKey:
		return p.Sign(rand.Reader, msg, nil)
	case crypto.EncryptKey:
		return nil, fmt.Errorf("wrong type(%T)", privateKey)
	case *tencentcloudkms.Signer:
		return p.Sign(rand.Reader, msg, nil)
	case goCrypro.Signer:
		return p.Sign(rand.Reader, msg, nil)
	default:
		return nil, fmt.Errorf("tls: server's certificate contains an unsupported type of private key: %T",
			privateKey)
	}
}

//  verify .
//  @Description:
//  @param publicKey
//  @param msg
//  @param sign
//  @return error
func verify(publicKey interface{}, msg []byte, sign []byte) error {
	var err error
	switch p := publicKey.(type) {
	case *rsa.PublicKey:
		var ok bool
		ok, err = p.Verify(msg, sign)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("verify false")
		}
		return nil
	case *sm2.PublicKey:
		ok := p.Verify(msg, sign)
		if !ok {
			return errors.New("verify err")
		}
		return nil
	case crypto.PublicKey:
		var ok bool
		ok, err = p.Verify(msg, sign)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("verify false")
		}
		return nil
	case *ecdsa.PublicKey:
		//pk := libp2pcrypto.NewECDSAPublicKey(p)
		//var pk crypto.PublicKey
		//var ok bool
		//ok, err = pk.Verify(msg, sign)
		//if err != nil {
		//	return err
		//}
		ok := ecdsa.VerifyASN1(p, msg, sign)
		if !ok {
			return errors.New("verify false")
		}
		return nil
	default:
		return fmt.Errorf("tls: server's certificate contains an unsupported type of public key: %T", publicKey)
	}
}
