/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io/ioutil"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	bcx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/common/v2/serialize"
	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
	cmutils "techtradechain.com/techtradechain/utils/v2"
)

const (
	// ZXLAddressPrefix define zhixinlian address prefix
	ZXLAddressPrefix = "ZX"
)

// SignPayload sign payload
// Deprecated: use ./utils.MakeEndorserWithPem
func SignPayload(keyPem, certPem []byte, payload *common.Payload) (*common.EndorsementEntry, error) {
	key, err := asym.PrivateKeyFromPEM(keyPem, nil)
	if err != nil {
		return nil, fmt.Errorf("asym.PrivateKeyFromPEM failed, %s", err)
	}

	blockCrt, _ := pem.Decode(certPem)
	if blockCrt == nil {
		return nil, fmt.Errorf("decode pem failed, invalid certificate")
	}
	crt, err := bcx509.ParseCertificate(blockCrt.Bytes)
	if err != nil {
		return nil, fmt.Errorf("bcx509.ParseCertificate failed, %s", err)
	}

	hashalgo, err := bcx509.GetHashFromSignatureAlgorithm(crt.SignatureAlgorithm)
	if err != nil {
		return nil, fmt.Errorf("invalid algorithm: %v", err)
	}

	signature, err := utils.SignPayloadWithHashType(key, hashalgo, payload)
	if err != nil {
		return nil, fmt.Errorf("SignPayload failed, %s", err)
	}

	var orgId string
	if len(crt.Subject.Organization) != 0 {
		orgId = crt.Subject.Organization[0]
	}

	sender := &accesscontrol.Member{
		OrgId:      orgId,
		MemberInfo: certPem,
		MemberType: accesscontrol.MemberType_CERT,
	}

	entry := &common.EndorsementEntry{
		Signer:    sender,
		Signature: signature,
	}

	return entry, nil
}

// SignPayloadWithPath use key/cert file path to sign payload
// Deprecated: SignPayloadWithPath use ./utils.MakeEndorserWithPath instead.
func SignPayloadWithPath(keyFilePath, crtFilePath string, payload *common.Payload) (*common.EndorsementEntry, error) {
	// 读取私钥
	keyBytes, err := ioutil.ReadFile(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("read key file failed, %s", err)
	}

	// 读取证书
	crtBytes, err := ioutil.ReadFile(crtFilePath)
	if err != nil {
		return nil, fmt.Errorf("read crt file failed, %s", err)
	}

	return SignPayload(keyBytes, crtBytes, payload)
}

// GetEVMAddressFromCertPath get evm address from cert file path
func GetEVMAddressFromCertPath(certFilePath string) (string, error) {
	certBytes, err := ioutil.ReadFile(certFilePath)
	if err != nil {
		return "", fmt.Errorf("read cert file [%s] failed, %s", certFilePath, err)
	}

	return GetEVMAddressFromCertBytes(certBytes)
}

// GetEVMAddressFromPrivateKeyPath get evm address from private key file path
func GetEVMAddressFromPrivateKeyPath(privateKeyFilePath string, hashType crypto.HashType) (string, error) {
	keyPem, err := ioutil.ReadFile(privateKeyFilePath)
	if err != nil {
		return "", fmt.Errorf("readFile failed, %s", err.Error())
	}

	return GetEVMAddressFromPrivateKeyBytes(keyPem, hashType)
}

// GetEVMAddressFromCertBytes get evm address from cert bytes
func GetEVMAddressFromCertBytes(certBytes []byte) (string, error) {
	block, _ := pem.Decode(certBytes)
	cert, err := bcx509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("ParseCertificate cert failed, %s", err)
	}

	return cmutils.CertToAddrStr(cert, config.AddrType_ETHEREUM)
}

// GetEVMAddressFromPrivateKeyBytes get evm address from private key bytes
func GetEVMAddressFromPrivateKeyBytes(privateKeyBytes []byte, hashType crypto.HashType) (string, error) {
	privateKey, err := asym.PrivateKeyFromPEM(privateKeyBytes, nil)
	if err != nil {
		return "", fmt.Errorf("PrivateKeyFromPEM failed, %s", err.Error())
	}
	publicKey := privateKey.PublicKey()
	return cmutils.PkToAddrStr(publicKey, config.AddrType_ETHEREUM, hashType)
}

// GetEVMAddressFromPKHex get evm address from public key hex
func GetEVMAddressFromPKHex(pkHex string, hashType crypto.HashType) (string, error) {
	pkDER, err := hex.DecodeString(pkHex)
	if err != nil {
		return "", err
	}
	pk, err := asym.PublicKeyFromDER(pkDER)
	if err != nil {
		return "", fmt.Errorf("fail to resolve public key from DER format: %v", err)
	}
	return cmutils.PkToAddrStr(pk, config.AddrType_ETHEREUM, hashType)
}

// GetEVMAddressFromPKPEM get evm address from public key pem
func GetEVMAddressFromPKPEM(pkPEM string, hashType crypto.HashType) (string, error) {
	pemBlock, _ := pem.Decode([]byte(pkPEM))
	if pemBlock == nil {
		return "", fmt.Errorf("fail to resolve public key from PEM string")
	}
	pkDER := pemBlock.Bytes
	pk, err := asym.PublicKeyFromDER(pkDER)
	if err != nil {
		return "", fmt.Errorf("fail to resolve public key from DER format: %v", err)
	}
	return cmutils.PkToAddrStr(pk, config.AddrType_ETHEREUM, hashType)
}

// EasyCodecItemToParamsMap easy codec items to params map
func (cc *ChainClient) EasyCodecItemToParamsMap(items []*serialize.EasyCodecItem) map[string][]byte {
	return serialize.EasyCodecItemToParamsMap(items)
}

// GetZXAddressFromPKHex get zhixinlian address from public key hex
func GetZXAddressFromPKHex(pkHex string, hashType crypto.HashType) (string, error) {
	pkDER, err := hex.DecodeString(pkHex)
	if err != nil {
		return "", err
	}

	pk, err := asym.PublicKeyFromDER(pkDER)
	if err != nil {
		return "", fmt.Errorf("fail to resolve public key from DER format: %v", err)
	}

	addr, err := cmutils.PkToAddrStr(pk, config.AddrType_ZXL, hashType)
	if err != nil {
		return "", err
	}
	return ZXLAddressPrefix + addr, err
}

// GetZXAddressFromPKPEM get zhixinlian address from public key pem
func GetZXAddressFromPKPEM(pkPEM string, hashType crypto.HashType) (string, error) {
	pemBlock, _ := pem.Decode([]byte(pkPEM))
	if pemBlock == nil {
		return "", fmt.Errorf("fail to resolve public key from PEM string")
	}
	pkDER := pemBlock.Bytes
	pk, err := asym.PublicKeyFromDER(pkDER)
	if err != nil {
		return "", fmt.Errorf("fail to resolve public key from DER format: %v", err)
	}

	addr, err := cmutils.PkToAddrStr(pk, config.AddrType_ZXL, hashType)
	if err != nil {
		return "", err
	}
	return ZXLAddressPrefix + addr, err
}

// GetZXAddressFromCertPEM get zhixinlian address from cert pem
func GetZXAddressFromCertPEM(certPEM string) (string, error) {
	pemBlock, _ := pem.Decode([]byte(certPEM))
	if pemBlock == nil {
		return "", fmt.Errorf("fail to resolve certificate from ")
	}

	cert, err := bcx509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return "", fmt.Errorf("fail to resolve certificate from PEM format: %v", err)
	}
	addr, err := cmutils.CertToAddrStr(cert, config.AddrType_ZXL)
	if err != nil {
		return "", err
	}
	return ZXLAddressPrefix + addr, err
}

// GetZXAddressFromCertPath get zhixinlian address from cert file path
func GetZXAddressFromCertPath(certPath string) (string, error) {
	certContent, err := ioutil.ReadFile(certPath)
	if err != nil {
		return "", fmt.Errorf("fail to load certificate from file [%s]: %v", certPath, err)
	}

	return GetZXAddressFromCertPEM(string(certContent))
}

// GetCMAddressFromPKHex get techtradechain address from public key hex
func GetCMAddressFromPKHex(pkHex string, hashType crypto.HashType) (string, error) {
	pkDER, err := hex.DecodeString(pkHex)
	if err != nil {
		return "", err
	}

	pk, err := asym.PublicKeyFromDER(pkDER)
	if err != nil {
		return "", err
	}

	return cmutils.PkToAddrStr(pk, config.AddrType_TECHTRADECHAIN, hashType)
}

// GetCMAddressFromPKPEM get techtradechain address from public key pem
func GetCMAddressFromPKPEM(pkPEM string, hashType crypto.HashType) (string, error) {
	publicKey, err := asym.PublicKeyFromPEM([]byte(pkPEM))
	if err != nil {
		return "", fmt.Errorf("parse public key failed, %s", err.Error())
	}

	return cmutils.PkToAddrStr(publicKey, config.AddrType_TECHTRADECHAIN, hashType)
}

// GetCMAddressFromCertPEM get techtradechain address from cert pem
func GetCMAddressFromCertPEM(certPEM string) (string, error) {
	pemBlock, _ := pem.Decode([]byte(certPEM))
	if pemBlock == nil {
		return "", fmt.Errorf("fail to resolve certificate from PEM string")
	}
	crt, err := bcx509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return "", fmt.Errorf("get techtradechain address failed, %s", err.Error())
	}

	return cmutils.CertToAddrStr(crt, config.AddrType_TECHTRADECHAIN)
}

// GetCMAddressFromCertPath get techtradechain address from cert file path
func GetCMAddressFromCertPath(certPath string) (string, error) {
	certContent, err := ioutil.ReadFile(certPath)
	if err != nil {
		return "", fmt.Errorf("fail to load certificate from file [%s]: %v", certPath, err)
	}

	return GetCMAddressFromCertPEM(string(certContent))
}
