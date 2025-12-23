/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

import (
	"encoding/hex"
	"fmt"

	"github.com/tjfoc/gmsm/sm3"

	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	acPb "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/protocol/v2"

	"techtradechain.com/techtradechain/common/v2/cert"
	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/pb-go/v2/config"
)

//generateAddrInt create big.Int address
func generateAddrInt(data []byte, addrType config.AddrType) (addr *evmutils.Int, err error) {
	var str string
	if addrType == config.AddrType_ZXL {
		str, err = evmutils.ZXAddress(data)
		addr = evmutils.FromHexString(str[2:])
	} else {
		//TECHTRADECHAIN and ETHEREUM have the same algorithm, but different parameters--the data before serialization
		addr = evmutils.MakeAddress(data)
	}

	return addr, err
}

//generateAddrStr create string address
func generateAddrStr(data []byte, addrType config.AddrType) (addr string, err error) {
	if addrType == config.AddrType_ZXL {
		addr, err = evmutils.ZXAddress(data)
		addr = addr[2:]
	} else {
		//TECHTRADECHAIN and ETHEREUM have the same algorithm, but different parameters--the data before serialization
		bytesAddr := evmutils.Keccak256(data)
		addr = hex.EncodeToString(bytesAddr)[24:]
	}

	return addr, err
}

func antiCollision(name string, addrType config.AddrType, blockVersion uint32) []byte {
	if blockVersion < 2300 {
		return []byte(name)
	}

	if addrType == config.AddrType_ZXL {
		sm3Hash := sm3.New()
		sm3Hash.Write([]byte(name))
		return sm3Hash.Sum(nil)
	}

	return evmutils.Keccak256([]byte(name))
}

//NameToAddrInt create big.Int address by name
func NameToAddrInt(name string, addrType config.AddrType, blockVersion uint32) (*evmutils.Int, error) {
	data := antiCollision(name, addrType, blockVersion)
	return generateAddrInt(data, addrType)
}

//NameToAddrStr create string address by name
func NameToAddrStr(name string, addrType config.AddrType, blockVersion uint32) (string, error) {
	data := antiCollision(name, addrType, blockVersion)
	return generateAddrStr(data, addrType)
}

//PkToAddrInt create big.Int address by pk
func PkToAddrInt(pk crypto.PublicKey, addrType config.AddrType, hashType crypto.HashType) (*evmutils.Int, error) {
	//calculate address by SKI when AddrType is TECHTRADECHAIN, and by public key when AddrType is ZXL or ETHEREUM
	if addrType == config.AddrType_TECHTRADECHAIN {
		ski, err := cert.ComputeSKI(hashType, pk.ToStandardKey())
		if err != nil {
			return nil, err
		}
		return generateAddrInt(ski, addrType)
	}

	pkBytes, err := evmutils.MarshalPublicKey(pk)
	if err != nil {
		return nil, err
	}

	if addrType == config.AddrType_ETHEREUM {
		return generateAddrInt(pkBytes[1:], addrType)
	}

	return generateAddrInt(pkBytes, addrType)
}

//PkToAddrStr crete string address by pk
func PkToAddrStr(pk crypto.PublicKey, addrType config.AddrType, hashType crypto.HashType) (string, error) {
	//calculate address by SKI when AddrType is TECHTRADECHAIN, and by public key when AddrType is ZXL or ETHEREUM
	if addrType == config.AddrType_TECHTRADECHAIN {
		ski, err := cert.ComputeSKI(hashType, pk.ToStandardKey())
		if err != nil {
			return "", err
		}
		return generateAddrStr(ski, addrType)
	}

	pkBytes, err := evmutils.MarshalPublicKey(pk)
	if err != nil {
		return "", err
	}

	if addrType == config.AddrType_ETHEREUM {
		return generateAddrStr(pkBytes[1:], addrType)
	}

	return generateAddrStr(pkBytes, addrType)
}

//CertToAddrInt create big.Int address by certificate
func CertToAddrInt(cert *x509.Certificate, addrType config.AddrType) (*evmutils.Int, error) {
	//calculate address by SKI when AddrType is TECHTRADECHAIN, and by public key when AddrType is ZXL or ETHEREUM
	if addrType == config.AddrType_TECHTRADECHAIN {
		return generateAddrInt(cert.SubjectKeyId, addrType)
	}

	pkBytes, err := evmutils.MarshalPublicKey(cert.PublicKey)
	if err != nil {
		return nil, err
	}

	if addrType == config.AddrType_ETHEREUM {
		return generateAddrInt(pkBytes[1:], addrType)
	}

	return generateAddrInt(pkBytes, addrType)
}

//CertToAddrStr create string address by certificate
func CertToAddrStr(cert *x509.Certificate, addrType config.AddrType) (string, error) {
	//calculate address by SKI when AddrType is TECHTRADECHAIN, and by public key when AddrType is ZXL or ETHEREUM
	if addrType == config.AddrType_TECHTRADECHAIN {
		return generateAddrStr(cert.SubjectKeyId, addrType)
	}

	pkBytes, err := evmutils.MarshalPublicKey(cert.PublicKey)
	if err != nil {
		return "", err
	}

	if addrType == config.AddrType_ETHEREUM {
		return generateAddrStr(pkBytes[1:], addrType)
	}

	return generateAddrStr(pkBytes, addrType)
}

////getProtocolMember get protocol member from pb-go member
//func getProtocolMember(pbMember *acPb.Member, txCtx protocol.TxSimContext) (protocol.Member, error) {
//	ac, err := txCtx.GetAccessControl()
//	if err != nil {
//		return nil, err
//	}
//
//	member, err := ac.NewMember(pbMember)
//	if err != nil {
//		return nil, err
//	}
//
//	return member, nil
//}

//ParsePkFromPbMember parse public key from member
func ParsePkFromPbMember(member *acPb.Member) (crypto.PublicKey, error) {
	if member.MemberType == acPb.MemberType_PUBLIC_KEY {
		return asym.PublicKeyFromPEM(member.MemberInfo)
	}

	if member.MemberType == acPb.MemberType_CERT {
		cert, err := ParseCert(member.MemberInfo)
		if err != nil {
			return nil, err
		}
		return cert.PublicKey, nil
	}

	return nil, fmt.Errorf("parse failed, type[%s] not support parse public key", member.MemberType.String())
}

//GetIntAddrFromPbMember calculate Int address from pb member
func GetIntAddrFromPbMember(pbMember *acPb.Member, addrType config.AddrType, hashType crypto.HashType) (*evmutils.Int,
	error) {
	if pbMember.MemberType == acPb.MemberType_ADDR {
		//senderAddr = evmutils.FromHexString(string(parameters[syscontract.CrossParams_SENDER.String()]))
		return evmutils.FromHexString(string(pbMember.MemberInfo)), nil
	}

	if pbMember.MemberType == acPb.MemberType_PUBLIC_KEY {
		pk, err := ParsePkFromPbMember(pbMember)
		if err != nil {
			return nil, err
		}
		return PkToAddrInt(pk, addrType, hashType)
	}

	if pbMember.MemberType == acPb.MemberType_CERT {
		cert, err := ParseCert(pbMember.MemberInfo)
		if err != nil {
			return nil, err
		}
		return CertToAddrInt(cert, addrType)
	}

	return nil, fmt.Errorf("get addr failed, type[%s] not support", pbMember.MemberType.String())
}

//GetStrAddrFromPbMember calculate string address from pb Member
func GetStrAddrFromPbMember(pbMember *acPb.Member, addrType config.AddrType, hashType crypto.HashType) (string, error) {
	if pbMember.MemberType == acPb.MemberType_ADDR {
		//senderAddr = evmutils.FromHexString(string(parameters[syscontract.CrossParams_SENDER.String()]))
		return string(pbMember.MemberInfo), nil
	}

	if pbMember.MemberType == acPb.MemberType_PUBLIC_KEY {
		pk, err := ParsePkFromPbMember(pbMember)
		if err != nil {
			return "", err
		}
		return PkToAddrStr(pk, addrType, hashType)
	}

	if pbMember.MemberType == acPb.MemberType_CERT {
		cert, err := ParseCert(pbMember.MemberInfo)
		if err != nil {
			return "", err
		}
		return CertToAddrStr(cert, addrType)
	}

	return "", fmt.Errorf("get addr failed, type[%s] not support", pbMember.MemberType.String())
}

//GetIntAddrFromMember calculate Int address from protocol member
func GetIntAddrFromMember(member protocol.Member, addrType config.AddrType) (*evmutils.Int, error) {
	pbMember, err := member.GetMember()
	if err != nil {
		return nil, err
	}

	if pbMember.MemberType == acPb.MemberType_ADDR {
		return evmutils.FromHexString(string(pbMember.MemberInfo)), nil
	}

	//AddrType_TECHTRADECHAIN calculating address by uid, and AddrType_ZXL/ETHEREUM by public key
	if addrType == config.AddrType_TECHTRADECHAIN {
		ski, err1 := evmutils.FromHex(member.GetUid())
		if err1 != nil {
			return nil, err1
		}

		return generateAddrInt(ski, addrType)
	}

	pkBytes, err2 := evmutils.MarshalPublicKey(member.GetPk())
	if err2 != nil {
		return nil, err2
	}

	if addrType == config.AddrType_ETHEREUM {
		return generateAddrInt(pkBytes[1:], addrType)
	}

	return generateAddrInt(pkBytes, addrType)
}

//GetStrAddrFromMember calculate string address from protocol member
func GetStrAddrFromMember(member protocol.Member, addrType config.AddrType) (string, error) {
	pbMember, err := member.GetMember()
	if err != nil {
		return "", err
	}

	if pbMember.MemberType == acPb.MemberType_ADDR {
		return string(pbMember.MemberInfo), nil
	}

	if addrType == config.AddrType_TECHTRADECHAIN {
		data, err1 := evmutils.FromHex(member.GetUid())
		if err1 != nil {
			return "", err1
		}

		address := evmutils.Keccak256(data)
		addr := hex.EncodeToString(address)[24:]
		return addr, nil
	}

	pkBytes, err2 := evmutils.MarshalPublicKey(member.GetPk())
	if err2 != nil {
		return "", err2
	}

	if addrType == config.AddrType_ETHEREUM {
		return generateAddrStr(pkBytes[1:], addrType)
	}

	return generateAddrStr(pkBytes, addrType)
}
