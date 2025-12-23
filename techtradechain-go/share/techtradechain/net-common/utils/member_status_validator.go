/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"techtradechain.com/techtradechain/net-common/common"
	pbac "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
)

// MemberStatusValidateWithCertMode check the member status in the cert mode
func MemberStatusValidateWithCertMode(
	memberStatusValidator *common.MemberStatusValidator,
	certBytes []byte) (chainIds []string, passed bool, err error) {
	m := &pbac.Member{
		OrgId:      "",
		MemberType: pbac.MemberType_CERT,
		MemberInfo: certBytes,
	}
	return memberStatusValidator.ValidateMemberStatus([]*pbac.Member{m})
}

// ChainMemberStatusValidateWithCertMode check the member status in the cert mode with the chain
func ChainMemberStatusValidateWithCertMode(
	chainId string,
	memberStatusValidator *common.MemberStatusValidator,
	certBytes []byte) (passed bool, err error) {
	m := &pbac.Member{
		OrgId:      "",
		MemberType: pbac.MemberType_CERT,
		MemberInfo: certBytes,
	}
	return memberStatusValidator.ValidateMemberStatusWithChain([]*pbac.Member{m}, chainId)
}

// MemberStatusValidateWithPubKeyMode check the member status in the public key mode
func MemberStatusValidateWithPubKeyMode(
	memberStatusValidator *common.MemberStatusValidator,
	pubKeyBytes []byte) (chainIds []string, passed bool, err error) {
	m := &pbac.Member{
		OrgId:      "",
		MemberType: pbac.MemberType_PUBLIC_KEY,
		MemberInfo: pubKeyBytes,
	}
	return memberStatusValidator.ValidateMemberStatus([]*pbac.Member{m})
}

// ChainMemberStatusValidateWithPubKeyMode check the member status in the public key mode with the chain
func ChainMemberStatusValidateWithPubKeyMode(
	chainId string,
	memberStatusValidator *common.MemberStatusValidator,
	pubKeyBytes []byte) (passed bool, err error) {
	m := &pbac.Member{
		OrgId:      "",
		MemberType: pbac.MemberType_PUBLIC_KEY,
		MemberInfo: pubKeyBytes,
	}
	return memberStatusValidator.ValidateMemberStatusWithChain([]*pbac.Member{m}, chainId)
}
