/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

import (
	acPb "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
)

// MemberGetter member getter interface
type MemberGetter interface {
	// GetFullMemberInfo 根据CERT_HASH获得完整的Cert，根据DID获得DID Document
	GetFullMemberInfo(memberId []byte, mtype acPb.MemberType) ([]byte, error)
}

// GetMemberPubKeySA get member public key
func GetMemberPubKeySA(member *acPb.Member, getter MemberGetter) ([]byte, uint32) {
	return []byte("pubkey"), uint32(member.MemberType)
}

//func GetMemberPubKeySA(member *accesscontrol.Member,getter MemberGetter) ([]byte,uint32){
//	return []byte("pubkey"),member.SignatureAlgorithm
//}
