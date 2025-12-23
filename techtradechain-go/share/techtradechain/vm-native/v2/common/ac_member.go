/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Package common is package for common
package common

import (
	"fmt"

	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/protocol/v2"
)

// GetSenderMemberFull return MemberFull data identifying the sender of tx
func GetSenderMemberFull(txSimContext protocol.TxSimContext, log protocol.Logger) (*accesscontrol.MemberFull, error) {
	sender := txSimContext.GetSender()
	if txSimContext.GetBlockVersion() == 220 { //兼容历史数据的问题
		return &accesscontrol.MemberFull{
			OrgId:      sender.GetOrgId(),
			MemberType: sender.GetMemberType(),
			MemberInfo: sender.GetMemberInfo(),
		}, nil
	}
	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		log.Warn(err)
		return nil, err
	}

	member, err := ac.NewMember(sender)
	if err != nil {
		log.Warn(err)
		return nil, err
	}
	memberFull := &accesscontrol.MemberFull{
		OrgId:      sender.GetOrgId(),
		MemberType: sender.GetMemberType(),
		MemberInfo: sender.GetMemberInfo(),
		MemberId:   member.GetMemberId(),
		Role:       string(member.GetRole()),
		Uid:        member.GetUid(),
	}
	return memberFull, nil
}

// GetPkFromMemberFull get the public key bytes of memberFull
func GetPkFromMemberFull(memberFull *accesscontrol.MemberFull) ([]byte, error) {
	var (
		pk  []byte
		err error
	)
	switch memberFull.MemberType {
	case accesscontrol.MemberType_CERT, accesscontrol.MemberType_ALIAS, accesscontrol.MemberType_CERT_HASH:
		pk, err = publicKeyFromCert(memberFull.MemberInfo)
		if err != nil {
			return nil, err
		}

	case accesscontrol.MemberType_PUBLIC_KEY:
		pk = memberFull.MemberInfo
	default:
		err = fmt.Errorf("invalid member type: %s", memberFull.MemberType)
		return nil, err
	}

	return pk, nil
}
