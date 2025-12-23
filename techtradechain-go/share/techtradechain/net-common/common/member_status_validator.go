/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	"sync"

	pbac "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/protocol/v2"
)

// MemberStatusValidator is a validator for validating member status.
type MemberStatusValidator struct {
	accessControls sync.Map
	blockedPeerIds sync.Map
}

// NewMemberStatusValidator create a new MemberStatusValidator instance.
func NewMemberStatusValidator() *MemberStatusValidator {
	return &MemberStatusValidator{}
}

// AddPeerId Add a pid to blocked list.
func (v *MemberStatusValidator) AddPeerId(pid string) {
	v.blockedPeerIds.LoadOrStore(pid, struct{}{})
}

// RemovePeerId remove pid given from blocked list.
func (v *MemberStatusValidator) RemovePeerId(pid string) {
	v.blockedPeerIds.Delete(pid)
}

// ContainsPeerId return whether pid given exist in blocked list.
func (v *MemberStatusValidator) ContainsPeerId(pid string) bool {
	_, ok := v.blockedPeerIds.Load(pid)
	return ok
}

// AddAC Add access control of chain to validator.
func (v *MemberStatusValidator) AddAC(chainId string, ac protocol.AccessControlProvider) {
	v.accessControls.LoadOrStore(chainId, ac)
}

// DeleteAC delete access control of chain to validator.
func (v *MemberStatusValidator) DeleteAC(chainId string) {
	v.accessControls.Delete(chainId)
}

// ValidateMemberStatus check the status of members.
func (v *MemberStatusValidator) ValidateMemberStatus(members []*pbac.Member) ([]string, bool, error) {
	chainIdList := make([]string, 0, 1)
	bl := false
	v.accessControls.Range(func(key, value interface{}) bool {
		chainId, ok := key.(string)
		if !ok {
			return false
		}
		ac, _ := value.(protocol.AccessControlProvider)
		if ac == nil {
			return false
		}
		allOk := true
		for _, member := range members {
			s, _ := ac.GetMemberStatus(member)
			if s == pbac.MemberStatus_INVALID || s == pbac.MemberStatus_FROZEN || s == pbac.MemberStatus_REVOKED {
				allOk = false
				break
			}
		}
		if allOk {
			bl = true
			chainIdList = append(chainIdList, chainId)
		}
		return true
	})

	return chainIdList, bl, nil
}

// ValidateMemberStatusWithChain check the status of members with the access control of the chain named chainId given.
func (v *MemberStatusValidator) ValidateMemberStatusWithChain(members []*pbac.Member, chainId string) (bool, error) {
	value, ok := v.accessControls.Load(chainId)
	if !ok {
		return false, nil
	}
	ac, _ := value.(protocol.AccessControlProvider)
	if ac == nil {
		return false, nil
	}
	for _, member := range members {
		s, _ := ac.GetMemberStatus(member)
		if s == pbac.MemberStatus_INVALID || s == pbac.MemberStatus_FROZEN || s == pbac.MemberStatus_REVOKED {
			return false, nil
		}
	}
	return true, nil
}
