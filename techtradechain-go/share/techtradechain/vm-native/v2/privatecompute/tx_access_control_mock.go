package privatecompute

import (
	"techtradechain.com/techtradechain/common/v2/crypto"
	pbac "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
)

// PrincipalMock mock principal
type PrincipalMock struct {
}

// GetValidEndorsementsLT2330 mock a func of AccessControlProvider
func (ac *ACProviderMock) GetValidEndorsementsLT2330(
	principal protocol.Principal, blockVersion uint32) ([]*common.EndorsementEntry, error) {
	//TODO implement me
	panic("implement me")
}

// VerifyPrincipalLT2330 mock a func of AccessControlProvider
func (ac *ACProviderMock) VerifyPrincipalLT2330(
	principal protocol.Principal, blockVersion uint32) (bool, error) {
	return true, nil
}

// GetResourceName returns resource name of the verification
func (pm *PrincipalMock) GetResourceName() string {
	panic("implement me")
}

// GetEndorsement returns all endorsements (signatures) of the verification
func (pm *PrincipalMock) GetEndorsement() []*common.EndorsementEntry {
	panic("implement me")
}

// GetMessage returns signing data of the verification
func (pm *PrincipalMock) GetMessage() []byte {
	panic("implement me")
}

// GetTargetOrgId returns target organization id of the verification if the verification is for a specific organization
func (pm *PrincipalMock) GetTargetOrgId() string {
	panic("implement me")
}

// ACProviderMock is ac provider mock
type ACProviderMock struct {
}

// GetCertFromCache get cert from cache
func (ac *ACProviderMock) GetCertFromCache(keyBytes []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

// RefineEndorsements verify signature
func (ac *ACProviderMock) RefineEndorsements(endorsements []*common.EndorsementEntry,
	msg []byte) []*common.EndorsementEntry {
	//TODO implement me
	panic("implement me")
}

// GetHashAlg return hash algorithm the access control provider uses
func (ac *ACProviderMock) GetHashAlg() string {
	panic("implement me")
}

// ValidateResourcePolicy checks whether the given resource policy is valid
func (ac *ACProviderMock) ValidateResourcePolicy(resourcePolicy *config.ResourcePolicy) bool {
	panic("implement me")
}

// CreatePrincipal creates a principal for one time authentication
func (ac *ACProviderMock) CreatePrincipal(resourceName string, endorsements []*common.EndorsementEntry,
	message []byte) (protocol.Principal, error) {
	return &PrincipalMock{}, nil
}

// CreatePrincipalForTargetOrg creates a principal for "SELF" type policy,
// which needs to convert SELF to a sepecific organization id in one authentication
func (ac *ACProviderMock) CreatePrincipalForTargetOrg(resourceName string, endorsements []*common.EndorsementEntry,
	message []byte, targetOrgId string) (protocol.Principal, error) {
	panic("implement me")
}

// GetValidEndorsements filters all endorsement entries and returns all valid ones
func (ac *ACProviderMock) GetValidEndorsements(
	principal protocol.Principal, blockVersion uint32) ([]*common.EndorsementEntry, error) {
	panic("implement me")
}

// VerifyMsgPrincipal verifies if the policy for the resource is met
func (ac *ACProviderMock) VerifyMsgPrincipal(principal protocol.Principal, blockVersion uint32) (bool, error) {
	panic("implement me")
}

// VerifyTxPrincipal verifies if the policy for the resource is met
func (ac *ACProviderMock) VerifyTxPrincipal(
	tx *common.Transaction, resourceName string, blockVersion uint32) (bool, error) {
	return true, nil
}

// IsRuleSupportedByMultiSign verify policy of resourceName is supported by multi-sign
func (ac *ACProviderMock) IsRuleSupportedByMultiSign(resourceName string, blockVersion uint32) error {
	panic("implement me")
}

// VerifyMultiSignTxPrincipal verify multi-sign tx should be failed.
func (ac *ACProviderMock) VerifyMultiSignTxPrincipal(
	mInfo *syscontract.MultiSignInfo, blockVersion uint32) (syscontract.MultiSignStatus, error) {
	panic("implement me")
}

// NewMember creates a member from pb Member
func (ac *ACProviderMock) NewMember(member *pbac.Member) (protocol.Member, error) {
	panic("implement me")
}

// GetMemberStatus get the status information of the member
func (ac *ACProviderMock) GetMemberStatus(member *pbac.Member) (pbac.MemberStatus, error) {
	panic("implement me")
}

// VerifyRelatedMaterial verify the member's relevant identity material
func (ac *ACProviderMock) VerifyRelatedMaterial(verifyType pbac.VerifyType, data []byte) (bool, error) {
	panic("implement me")
}

// GetAllPolicy returns all policies
func (ac *ACProviderMock) GetAllPolicy() (map[string]*pbac.Policy, error) {
	panic("implement me")
}

// GetAddressFromCache get address from cache
func (ac *ACProviderMock) GetAddressFromCache(pkBytes []byte) (string, crypto.PublicKey, error) {
	panic("implement me")
}

// GetPayerFromCache get payer from cache
func (ac *ACProviderMock) GetPayerFromCache(key []byte) ([]byte, error) {
	panic("implement me")
}

// SetPayerToCache set payer to cache
func (ac *ACProviderMock) SetPayerToCache(key []byte, value []byte) error {
	panic("implement me")
}
