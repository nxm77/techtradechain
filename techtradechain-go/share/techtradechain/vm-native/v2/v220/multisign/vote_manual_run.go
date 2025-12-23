package multisign220

import (
	"fmt"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
)

// VoteWithManualRun voting on existing multiSign transaction requests
// 	this will cause some scene below:
//  1) vote agree: cause more than half voters agree
//  2) vote reject: cause half and more voters reject
//  3) vote agree/reject: but don't fulfill above 1) 2) scene
func (r *MultiSignRuntime) VoteWithManualRun(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	result []byte, err error) {

	// 1、verify param
	// 2、get history vote record
	// 3、judge vote authority
	// 4、change vote status
	// 5、call actual native contract

	voteInfoBytes := parameters[syscontract.MultiVote_VOTE_INFO.String()]
	txId := parameters[syscontract.MultiVote_TX_ID.String()]
	blockVersion := txSimContext.GetBlockVersion()
	r.log.Infof("multi sign vote start. MultiVote_TX_ID[%s]", txId)

	if utils.IsAnyBlank(voteInfoBytes, txId) {
		err = fmt.Errorf("multi sign vote params verify fail. voteInfo/txId cannot be empty")
		r.log.Warn(err)
		return nil, err
	}

	// 获取 multiSignInfo
	multiSignInfo, err := r.getMultiSignInfo(txSimContext, txId)
	if err != nil {
		return nil, err
	}

	// check the multiSign status
	if multiSignInfo.Status != syscontract.MultiSignStatus_PROCESSING {
		return nil, fmt.Errorf("the status of multiSignInfo is not `PROCESSING`")
	}

	// verify: has the user voted
	reqVoteInfo := &syscontract.MultiSignVoteInfo{}
	err = proto.Unmarshal(voteInfoBytes, reqVoteInfo)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}

	if reqVoteInfo.Endorsement == nil {
		err = fmt.Errorf("reqVoteInfo has no `Endorsement`")
		r.log.Warnf("reqVoteInfo has no `Endorsement` field => %v", reqVoteInfo)
		return nil, err
	}

	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}

	// check if the sender has voted before
	if err = r.hasVoted(ac, reqVoteInfo, multiSignInfo, txId); err != nil {
		r.log.Warn(err)
		return nil, err
	}

	// construct a `principal` used for verify sign
	mPayloadByte, _ := multiSignInfo.Payload.Marshal()
	resourceName := multiSignInfo.ContractName + "-" + multiSignInfo.Method
	principal, err := ac.CreatePrincipal(resourceName,
		[]*commonPb.EndorsementEntry{reqVoteInfo.Endorsement},
		mPayloadByte)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}

	// verify sign: check if this vote is a valid vote
	endorsement, err := ac.GetValidEndorsements(principal, blockVersion)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}

	// check is the vote tx is invalid
	if len(endorsement) == 0 {
		err = fmt.Errorf("the multi sign vote signature[org:%s] is invalid",
			reqVoteInfo.Endorsement.Signer.OrgId)
		r.log.Error(err)
		return nil, err
	}
	// modify the multiSignInfo
	multiSignInfo.VoteInfos = append(multiSignInfo.VoteInfos, reqVoteInfo)

	// split the voters into agreed array and rejected array
	endorsersAgreed, endorsersRejected := getVoteInfo(multiSignInfo)
	r.log.Debugf("endorsers agreed num => %v", len(endorsersAgreed))
	r.log.Debugf("endorsers rejected num => %v", len(endorsersRejected))

	// handle multi sign agree
	if len(endorsersAgreed) != 0 {
		result, err = r.handleAgree(
			txId, txSimContext, multiSignInfo,
			ac, resourceName,
			endorsersAgreed, mPayloadByte)
		// 处理 error or 1) scene，返回
		if err != nil || result != nil {
			r.log.Infof("multi sign vote[%s] end.", txId)
			return result, err
		}
	}
	// handle multi sign reject
	if len(endorsersRejected) != 0 {
		result, err = r.handleReject(
			txId, txSimContext, multiSignInfo,
			ac, resourceName,
			endorsersRejected, mPayloadByte)
		// 处理 error or 2) scene，返回
		if err != nil || result != nil {
			r.log.Infof("multi sign vote[%s] end.", txId)
			return result, err
		}
	}

	result, err = r.handleUnreachable(
		txId, txSimContext, multiSignInfo)
	if err != nil || result != nil {
		r.log.Infof("multi sign vote[%s] end.", txId)
		return result, err
	}

	// 3) scene
	// 计数增加
	err = r.saveMultiSignInfo(txSimContext, txId, multiSignInfo)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}
	r.log.Infof("multi sign vote[%s] end.", txId)
	return []byte("OK"), nil
}

// handleReject check is half or more of voters have rejected this request.
// 	this will cause some scene below:
//  1) verify failed: this make this vote tx invalid
//  2) verify passed: this make this multi sign tx status changed.
//  3）verify not passed, but the reject number reach the half of total voters' number
//  4) verify not passed: this increase the number of reject
func (r *MultiSignRuntime) handleReject(
	txId []byte,
	txSimContext protocol.TxSimContext,
	multiSignInfo *syscontract.MultiSignInfo,
	ac protocol.AccessControlProvider,
	resourceName string,
	endorsersRejected []*commonPb.EndorsementEntry,
	mPayloadByte []byte) ([]byte, error) {

	var err error
	blockVersion := txSimContext.GetBlockVersion()
	if ok, err2 := r.verifySignature(ac, resourceName, endorsersRejected, mPayloadByte, blockVersion); err2 != nil {
		// 验证`拒绝`签名异常：退出，不记录
		r.log.Debugf("verify reject signature failed. err = %v", err2)
		return nil, err2

	} else if ok {
		// 超过半数的签名`拒绝`：修改多签状态，保存
		r.log.Debugf("verify reject signature success. result => `rejected`")
		multiSignInfo.Status = syscontract.MultiSignStatus_REFUSED

		// save multiSignInfo
		err = r.saveMultiSignInfo(txSimContext, txId, multiSignInfo)
		if err != nil {
			r.log.Warn(err)
			return nil, err
		}
		return []byte("OK"), nil

	}

	r.log.Debugf("verify reject signature success. result => `no enough reject vote`")
	return nil, nil
}

// handleAgree check is more than half of voters have agreed on this request.
// when calling this function: the `enable_manual_run` flag mut be true.
// 	this will cause some scene below:
//  1) verify failed: this make this vote tx invalid
//  2) verify passed: this make this multi sign tx status changed.
//  3) verify not passed: this increase the number of agree
func (r *MultiSignRuntime) handleAgree(
	txId []byte,
	txSimContext protocol.TxSimContext,
	multiSignInfo *syscontract.MultiSignInfo,
	ac protocol.AccessControlProvider,
	resourceName string,
	endorsersAgreed []*commonPb.EndorsementEntry,
	mPayloadByte []byte) ([]byte, error) {

	var err error
	blockVersion := txSimContext.GetBlockVersion()
	if ok, err2 := r.verifySignature(ac, resourceName, endorsersAgreed, mPayloadByte, blockVersion); err2 != nil {
		// 验证`同意`签名异常：退出，不记录
		r.log.Debugf("verify agree signature failed. err = %v", err2)
		return nil, err2

	} else if ok {
		// 验证`同意`签名成功，修改多签状态，退出
		r.log.Debugf("verify agree signature success. result => `passed`")
		multiSignInfo.Status = syscontract.MultiSignStatus_PASSED

		// save multiSignInfo
		err = r.saveMultiSignInfo(txSimContext, txId, multiSignInfo)
		if err != nil {
			r.log.Warn(err)
			return nil, err
		}
		r.log.Infof("multi sign vote[%s] end", txId)
		return []byte("OK"), nil

	}

	// 验证`同意`签名失败：进入验证失败签名流程
	r.log.Debugf("verify agree signature success. result => `no enough agree vote`")
	return nil, nil
}

func (r *MultiSignRuntime) handleUnreachable(
	txId []byte,
	txSimContext protocol.TxSimContext,
	multiSignInfo *syscontract.MultiSignInfo) ([]byte, error) {

	lastChainConfig, err := txSimContext.GetBlockchainStore().GetLastChainConfig()
	if err != nil {
		err2 := fmt.Errorf("get chain config failed, err = %v", err)
		r.log.Warn(err2)
		return nil, err2
	}

	// 刚好半数签名`拒绝`：进入保存
	totalVotes := len(lastChainConfig.GetTrustRoots())
	if lastChainConfig.AuthType == protocol.Public {
		totalVotes = len(lastChainConfig.GetTrustRoots()[0].Root)
	}
	if len(multiSignInfo.VoteInfos) == totalVotes {
		r.log.Debugf("no voter can vote again, change status to `refused`")
		multiSignInfo.Status = syscontract.MultiSignStatus_REFUSED

		// save multiSignInfo
		err = r.saveMultiSignInfo(txSimContext, txId, multiSignInfo)
		if err != nil {
			r.log.Warn(err)
			return nil, err
		}
		return []byte("OK"), nil

	}
	r.log.Debugf("verify reject signature success. result => `wait other vote`")
	return nil, nil
}

func getVoteInfo(multiSignInfo *syscontract.MultiSignInfo) (
	[]*commonPb.EndorsementEntry, []*commonPb.EndorsementEntry) {

	endorsersAgreed := make([]*commonPb.EndorsementEntry, 0)
	endorsersRejected := make([]*commonPb.EndorsementEntry, 0)
	for _, info := range multiSignInfo.VoteInfos {
		if info.Vote == syscontract.VoteStatus_AGREE {
			endorsersAgreed = append(endorsersAgreed, info.Endorsement)
		} else if info.Vote == syscontract.VoteStatus_REJECT {
			endorsersRejected = append(endorsersRejected, info.Endorsement)
		}
	}
	return endorsersAgreed, endorsersRejected
}
