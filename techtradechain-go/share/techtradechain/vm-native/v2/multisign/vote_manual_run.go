package multisign

import (
	"fmt"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
)

// VoteWithManualRun voting on existing multiSign transaction requests
// this will cause some scene below:
// 1) vote agree: cause more than half voters agree
// 2) vote reject: cause half and more voters reject
// 3) vote agree/reject: but don't fulfill above 1) 2) scene
func (r *MultiSignRuntime) VoteWithManualRun(
	txSimContext protocol.TxSimContext,
	parameters map[string][]byte) *commonPb.ContractResult {

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
		errMsg := "multi sign vote(manual_run) params verify fail. voteInfo/txId cannot be empty"
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	// 获取 multiSignInfo
	multiSignInfo, err := r.getMultiSignInfo(txSimContext, txId)
	if err != nil {
		errMsg := err.Error()
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	// check the multiSign status
	if multiSignInfo.Status != syscontract.MultiSignStatus_PROCESSING {
		errMsg := "the status of multiSignInfo is not `PROCESSING`"
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	// verify: has the user voted
	reqVoteInfo := &syscontract.MultiSignVoteInfo{}
	err = proto.Unmarshal(voteInfoBytes, reqVoteInfo)
	if err != nil {
		r.log.Warn(err)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       err.Error(),
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	if reqVoteInfo.Endorsement == nil {
		errMsg := "reqVoteInfo has no `Endorsement`"
		r.log.Warnf(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       err.Error(),
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		r.log.Warn(err)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       err.Error(),
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	// check if the sender has voted before
	if err = r.hasVoted(ac, reqVoteInfo, multiSignInfo, txId); err != nil {
		r.log.Warn(err)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       err.Error(),
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	voteValid, err := r.checkVoteValid(ac, multiSignInfo, reqVoteInfo, blockVersion)
	if !voteValid {
		errMsg := fmt.Sprintf("checkVoteValid failed, err = %v", err)
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	// modify the multiSignInfo
	multiSignInfo.VoteInfos = append(multiSignInfo.VoteInfos, reqVoteInfo)

	_, err = ac.VerifyMultiSignTxPrincipal(multiSignInfo, blockVersion)
	if err != nil {
		r.log.Warnf("verify multi-sign principal failed, err = %v", err)
	}

	err = r.saveMultiSignInfo(txSimContext, txId, multiSignInfo)
	if err != nil {
		r.log.Warn(err)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       err.Error(),
			GasUsed:       0,
			ContractEvent: nil,
		}
	}
	r.log.Infof("multi sign vote[%s] end.", txId)
	return &commonPb.ContractResult{
		Code:          0,
		Result:        []byte("OK"),
		Message:       "OK",
		GasUsed:       0,
		ContractEvent: nil,
	}
}

func (r *MultiSignRuntime) checkVoteValid(ac protocol.AccessControlProvider,
	multiSignInfo *syscontract.MultiSignInfo,
	reqVoteInfo *syscontract.MultiSignVoteInfo,
	blockVersion uint32) (bool, error) {

	// construct a `principal` used for verify sign
	mPayloadByte, _ := multiSignInfo.Payload.Marshal()
	resourceName := multiSignInfo.ContractName + "-" + multiSignInfo.Method
	principal, err := ac.CreatePrincipal(resourceName,
		[]*commonPb.EndorsementEntry{reqVoteInfo.Endorsement},
		mPayloadByte)
	if err != nil {
		r.log.Warn(err)
		return false, err
	}

	// verify sign: check if this vote is a valid vote
	endorsement, err := ac.GetValidEndorsements(principal, blockVersion)
	if err != nil {
		r.log.Warn(err)
		return false, err
	}

	// check is the vote tx is invalid
	if len(endorsement) == 0 {
		if reqVoteInfo.Endorsement.Signer.OrgId != "" {
			err = fmt.Errorf("the multi sign vote signature[org:%s] [member-info:%s] is invalid",
				reqVoteInfo.Endorsement.Signer.OrgId, reqVoteInfo.Endorsement.Signer.MemberInfo)
		} else {
			err = fmt.Errorf("the multi sign vote signature[member-info:%s] is invalid",
				reqVoteInfo.Endorsement.Signer.MemberInfo)
		}
		r.log.Error(err)
		return false, err
	}

	return true, nil
}
