package multisign

import (
	"fmt"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
)

// VoteWithoutManualRun voting on existing multiSign transaction requests
// this will cause some scene below:
// 1) vote agree: cause more than half voters agree
// 2) vote agree/reject: but don't fulfill above 1) scene
func (r *MultiSignRuntime) VoteWithoutManualRun(
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
		errMsg := "multi sign vote(no_manual_run) params verify fail. voteInfo/txId cannot be empty"
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	multiSignInfoBytes, err := txSimContext.Get(contractName, txId)
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
	if multiSignInfoBytes == nil {
		errMsg := fmt.Sprintf("not found tx id[%s]", txId)
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	multiSignInfo := &syscontract.MultiSignInfo{}
	err = proto.Unmarshal(multiSignInfoBytes, multiSignInfo)
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

	multiSignInfo.VoteInfos = append(multiSignInfo.VoteInfos, reqVoteInfo)
	_, err = ac.VerifyMultiSignTxPrincipal(multiSignInfo, blockVersion)
	if err != nil {
		r.log.Warnf("verify multi-sign principal failed, err = %v", err)
	}

	var invokeContractResult *commonPb.ContractResult
	if multiSignInfo.Status == syscontract.MultiSignStatus_PASSED {
		// call contract and prepare invokeContractResult
		invokeContractResult = r.invokeContract(txSimContext, multiSignInfo)
		// make user can know the inner contract call is success or failed.
		if invokeContractResult.Code > 0 {
			invokeContractResult.Message = "DelegationFailed:" + invokeContractResult.Message
		} else {
			invokeContractResult.Message = "DelegationSuccess:" + invokeContractResult.Message
		}
	}

	// record status
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

	r.log.Infof("multi sign vote[%s] end", txId)
	if invokeContractResult != nil {
		invokeContractResult.Code = 0
		return invokeContractResult
	}

	return &commonPb.ContractResult{
		Code:          0,
		Result:        []byte("OK"),
		Message:       "OK",
		GasUsed:       0,
		ContractEvent: nil,
	}
}
