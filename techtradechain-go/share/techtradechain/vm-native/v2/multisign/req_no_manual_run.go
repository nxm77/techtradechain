package multisign

import (
	"fmt"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"

	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
)

func (r *MultiSignRuntime) reqWithoutManualRun(
	txSimContext protocol.TxSimContext, parameters map[string][]byte) *commonPb.ContractResult {
	// 1、verify param
	blockVersion := txSimContext.GetBlockVersion()
	sysContractName := parameters[syscontract.MultiReq_SYS_CONTRACT_NAME.String()]
	sysMethod := parameters[syscontract.MultiReq_SYS_METHOD.String()]
	r.log.Infof("multi sign req start. ContractName[%s] Method[%s]", sysContractName, sysMethod)

	if utils.IsAnyBlank(sysContractName, sysMethod) {
		errMsg := "multi req(no_manual_run) params verify fail. sysContractName/sysMethod cannot be empty"
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}
	if !supportMultiSign(string(sysContractName), string(sysMethod)) {
		errMsg := fmt.Sprintf("multi sign not support %s, only support CONTRACT_MANAGE", sysContractName)
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}
	if err := r.supportRule(txSimContext, sysContractName, sysMethod); err != nil {
		r.log.Warn(err)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       err.Error(),
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	// building multi sign objects
	tx := txSimContext.GetTx()
	multiSignInfo := &syscontract.MultiSignInfo{
		Payload:      tx.Payload,
		ContractName: string(sysContractName),
		Method:       string(sysMethod),
		Status:       syscontract.MultiSignStatus_PROCESSING,
		VoteInfos:    nil,
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

	if len(tx.Endorsers) > 0 {
		endorsements, e := r.filterValidEndorsements(ac, tx, multiSignInfo, blockVersion)
		if e != nil {
			r.log.Warn(e)
			return &commonPb.ContractResult{
				Code:          1,
				Result:        nil,
				Message:       e.Error(),
				GasUsed:       0,
				ContractEvent: nil,
			}
		}

		for _, endorser := range endorsements {
			multiSignInfo.VoteInfos = append(multiSignInfo.VoteInfos, &syscontract.MultiSignVoteInfo{
				Vote:        syscontract.VoteStatus_AGREE,
				Endorsement: endorser,
			})
		}
	}

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

	// save status
	err = r.saveMultiSignInfo(txSimContext, []byte(tx.Payload.TxId), multiSignInfo)
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

	r.log.Infof("multi sign req end. ContractName[%s] Method[%s], votes count %d",
		sysContractName, sysMethod, len(multiSignInfo.VoteInfos))

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
