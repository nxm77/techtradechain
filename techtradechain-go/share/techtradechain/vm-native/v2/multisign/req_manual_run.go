package multisign

import (
	"fmt"

	"techtradechain.com/techtradechain/pb-go/v2/common"

	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
)

func (r *MultiSignRuntime) reqWithManualRun(
	txSimContext protocol.TxSimContext, parameters map[string][]byte) *common.ContractResult {
	// 1、verify param
	blockVersion := txSimContext.GetBlockVersion()
	sysContractName := parameters[syscontract.MultiReq_SYS_CONTRACT_NAME.String()]
	sysMethod := parameters[syscontract.MultiReq_SYS_METHOD.String()]
	r.log.Infof("multi sign req start. ContractName[%s] Method[%s]", sysContractName, sysMethod)

	if utils.IsAnyBlank(sysContractName, sysMethod) {
		errMsg := "multi req(manual_run) params verify fail. sysContractName/sysMethod cannot be empty"
		r.log.Warn(errMsg)
		return &common.ContractResult{
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
		return &common.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}
	if err := r.supportRule(txSimContext, sysContractName, sysMethod); err != nil {
		r.log.Warn(err)
		return &common.ContractResult{
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
		VoteInfos:    make([]*syscontract.MultiSignVoteInfo, 0, len(tx.Endorsers)),
	}
	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		r.log.Warn(err)
		return &common.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       err.Error(),
			GasUsed:       0,
			ContractEvent: nil,
		}
	}
	var endorsements []*common.EndorsementEntry
	if len(tx.Endorsers) > 0 {
		endorsements, err = r.filterValidEndorsements(ac, tx, multiSignInfo, blockVersion)
		if err != nil {
			r.log.Warn(err)
			return &common.ContractResult{
				Code:          1,
				Result:        nil,
				Message:       err.Error(),
				GasUsed:       0,
				ContractEvent: nil,
			}
		}
	}
	for _, endorsement := range endorsements {
		voteInfo := &syscontract.MultiSignVoteInfo{
			Vote:        syscontract.VoteStatus_AGREE,
			Endorsement: endorsement,
		}
		multiSignInfo.VoteInfos = append(multiSignInfo.VoteInfos, voteInfo)
	}
	_, err = ac.VerifyMultiSignTxPrincipal(multiSignInfo, blockVersion)
	if err != nil {
		r.log.Warnf("verify multi-sign principal failed, err = %v", err)
	}
	// save status
	err = r.saveMultiSignInfo(txSimContext, []byte(tx.Payload.TxId), multiSignInfo)
	if err != nil {
		r.log.Warn(err)
		return &common.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       err.Error(),
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	r.log.Infof("multi sign req end. ContractName[%s] Method[%s], votes count %d",
		sysContractName, sysMethod, len(multiSignInfo.VoteInfos))
	return &common.ContractResult{
		Code:          0,
		Result:        []byte(tx.Payload.TxId),
		Message:       "OK",
		GasUsed:       0,
		ContractEvent: nil,
	}
}

func (r *MultiSignRuntime) filterValidEndorsements(
	ac protocol.AccessControlProvider,
	tx *common.Transaction,
	multiSignInfo *syscontract.MultiSignInfo,
	blockVersion uint32) ([]*common.EndorsementEntry, error) {

	mPayloadByte, _ := multiSignInfo.Payload.Marshal()
	resourceName := multiSignInfo.ContractName + "-" + multiSignInfo.Method
	principal, err := ac.CreatePrincipal(resourceName,
		tx.Endorsers,
		mPayloadByte)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}

	endorsements, err := ac.GetValidEndorsements(principal, blockVersion)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}

	return endorsements, nil
}
