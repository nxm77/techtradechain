package multisign2320

import (
	"fmt"

	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
)

func (r *MultiSignRuntime) reqWithManualRun(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	result []byte, err error) {
	// 1、verify param
	sysContractName := parameters[syscontract.MultiReq_SYS_CONTRACT_NAME.String()]
	sysMethod := parameters[syscontract.MultiReq_SYS_METHOD.String()]
	blockVersion := txSimContext.GetBlockVersion()
	r.log.Infof("multi sign req start. ContractName[%s] Method[%s]", sysContractName, sysMethod)

	if utils.IsAnyBlank(sysContractName, sysMethod) {
		err = fmt.Errorf("multi req params verify fail. sysContractName/sysMethod cannot be empty")
		return nil, err
	}
	if !supportMultiSign(string(sysContractName), string(sysMethod)) {
		err = fmt.Errorf("multi sign not support %s, only support CONTRACT_MANAGE", sysContractName)
		return nil, err
	}
	if err = r.supportRule(txSimContext, sysContractName, sysMethod); err != nil {
		return nil, err
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
		return nil, err
	}

	if len(tx.Endorsers) > 0 {
		// construct a `principal` used for verify sign
		mPayloadByte, _ := multiSignInfo.Payload.Marshal()
		resourceName := multiSignInfo.ContractName + "-" + multiSignInfo.Method
		principal, err2 := ac.CreatePrincipal(resourceName,
			tx.Endorsers,
			mPayloadByte)
		if err2 != nil {
			r.log.Warn(err2)
			return nil, err2
		}

		// verify sign: check if this vote is a valid vote
		endorsersAgreed, err2 := ac.GetValidEndorsements(principal, blockVersion)
		if err2 != nil {
			r.log.Warn(err2)
			return nil, err2
		}

		if ok, err2 := r.verifySignature(ac, resourceName, endorsersAgreed, mPayloadByte, blockVersion); err2 != nil {
			// 验证`同意`签名异常：退出，不记录
			r.log.Debugf("verify agree signature failed. err = %v", err2)

		} else if ok {
			// 验证`同意`签名成功，修改多签状态，退出
			r.log.Debugf("verify agree signature success. result => `passed`")
			multiSignInfo.Status = syscontract.MultiSignStatus_PASSED

			for _, endorser := range endorsersAgreed {
				multiSignInfo.VoteInfos = append(multiSignInfo.VoteInfos, &syscontract.MultiSignVoteInfo{
					Vote:        syscontract.VoteStatus_AGREE,
					Endorsement: endorser,
				})
			}
		} else {
			for _, endorser := range endorsersAgreed {
				multiSignInfo.VoteInfos = append(multiSignInfo.VoteInfos, &syscontract.MultiSignVoteInfo{
					Vote:        syscontract.VoteStatus_AGREE,
					Endorsement: endorser,
				})
			}
		}
	}

	// save status
	err = r.saveMultiSignInfo(txSimContext, []byte(tx.Payload.TxId), multiSignInfo)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}

	r.log.Infof("multi sign req end. ContractName[%s] Method[%s], votes count %d",
		sysContractName, sysMethod, len(multiSignInfo.VoteInfos))
	return []byte(tx.Payload.TxId), nil
}
