package multisign220

import (
	"fmt"

	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
)

func (r *MultiSignRuntime) reqWithoutManualRun(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	result []byte, err error) {
	// 1、verify param
	sysContractName := parameters[syscontract.MultiReq_SYS_CONTRACT_NAME.String()]
	sysMethod := parameters[syscontract.MultiReq_SYS_METHOD.String()]
	r.log.Infof("multi sign req start. ContractName[%s] Method[%s]", sysContractName, sysMethod)

	if utils.IsAnyBlank(sysContractName, sysMethod) {
		err = fmt.Errorf("multi req params verify fail. sysContractName/sysMethod cannot be empty")
		return nil, err
	}
	if !supportMultiSign(string(sysContractName), string(sysMethod)) {
		err = fmt.Errorf("multi sign not support %s, only support CONTRACT_MANAGE", sysContractName)
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

	for _, endorser := range tx.Endorsers {
		multiSignInfo.VoteInfos = append(multiSignInfo.VoteInfos, &syscontract.MultiSignVoteInfo{
			Vote:        syscontract.VoteStatus_AGREE,
			Endorsement: endorser,
		})
	}

	// save status
	multiSignInfoBytes, _ := multiSignInfo.Marshal()
	err = txSimContext.Put(contractName, []byte(tx.Payload.TxId), multiSignInfoBytes)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}

	r.log.Infof("multi sign req end. ContractName[%s] Method[%s], votes count %d",
		sysContractName, sysMethod, len(multiSignInfo.VoteInfos))
	return []byte(tx.Payload.TxId), nil
}
