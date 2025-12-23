package multisign2320

import (
	"fmt"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
)

// VoteWithoutManualRun voting on existing multiSign transaction requests
// 	this will cause some scene below:
//  1) vote agree: cause more than half voters agree
//  2) vote agree/reject: but don't fulfill above 1) scene
func (r *MultiSignRuntime) VoteWithoutManualRun(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	result []byte, event []*commonPb.ContractEvent, err error) {
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
		return nil, nil, err
	}

	multiSignInfoBytes, err := txSimContext.Get(contractName, txId)
	if err != nil {
		r.log.Warn(err)
		return nil, nil, err
	}
	if multiSignInfoBytes == nil {
		return nil, nil, fmt.Errorf("not found tx id[%s]", txId)
	}

	multiSignInfo := &syscontract.MultiSignInfo{}
	err = proto.Unmarshal(multiSignInfoBytes, multiSignInfo)
	if err != nil {
		r.log.Warn(err)
		return nil, nil, err
	}

	// verify: has the user voted
	reqVoteInfo := &syscontract.MultiSignVoteInfo{}
	err = proto.Unmarshal(voteInfoBytes, reqVoteInfo)
	if err != nil {
		r.log.Warn(err)
		return nil, nil, err
	}
	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		r.log.Warn(err)
		return nil, nil, err
	}
	if err = r.hasVoted(ac, reqVoteInfo, multiSignInfo, txId); err != nil {
		r.log.Warn(err)
		return nil, nil, err
	}

	// verify: sign

	// get multiSignInfo
	mPayloadByte, _ := multiSignInfo.Payload.Marshal()
	resourceName := multiSignInfo.ContractName + "-" + multiSignInfo.Method
	principal, err := ac.CreatePrincipal(resourceName,
		[]*commonPb.EndorsementEntry{reqVoteInfo.Endorsement},
		mPayloadByte)
	if err != nil {
		r.log.Warn(err)
		return nil, nil, err
	}

	// construct principal used for verify
	endorsement, err := ac.GetValidEndorsements(principal, blockVersion)
	if err != nil {
		r.log.Warn(err)
		return nil, nil, err
	}
	if len(endorsement) == 0 {
		err = fmt.Errorf("the multi sign vote signature[org:%s] is invalid",
			reqVoteInfo.Endorsement.Signer.OrgId)
		r.log.Error(err)
		return nil, nil, err
	}

	// update multiSignInfo
	multiSignInfo.VoteInfos = append(multiSignInfo.VoteInfos, reqVoteInfo)

	// verify: multi sign
	endorsers := make([]*commonPb.EndorsementEntry, 0)
	for _, info := range multiSignInfo.VoteInfos {
		if info.Vote == syscontract.VoteStatus_AGREE {
			endorsers = append(endorsers, info.Endorsement)
		}
	}

	// check the multi sign tx should be executed.
	if len(endorsers) != 0 {
		if ok, err2 := r.verifySignature(ac, resourceName, endorsers, mPayloadByte, blockVersion); err2 != nil {
			return nil, nil, err2
		} else if ok {
			r.log.Infof("multi sign vote [org:%s] verify success, currently %d valid signatures are collected",
				reqVoteInfo.Endorsement.Signer.OrgId, len(endorsers))
			// call contract and set status
			contractResult := r.invokeContract(txSimContext, multiSignInfo)
			event = contractResult.ContractEvent
		}
		//} else {
		// do nothing
		// maybe: authentication fail not enough participants support this action:
		// 3 valid endorsements required, 1 valid endorsements received
	}

	// record status
	multiSignInfoBytes, err = multiSignInfo.Marshal()
	if err != nil {
		r.log.Error(err)
		return nil, nil, err
	}
	err = txSimContext.Put(contractName, txId, multiSignInfoBytes)
	if err != nil {
		r.log.Error(err)
		return nil, nil, err
	}
	r.log.Infof("multi sign vote[%s] end", txId)
	return []byte("OK"), event, nil
}
