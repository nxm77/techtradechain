package accountmgr

import (
	"bytes"
	"errors"
	"fmt"

	configPb "techtradechain.com/techtradechain/pb-go/v2/config"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/pb-go/v2/store"

	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
	"github.com/gogo/protobuf/proto"
)

// ParamSeparator the separator splitting the params
const ParamSeparator = ":"

// ReqIDPrefix used for constructing request id keys.
const ReqIDPrefix = "ReqID:"

func isTxSentByCreator(txSimContext protocol.TxSimContext,
	creator *accesscontrol.MemberFull, log protocol.Logger) (bool, error) {

	senderMemberFull, err := common.GetSenderMemberFull(txSimContext, log)
	if err != nil {
		return false, fmt.Errorf("get sender member failed, err = %v", err)
	}
	senderPk, err := common.GetPkFromMemberFull(senderMemberFull)
	if err != nil {
		return false, fmt.Errorf("get sender pk failed, err = %v", err)
	}

	creatorPk, err := common.GetPkFromMemberFull(creator)
	if err != nil {
		return false, fmt.Errorf("get creator pk failed, err = %v", err)
	}
	log.Debugf("creator pk => \n%s\n", creatorPk)
	log.Debugf("sender pk => \n%s\n", senderPk)

	return bytes.Equal(senderPk, creatorPk), nil
}

func constructReqIdKey(reqId string) []byte {
	return []byte(ReqIDPrefix + reqId)
}

type setMethodPayerParams struct {
	syscontract.SetContractMethodPayerParams
	PayerPK []byte
}

func extractParams(msg []byte) (*setMethodPayerParams, error) {

	params := syscontract.SetContractMethodPayerParams{}
	if err := proto.Unmarshal(msg, &params); err != nil {
		return nil, fmt.Errorf("unmarshal SetContractMethodPayerParams failed, err = %v", err)
	}

	if params.RequestId == "" {
		return nil, fmt.Errorf("extract params failed, request id is null")
	}

	return &setMethodPayerParams{
		SetContractMethodPayerParams: params,
	}, nil
}

func extractAndVerifyParams(txSimContext protocol.TxSimContext,
	params map[string][]byte, log protocol.Logger) (*setMethodPayerParams, error) {

	parameters := params[syscontract.SetContractMethodPayer_PARAMS.String()]
	endorsementEntryBytes := params[syscontract.SetContractMethodPayer_ENDORSEMENT.String()]
	if endorsementEntryBytes == nil {
		return nil, errors.New("could not find `ENDORSEMENT_ENTRY` from params")
	}

	// 获取 payer 签名
	endorsementEntry := commonPb.EndorsementEntry{}
	if err := proto.Unmarshal(endorsementEntryBytes, &endorsementEntry); err != nil {
		return nil, fmt.Errorf("unmarshal endorsement_entry failed, err = %v", err)
	}

	// 分离 contractName, method, payer, timestamp 参数
	args, err := extractParams(parameters)
	if err != nil {
		return nil, fmt.Errorf("extract params failed, err = %v", err)
	}
	old, err := txSimContext.Get(syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		constructReqIdKey(args.RequestId))
	if err != nil {
		return nil, fmt.Errorf("find req_id failed, err = %v", err)
	}
	if old != nil {
		return nil, fmt.Errorf("duplicated request, req_id = %v", args.RequestId)
	}

	// 获取公钥
	chainConfig := txSimContext.GetLastChainConfig()
	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		return nil, fmt.Errorf("get ac failed, err = %v", err)
	}
	signerMember, err := ac.NewMember(endorsementEntry.GetSigner())
	if err != nil {
		return nil, fmt.Errorf("failed to create access control member from pb, err = %v", err)
	}
	pk := signerMember.GetPk()
	pkStr, err := pk.String()
	if err != nil {
		return nil, fmt.Errorf("get pk bytes failed, err = %v", err)
	}

	// 验证参数签名
	isSignOK, err := pk.VerifyWithOpts(
		parameters,
		endorsementEntry.GetSignature(),
		&crypto.SignOpts{
			Hash: crypto.HashAlgoMap[chainConfig.Crypto.Hash],
			UID:  crypto.CRYPTO_DEFAULT_UID,
		})
	if err != nil {
		log.Warnf("pk = %v", pkStr)
		log.Warnf("parameters = %s", parameters)
		return nil, fmt.Errorf("public key verify failed, err = %v", err)
	}
	if !isSignOK {
		return nil, errors.New("public key verify got no err, but return false")
	}

	// 验证”签名人“与”代扣地址“是否一致
	address, err := common.PublicKeyToAddress([]byte(pkStr), chainConfig)
	if err != nil {
		return nil, fmt.Errorf("public key => address failed, err = %v", err)
	}
	if address != args.PayerAddress {
		return nil, fmt.Errorf(
			"payerAddress is not match with signer, payerAddress = %v, signer = %v",
			args.PayerAddress, address)
	}

	args.PayerPK = []byte(pkStr)
	return args, nil
}

func (g *AccountManagerRuntime) setMethodPayer(
	txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte, error) {
	var err error

	// 获取参数
	args, err := extractAndVerifyParams(txSimContext, params, g.log)
	if err != nil {
		return nil, err
	}

	g.log.Debugf("args = %#v", args)
	// 获取 Contract 对象
	contract, err := txSimContext.GetContractByName(args.ContractName)
	if err != nil {
		return nil, err
	}
	if contract == nil || contract.GetCreator() == nil {
		return nil, fmt.Errorf("[%s] not found", args.ContractName)
	}
	// 验证 sender == creator
	match, err := isTxSentByCreator(txSimContext, contract.GetCreator(), g.log)
	if err != nil {
		return nil, err
	}
	if !match {
		return nil, fmt.Errorf("the tx sender's pk is not same with the contract creator's pk")
	}

	// 设置 payer
	payerKey := utils.GetContractMethodPayerDbKey(args.ContractName, args.Method)
	g.log.Debugf("set payer: %s => %s", payerKey, args.PayerPK)
	err = txSimContext.Put(
		syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		payerKey,
		args.PayerPK)
	if err != nil {
		return nil, fmt.Errorf("set method `%v` payer failed, err = %v", payerKey, err)
	}
	// 设置 req_id，排重
	err = txSimContext.Put(
		syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		constructReqIdKey(args.RequestId),
		[]byte("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("set req_id failed, err = %v", err)
	}

	return nil, nil
}

func (g *AccountManagerRuntime) unsetMethodPayer(
	txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte, error) {

	// 获取参数
	contractName := string(params[syscontract.UnsetContractMethodPayer_CONTRACT_NAME.String()])
	method := string(params[syscontract.UnsetContractMethodPayer_METHOD.String()])

	// 获取 Contract 对象
	contract, err := txSimContext.GetContractByName(contractName)
	if err != nil {
		return nil, err
	}

	// 验证 sender == creator
	match, err := isTxSentByCreator(txSimContext, contract.GetCreator(), g.log)
	if err != nil {
		return nil, err
	}
	if !match {
		return nil, fmt.Errorf("the tx sender's pk is not same with the contract creator's pk")
	}

	// 清除 payer
	payerKey := utils.GetContractMethodPayerDbKey(contractName, method)
	err = txSimContext.Del(
		syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		payerKey)
	if err != nil {
		return nil, fmt.Errorf("set method `%v` payer failed, err = %v", payerKey, err)
	}

	return nil, nil
}

func (g *AccountManagerRuntime) getMethodPayer(
	txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte, error) {

	// 获取参数
	contractName := string(params[syscontract.GetContractMethodPayer_CONTRACT_NAME.String()])
	method := string(params[syscontract.GetContractMethodPayer_METHOD.String()])
	chainConfig := txSimContext.GetLastChainConfig()

	// 获取 payer
	methodKey := utils.GetContractMethodPayerDbKey(contractName, method)
	payerPK, err := txSimContext.Get(syscontract.SystemContract_ACCOUNT_MANAGER.String(), methodKey)
	if err != nil {
		return nil, fmt.Errorf("get method `%v` payer failed, err = %v", methodKey, err)
	}
	if payerPK == nil {
		return []byte{}, nil
	}

	payerAddress, err := common.PublicKeyToAddress(payerPK, chainConfig)
	if err != nil {
		return nil, fmt.Errorf("public key to address failed, err = %v", err)
	}

	return []byte(payerAddress), nil
}

func getPayerAddrByTx(tx *commonPb.Transaction, ac protocol.AccessControlProvider, cfg *configPb.ChainConfig) ([]byte,
	error) {

	txPayer := tx.GetPayer()
	if txPayer != nil {
		member, err1 := ac.NewMember(txPayer.GetSigner())
		if err1 != nil {
			return nil, fmt.Errorf("failed to create access control member from pb, err = %v", err1)
		}

		addr, err2 := utils.GetStrAddrFromMember(member, cfg.Vm.AddrType)
		if err2 != nil {
			return nil, fmt.Errorf("member to address failed, err = %v", err2)
		}
		//utils.GetStrAddrFromMember计算出的地址已经根据地址类型做了区分，只是至信链地址没有加"ZX"前缀
		if cfg.Vm.AddrType == configPb.AddrType_ZXL {
			addr = "ZX" + addr
		}
		return []byte(addr), nil
	}

	return nil, nil
}

//nolint:gocyclo
func (g *AccountManagerRuntime) getTxPayer(
	txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte, error) {

	// 获取参数
	txId := string(params[syscontract.GetTxPayer_TX_ID.String()])
	chainConfig := txSimContext.GetLastChainConfig()
	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		return nil, fmt.Errorf("get ac failed, err = %v", err)
	}
	g.log.Debugf("getTxPayer was called, txId = %v", txId)

	// 获取 blockstore
	blockStore := txSimContext.GetBlockchainStore()
	txInfo, err := blockStore.GetTxWithInfo(txId)
	if err != nil {
		g.log.Warnf("find tx(%v) failed, err = %v", txId, err)
		return nil, fmt.Errorf("find tx(%v) failed, err = %v", txId, err)
	}
	if txInfo == nil {
		g.log.Warnf("tx `%v` does not exist", txId)
		return nil, fmt.Errorf("tx `%v` does not exist", txId)
	}
	g.log.Debugf("GetTxWithInfo was called, txInfo = %#v", txInfo)

	tx := txInfo.Transaction
	addr, err1 := getPayerAddrByTx(tx, ac, chainConfig)
	if addr != nil && err1 == nil {
		return addr, nil
	}

	txPayload := tx.GetPayload()
	txBlockHeight := txInfo.GetBlockHeight()

	// 以 contractName + method 为 key, 查找与 txId 最近的配置值
	contractMethodPayerDBKey := utils.GetContractMethodPayerDbKey(txPayload.GetContractName(), txPayload.GetMethod())
	iter1, err := blockStore.GetHistoryForKey(
		syscontract.SystemContract_ACCOUNT_MANAGER.String(), contractMethodPayerDBKey)
	if err != nil {
		g.log.Warnf("find history for key(%v) failed, err = %v", contractMethodPayerDBKey, err)
		return nil, fmt.Errorf("find history for key(%v) failed, err = %v", contractMethodPayerDBKey, err)
	}
	defer iter1.Release()
	g.log.Debugf("get method iterator, iter = %v", iter1)

	var contractNameMethodPayerPK []byte
	var contractNamePayerPK []byte

	var valueMod1 *store.KeyModification
	minBlockHeight := uint64(0)
	if iter1 != nil {
		for iter1.Next() {
			valueMod1, err = iter1.Value()
			if err != nil {
				return nil, fmt.Errorf("iter history data failed, err = %v", err)
			}
			blockHeight := valueMod1.GetBlockHeight()
			g.log.Infof("[item]: block height = %v, PK => %s", blockHeight, valueMod1.Value)
			if blockHeight < txBlockHeight && blockHeight > minBlockHeight {
				minBlockHeight = blockHeight
				if valueMod1.IsDelete {
					contractNameMethodPayerPK = nil
				} else {
					contractNameMethodPayerPK = valueMod1.Value
				}
			}
		}
	}
	g.log.Debugf("method iterator end, payer = %s", contractNameMethodPayerPK)

	if contractNameMethodPayerPK == nil {
		// 以 contractName 为 key, 查找与 txId 最近的配置值
		contractPayerDBKey := utils.GetContractMethodPayerDbKey(txPayload.GetContractName(), "")
		iter2, err2 := blockStore.GetHistoryForKey(
			syscontract.SystemContract_ACCOUNT_MANAGER.String(), contractPayerDBKey)
		if err2 != nil {
			g.log.Warnf("find history for key(%v) failed, err = %v", contractPayerDBKey, err)
			return nil, fmt.Errorf("find history for key(%v) failed, err = %v", contractPayerDBKey, err)
		}
		defer iter2.Release()
		g.log.Debugf("get contract iterator, iter = %v", iter2)

		var valueMod2 *store.KeyModification
		minBlockHeight = uint64(0)
		if iter2 != nil {
			for iter2.Next() {
				valueMod2, err = iter2.Value()
				if err != nil {
					return nil, fmt.Errorf("iter history data failed, err = %v", err)
				}
				blockHeight := valueMod2.GetBlockHeight()
				g.log.Infof("[item]: block height = %v, PK => %s", blockHeight, valueMod2.Value)
				if blockHeight < txBlockHeight && blockHeight > minBlockHeight {
					minBlockHeight = blockHeight
					if valueMod2.IsDelete {
						contractNamePayerPK = nil
					} else {
						contractNamePayerPK = valueMod2.Value
					}
				}
			}
		}
	}

	// 找到处理 tx 时使用的 payerPK
	var payerPK []byte
	if contractNameMethodPayerPK != nil {
		payerPK = contractNameMethodPayerPK
	} else if contractNamePayerPK != nil {
		payerPK = contractNamePayerPK
	} else {
		sender := tx.GetSender().GetSigner()
		g.log.Infof("sender = %v", sender)
		member, err2 := ac.NewMember(sender)
		if err2 != nil {
			return nil, fmt.Errorf("failed to create access control member from pb, err = %v", err2)
		}

		pk, err2 := member.GetPk().String()
		if err2 != nil {
			return nil, fmt.Errorf("get public kry failed, err = %v", err2)
		}
		payerPK = []byte(pk)
	}
	g.log.Debugf("payer PK = %s", payerPK)

	payerAddr, err := common.PublicKeyToAddress(payerPK, chainConfig)
	if err != nil {
		return nil, fmt.Errorf("PublicKeyToAddress failed, err = %v", err)
	}

	return []byte(payerAddr), nil
}
