/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	guuid "github.com/google/uuid"

	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"techtradechain.com/techtradechain/common/v2/random/uuid"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
	"github.com/gogo/protobuf/proto"
)

// CalcUnsignedTxBytes calculate unsigned transaction bytes [request payload bytes]
func CalcUnsignedTxBytes(t *commonPb.Transaction) ([]byte, error) {
	if t == nil {
		return nil, errors.New("calc unsigned tx bytes error, tx == nil")
	}
	return t.Payload.Marshal()
}

// CalcUnsignedTxRequestBytes calculate unsigned transaction request bytes
func CalcUnsignedTxRequestBytes(txReq *commonPb.TxRequest) ([]byte, error) {
	if txReq == nil {
		return nil, errors.New("calc unsigned tx request bytes error, tx == nil")
	}
	return txReq.Payload.Marshal()
}

// CalcUnsignedCompleteTxBytes calculate unsigned complete transaction bytearray
//func CalcUnsignedCompleteTxBytes(t *commonPb.Transaction) ([]byte, error) {
//	if t == nil {
//		return nil, errors.New("calc unsigned complete tx bytes error, tx == nil")
//	}
//	headerBytes, err := proto.Marshal(t.Header)
//	if err != nil {
//		return nil, err
//	}
//	resultBytes, err := proto.Marshal(t.Result)
//	if err != nil {
//		return nil, err
//	}
//	completeTxBytes := bytes.Join([][]byte{headerBytes, t.RequestPayload, resultBytes}, []byte{})
//	return completeTxBytes, nil
//}

// CalcTxHash calculate transaction hash, include tx.Payload, tx.signature, tx.Payload, tx.Result
func CalcTxHash(hashType string, t *commonPb.Transaction) ([]byte, error) {
	//txBytes, err := CalcUnsignedCompleteTxBytes(t)
	txBytes, err := t.Marshal()
	if err != nil {
		return nil, err
	}

	hashedTx, err := hash.GetByStrType(hashType, txBytes)
	if err != nil {
		return nil, err
	}
	return hashedTx, nil
}

// CalcTxHashWithVersion use version to judge if set tx gasUsed to zero when calc tx hash
func CalcTxHashWithVersion(hashType string, t *commonPb.Transaction, version int) ([]byte, error) {
	if version >= 2300 {
		if t.Result != nil && t.Result.ContractResult != nil {
			gasUsed := t.Result.ContractResult.GasUsed
			defer func() {
				t.Result.ContractResult.GasUsed = gasUsed
			}()

			t.Result.ContractResult.GasUsed = 0
		}
		return CalcTxHash(hashType, t)
	}

	return CalcTxHash(hashType, t)
}

// CalcTxRequestHash calculate hash of transaction request
func CalcTxRequestHash(hashType string, t *commonPb.Transaction) ([]byte, error) {
	txBytes, err := CalcUnsignedTxBytes(t)
	if err != nil {
		return nil, err
	}

	hashedTx, err := hash.GetByStrType(hashType, txBytes)
	if err != nil {
		return nil, err
	}
	return hashedTx, nil
}

// CalcTxResultHash calculate hash of transaction result
func CalcTxResultHash(hashType string, result *commonPb.Result) ([]byte, error) {
	resultBytes, err := CalcResultBytes(result)
	if err != nil {
		return nil, err
	}
	resultHash, err := hash.GetByStrType(hashType, resultBytes)
	if err != nil {
		return nil, err
	}
	return resultHash, nil
}

// CalcResultBytes get bytearray of result
func CalcResultBytes(result *commonPb.Result) ([]byte, error) {
	if result == nil {
		return nil, errors.New("calculate result bytes error, result == nil")
	}
	tmpGas := result.ContractResult.GasUsed
	result.ContractResult.GasUsed = 0
	resultBytes, err := proto.Marshal(result)
	result.ContractResult.GasUsed = tmpGas
	if err != nil {
		return nil, err
	}
	return resultBytes, nil
}

// IsManageContractAsConfigTx Whether the Manager Contract is considered a configuration transaction
func IsManageContractAsConfigTx(tx *commonPb.Transaction, enableSqlDB bool) bool {
	if tx == nil {
		return false
	}
	return enableSqlDB && IsContractMgmtTx(tx)
}

//IsContractMgmtTx 是否是合约安装、升级的交易
func IsContractMgmtTx(tx *commonPb.Transaction) bool {
	payload := tx.Payload

	return payload.ContractName == syscontract.SystemContract_CONTRACT_MANAGE.String() &&
		(payload.Method == syscontract.ContractManageFunction_INIT_CONTRACT.String() ||
			payload.Method == syscontract.ContractManageFunction_UPGRADE_CONTRACT.String())
}

// IsConfigTx the transaction is a config transaction or not
// separate blocks are required
func IsConfigTx(tx *commonPb.Transaction) bool {
	if tx == nil || tx.Payload == nil {
		return false
	}
	return tx.Payload.ContractName == syscontract.SystemContract_CHAIN_CONFIG.String()
}

// IsManagementTx the transaction is a management transaction or not
// separate blocks are required
func IsManagementTx(tx *commonPb.Transaction) bool {
	if tx == nil {
		return false
	}
	return tx.Payload.ContractName == syscontract.SystemContract_CERT_MANAGE.String() ||
		tx.Payload.ContractName == syscontract.SystemContract_MULTI_SIGN.String() ||
		tx.Payload.ContractName == syscontract.SystemContract_PUBKEY_MANAGE.String() ||
		tx.Payload.ContractName == syscontract.SystemContract_CONTRACT_MANAGE.String() ||
		(tx.Payload.ContractName == syscontract.SystemContract_ACCOUNT_MANAGER.String() &&
			tx.Payload.Method == syscontract.GasAccountFunction_SET_CONTRACT_METHOD_PAYER.String()) ||
		(tx.Payload.ContractName == syscontract.SystemContract_ACCOUNT_MANAGER.String() &&
			tx.Payload.Method == syscontract.GasAccountFunction_UNSET_CONTRACT_METHOD_PAYER.String())
}

// IsCertManagementTx the transaction is a cert management transaction or not
func IsCertManagementTx(tx *commonPb.Transaction) bool {
	if tx == nil {
		return false
	}
	return tx.Payload.ContractName == syscontract.SystemContract_CERT_MANAGE.String()
}

// IsMultiSignManagementTx the transaction is a multi sign management transaction or not
func IsMultiSignManagementTx(tx *commonPb.Transaction) bool {
	if tx == nil {
		return false
	}
	return tx.Payload.ContractName == syscontract.SystemContract_MULTI_SIGN.String()
}

// IsPubKeyManagementTx the transaction is a pub key management transaction or not
func IsPubKeyManagementTx(tx *commonPb.Transaction) bool {
	if tx == nil {
		return false
	}
	return tx.Payload.ContractName == syscontract.SystemContract_PUBKEY_MANAGE.String()
}

// IsContractManagementTx the transaction is a contract management transaction or not
func IsContractManagementTx(tx *commonPb.Transaction) bool {
	if tx == nil {
		return false
	}
	return tx.Payload.ContractName == syscontract.SystemContract_CONTRACT_MANAGE.String()
}

// IsValidConfigTx the transaction is a valid config transaction or not
func IsValidConfigTx(tx *commonPb.Transaction) bool {
	if !IsConfigTx(tx) {
		return false
	}

	if tx.Result == nil || tx.Result.Code != commonPb.TxStatusCode_SUCCESS ||
		tx.Result.ContractResult == nil || tx.Result.ContractResult.Result == nil {
		return false
	}

	return true
}

//IsNativeTx check tx is a native contract invoke tx
func IsNativeTx(tx *commonPb.Transaction) (isNative bool, contractName string) {
	if tx == nil || tx.Payload == nil {
		return false, ""
	}
	txType := tx.Payload.TxType
	switch txType {
	case commonPb.TxType_INVOKE_CONTRACT:
		payload := tx.Payload
		return IsNativeContract(payload.ContractName), payload.ContractName
	default:
		return false, ""
	}
}

// GetRandTxId return hex string format random transaction id with length = 64
func GetRandTxId() string {
	return uuid.GetUUID() + uuid.GetUUID()
}

// GetTimestampTxId return hex string format random transaction id with length = 64
// GetTimestampTxId by current time, see: GetTimestampTxIdByNano
// eg: 687dca1d9c4fdf1652fdfc072182654f53622c496aa94c05b47d34263cd99ec9
func GetTimestampTxId() string {
	return GetTimestampTxIdByNano(time.Now().UnixNano())
}

// GetTimestampTxIdByNano nanosecond TODO TxId is generated linearly according to Nano
func GetTimestampTxIdByNano(nano int64) string {
	b := make([]byte, 16, 32)
	binary.BigEndian.PutUint64(b, uint64(nano))
	b[8] = 202
	/*
		Read generates len(p) random bytes from the default Source and
		writes them into p. It always returns len(p) and a nil error.
		Read, unlike the Rand.Read method, is safe for concurrent use.
	*/
	// nolint: gosec
	_, _ = rand.Read(b[9:16])
	u := guuid.New()
	b = append(b, u[:]...)
	return hex.EncodeToString(b)
}

// GetTxIdWithSeed return tx-id with seed
func GetTxIdWithSeed(seed int64) string {
	return uuid.GetUUIDWithSeed(seed) + uuid.GetUUIDWithSeed(seed)
}

// CalcTxVerifyWorkers calculate work size of transaction verify
func CalcTxVerifyWorkers(txCount int) int {
	if txCount>>12 > 0 {
		// more than 4095, then use 100 workers
		return 100
	} else if txCount>>11 > 0 {
		// more than 2047, then use 50 workers
		return 50
	} else if txCount>>10 > 0 {
		// more than 1023, then use 20 workers
		return 20
	} else if txCount>>8 > 0 {
		// more than 255, then use 10 workers
		return 10
	} else if txCount>>7 > 0 {
		// more than 127, then use 8 workers
		return 8
	} else if txCount>>5 > 0 {
		// more than 31, then use 5 workers
		return 5
	}
	// else use only 1 worker
	return 1
}

// DispatchTxVerifyTask dispatch transaction verify task
func DispatchTxVerifyTask(txs []*commonPb.Transaction) map[int][]*commonPb.Transaction {
	txCount := len(txs)
	batchCount := CalcTxVerifyWorkers(txCount)
	batchSize := txCount / batchCount
	batch := make(map[int][]*commonPb.Transaction)
	for i := 0; i < batchCount-1; i++ {
		batch[i] = txs[i*batchSize : i*batchSize+batchSize]
	}
	batch[batchCount-1] = txs[(batchCount-1)*batchSize:]
	return batch
}

// GetTxIds get tx ids
func GetTxIds(txs []*commonPb.Transaction) []string {
	ret := make([]string, len(txs))
	for i, tx := range txs {
		ret[i] = tx.Payload.TxId
	}
	return ret
}

// GetTxResourceName return the resource name this tx want to operate
func GetTxResourceName(tx *commonPb.Transaction) string {
	return tx.Payload.ContractName + "-" + tx.Payload.Method
}

// VerifyTxWithoutPayload verify a transaction with access control provider.
//The payload of the transaction will not be verified.
func VerifyTxWithoutPayload(tx *commonPb.Transaction, chainId string, ac protocol.AccessControlProvider,
	blockVersion uint32) error {
	if tx == nil {
		return errors.New("tx is nil")
	}
	if err := verifyTxHeader(tx.Payload, chainId); err != nil {
		return fmt.Errorf("verify tx header failed, %s", err)
	}
	if err := verifyTxAuth(tx, ac, blockVersion); err != nil {
		return fmt.Errorf("verify tx authentation failed, %s", err)
	}
	return nil
}

// verify transaction header
func verifyTxHeader(header *commonPb.Payload, targetChainId string) error {
	defaultTxIdLen := 64 // txId的长度
	// 1. header not null
	if header == nil {
		return errors.New("tx header is nil")
	}
	// 2. chain id matches target chain id
	if header.ChainId != targetChainId {
		return fmt.Errorf("chain id [%s] is incorrect, wanted [%s]", header.ChainId, targetChainId)
	}
	// 3. tx id length is 64
	if len(header.TxId) > defaultTxIdLen {
		return fmt.Errorf("tx id length is incorrect, wanted %d", defaultTxIdLen)
	}
	//only invoke contract tx need check txid
	if header.TxType == commonPb.TxType_INVOKE_CONTRACT {
		// 4. tx id only contains [a-z0-9]
		match := CheckTxIDFormat(header.TxId)
		if !match {
			return errors.New("check tx id failed, only [a-zA-Z0-9_] are allowed, your TxId is:" + header.TxId)
		}
	}
	// 5. timestamp (in seconds) before expiration time
	if header.ExpirationTime != 0 && header.ExpirationTime <= header.Timestamp {
		return fmt.Errorf("tx timestamp %d should be before expiration time %d", header.Timestamp, header.ExpirationTime)
	}
	// 6. sender should not be nil
	//if header.Sender == nil || header.Sender.OrgId == "" || header.Sender.MemberInfo == nil {
	//	return fmt.Errorf("tx sender is nil")
	//}
	return nil
}

// verify transaction sender's authentication (include signature verification,
//cert-chain verification, access verification)
func verifyTxAuth(t *commonPb.Transaction, ac protocol.AccessControlProvider, blockVersion uint32) error {
	resourceName := GetTxResourceName(t)
	allowed, err := ac.VerifyTxPrincipal(t, resourceName, blockVersion)
	if err != nil {
		return err
	}

	if !allowed {
		return fmt.Errorf("authentication failed")
	}

	return nil
}

// GenerateInstallContractPayload generate install contract payload
func GenerateInstallContractPayload(contractName, version string, runtimeType commonPb.RuntimeType, bytecode []byte,
	initParameters []*commonPb.KeyValuePair) (*commonPb.Payload, error) {
	var pairs []*commonPb.KeyValuePair
	pairs = append(pairs, &commonPb.KeyValuePair{
		Key:   syscontract.InitContract_CONTRACT_NAME.String(),
		Value: []byte(contractName),
	})
	pairs = append(pairs, &commonPb.KeyValuePair{
		Key:   syscontract.InitContract_CONTRACT_VERSION.String(),
		Value: []byte(version),
	})
	pairs = append(pairs, &commonPb.KeyValuePair{
		Key:   syscontract.InitContract_CONTRACT_RUNTIME_TYPE.String(),
		Value: []byte(runtimeType.String()),
	})
	pairs = append(pairs, &commonPb.KeyValuePair{
		Key:   syscontract.InitContract_CONTRACT_BYTECODE.String(),
		Value: bytecode,
	})
	pairs = append(pairs, initParameters...)
	payload := &commonPb.Payload{
		ContractName: syscontract.SystemContract_CONTRACT_MANAGE.String(),
		Method:       syscontract.ContractManageFunction_INIT_CONTRACT.String(),
		Parameters:   pairs,
	}
	return payload, nil
}

// GetRoleFromTx get role from tx
func GetRoleFromTx(tx *commonPb.Transaction, ac protocol.AccessControlProvider) (protocol.Role, error) {

	var member protocol.Member
	var err error
	member, err = ac.NewMember(tx.Sender.Signer)

	if err != nil {
		return "", err
	}

	return member.GetRole(), nil
}

// RearrangeRWSet rearrange rwSetMap into slice by block.Txs order
func RearrangeRWSet(block *commonPb.Block, rwSetMap map[string]*commonPb.TxRWSet) []*commonPb.TxRWSet {
	rwSet := make([]*commonPb.TxRWSet, 0)
	if rwSetMap == nil {
		return rwSet
	}
	for _, tx := range block.Txs {
		if set, ok := rwSetMap[tx.Payload.TxId]; ok {
			rwSet = append(rwSet, set)
		}
	}
	return rwSet
}
