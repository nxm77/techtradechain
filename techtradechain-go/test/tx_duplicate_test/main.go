/*
   Created by guoxin in 2022/2/28 11:49 AM
*/
package main

import (
	"techtradechain.com/techtradechain-go/module/accesscontrol"
	native "techtradechain.com/techtradechain-go/test/chainconfig_test"
	"techtradechain.com/techtradechain/common/v2/ca"
	"techtradechain.com/techtradechain/common/v2/container"
	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/localconf/v2"
	acPb "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	apiPb "techtradechain.com/techtradechain/pb-go/v2/api"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/protocol/v2/test"
	sdkutils "techtradechain.com/techtradechain/sdk-go/v2/utils"
	"techtradechain.com/techtradechain/store/v2"
	"techtradechain.com/techtradechain/store/v2/conf"
	"techtradechain.com/techtradechain/utils/v2"
	"context"
	"encoding/hex"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"log"
	"time"
)

const (
	ChainName = "chain1"

	// errors
	signFailedStr = "sign failed, %s"
	deadLineErr   = "WARN: client.call err: deadline"
)

var (
	// Solo
	caPaths = [][]string{
		{"./config/crypto-config/wx-org1.techtradechain.com/ca"},
	}
	userKeyPaths = []string{
		"./config/crypto-config/wx-org1.techtradechain.com/user/client1/client1.sign.key",
	}
	userCrtPaths = []string{
		"./config/crypto-config/wx-org1.techtradechain.com/user/client1/client1.sign.crt",
	}
	IPs = []string{
		"127.0.0.1",
	}
	// 四节点
	//caPaths = [][]string{
	//	{certPathPrefix + "crypto-config/wx-org1.techtradechain.com/ca"},
	//	{certPathPrefix + "crypto-config/wx-org2.techtradechain.com/ca"},
	//	{certPathPrefix + "crypto-config/wx-org3.techtradechain.com/ca"},
	//	{certPathPrefix + "crypto-config/wx-org4.techtradechain.com/ca"},
	//}
	//userKeyPaths = []string{
	//	certPathPrefix + "crypto-config/wx-org1.techtradechain.com/user/client1/client1.sign.key",
	//	certPathPrefix + "crypto-config/wx-org2.techtradechain.com/user/client1/client1.sign.key",
	//	certPathPrefix + "crypto-config/wx-org3.techtradechain.com/user/client1/client1.sign.key",
	//	certPathPrefix + "crypto-config/wx-org4.techtradechain.com/user/client1/client1.sign.key",
	//}
	//userCrtPaths = []string{
	//	certPathPrefix + "crypto-config/wx-org1.techtradechain.com/user/client1/client1.sign.crt",
	//	certPathPrefix + "crypto-config/wx-org2.techtradechain.com/user/client1/client1.sign.crt",
	//	certPathPrefix + "crypto-config/wx-org3.techtradechain.com/user/client1/client1.sign.crt",
	//	certPathPrefix + "crypto-config/wx-org4.techtradechain.com/user/client1/client1.sign.crt",
	//}
	//orgIds = []string{
	//	"wx-org1.techtradechain.com",
	//	"wx-org2.techtradechain.com",
	//	"wx-org3.techtradechain.com",
	//	"wx-org4.techtradechain.com",
	//}
	//IPs = []string{
	//	"127.0.0.1",
	//	"127.0.0.1",
	//	"127.0.0.1",
	//	"127.0.0.1",
	//}

	Ports = []int{
		12301,
		12302,
		12303,
		12304,
	}

	storeIns protocol.BlockchainStore
)

func main() {
	initStore()
	block, err := storeIns.GetLastBlock()
	if err != nil {
		log.Fatalf("Store error %v", err)
	}
	for i := 0; i < 10; i++ {
		if block == nil {
			log.Println("Store data not found, generator and send tx")
			// 发送交易N次
			sendTx(nil, 100)
		} else {
			// 重复发送交易N次
			// 获取交易
			ids := utils.GetTxIds(block.Txs)
			for {
				if block.Header.BlockHeight == 0 {
					break
				}
				if len(ids) >= 100 {
					break
				}
				block, err := storeIns.GetBlock(block.Header.BlockHeight - 1)
				if err != nil {
					return
				}
				ids = append(ids, utils.GetTxIds(block.Txs)...)
			}
			sendTx(ids, 0)
		}
	}
}

// sendTx 调用合约发送交易
func sendTx(txIds []string, n int) {
	if len(txIds) == 0 {
		for i := 0; i < n; i++ {
			txIds = append(txIds, sdkutils.GetTimestampTxId())
		}
	}
	for i := 0; i < len(txIds); i++ {
		sk, member := getUserSK(1, userKeyPaths[0], userCrtPaths[0])
		resp, err := sendRequest(sk, member, true, &native.InvokeContractMsg{
			TxId:         txIds[i],
			ChainId:      ChainName,
			TxType:       commonPb.TxType_INVOKE_CONTRACT,
			ContractName: "T",
			MethodName:   "P",
		})
		if err == nil {
			log.Printf("end tx resp: code:%d, msg:%s, payload:%+v\n", resp.Code, resp.Message, resp.ContractResult)
			log.Println()
		}
		if statusErr, ok := status.FromError(err); ok && statusErr.Code() == codes.DeadlineExceeded {
			log.Println(deadLineErr)
			log.Println()
		}
	}
}

func initStore() {
	var storeFactory store.Factory // nolint: typecheck
	err := container.Register(func() protocol.Logger { return &test.GoLogger{} }, container.Name("store"))
	if err != nil {
		log.Fatalf("Store Register log %v", err)
	}

	err = container.Register(localconf.TechTradeChainConfig.GetP11Handle)
	if err != nil {
		log.Fatalf("Store Register GetP11Handle %v", err)
	}
	err = container.Register(storeFactory.NewStore,
		container.Parameters(map[int]interface{}{0: ChainName, 1: &conf.StorageConfig{
			StorePath: "../../data/org1/ledgerData1",
			BlockDbConfig: &conf.DbConfig{
				Provider: "leveldb",
				LevelDbConfig: map[string]interface{}{
					"store_path": "../../data/org1/blocks",
				},
			},
			StateDbConfig: &conf.DbConfig{
				Provider: "leveldb",
				LevelDbConfig: map[string]interface{}{
					"store_path": "../../data/org1/statedb",
				},
			},
			HistoryDbConfig: &conf.HistoryDbConfig{
				DbConfig: conf.DbConfig{
					Provider: "leveldb",
					LevelDbConfig: map[string]interface{}{
						"store_path": "../../data/org1/history",
					},
				},
				DisableKeyHistory:      false,
				DisableContractHistory: false,
				DisableAccountHistory:  false,
			},
			ResultDbConfig: &conf.DbConfig{
				Provider: "leveldb",
				LevelDbConfig: map[string]interface{}{
					"store_path": "../../data/org1/result",
				},
			},
			DisableContractEventDB: true,
			Encryptor:              "sm4",
			EncryptKey:             "1234567890123456",
		}}),
		container.DependsOn(map[int]string{2: "store"}),
		container.Name(ChainName))
	if err != nil {
		log.Fatalf("Store Register %v", err)
	}
	err = container.Resolve(&storeIns, container.ResolveName(ChainName))
	if err != nil {
		log.Fatalf("Store Resolve %v", err)
	}
}

// 获取用户私钥
func getUserSK(orgIDNum int, keyPath, certPath string) (crypto.PrivateKey, *acPb.Member) {
	file, err := ioutil.ReadFile(keyPath)
	if err != nil {
		panic(err)
	}
	sk3, err := asym.PrivateKeyFromPEM(file, nil)
	if err != nil {
		panic(err)
	}
	file2, err := ioutil.ReadFile(certPath)
	if err != nil {
		panic(err)
	}
	sender := &acPb.Member{
		OrgId:      fmt.Sprintf("wx-org%d.techtradechain.com", orgIDNum),
		MemberInfo: file2,
		//IsFullCert: true,
	}
	return sk3, sender
}

func sendRequest(sk3 crypto.PrivateKey, sender *acPb.Member, isTls bool, msg *native.InvokeContractMsg) (*commonPb.TxResponse, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Fatalln(err)
		}
	}()

	conn, err := initGRPCConn(isTls, 0)
	if err != nil {
		panic(err)
	}
	client := apiPb.NewRpcNodeClient(conn)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Duration(5*time.Second)))
	defer cancel()
	if msg.TxId == "" {
		msg.TxId = sdkutils.GetTimestampTxId()
	}
	// 构造Header
	header := &commonPb.Payload{
		ChainId: msg.ChainId,
		//Sender:         sender,
		TxType:         msg.TxType,
		TxId:           msg.TxId,
		Timestamp:      time.Now().Unix(),
		ExpirationTime: 0,

		ContractName: msg.ContractName,
		Method:       msg.MethodName,
		Parameters:   msg.Pairs,
	}
	//payloadBytes, err := proto.Marshal(payload)
	//if err != nil {
	//	log.Fatalf(marshalFailedStr, err.Error())
	//}
	req := &commonPb.TxRequest{
		Payload: header,
		Sender:  &commonPb.EndorsementEntry{Signer: sender},
	}
	// 拼接后，计算Hash，对hash计算签名
	rawTxBytes, err := utils.CalcUnsignedTxRequestBytes(req)
	if err != nil {
		log.Fatalf("CalcUnsignedTxRequest failed in sendRequest, %s", err.Error())
	}
	signer := getSigner(sk3, sender)
	signBytes, err := signer.Sign(crypto.CRYPTO_ALGO_SHA256, rawTxBytes)
	if err != nil {
		log.Fatalf(signFailedStr, err.Error())
	}
	fmt.Println(crypto.CRYPTO_ALGO_SHA256, "signBytes"+hex.EncodeToString(signBytes), "rawTxBytes="+hex.EncodeToString(rawTxBytes))
	err = signer.Verify(crypto.CRYPTO_ALGO_SHA256, rawTxBytes, signBytes)
	if err != nil {
		panic(err)
	}
	req.Sender.Signature = signBytes
	fmt.Println(req.Payload.TxId)
	return client.SendRequest(ctx, req)
}

func initGRPCConn(useTLS bool, orgIdIndex int) (*grpc.ClientConn, error) {
	url := fmt.Sprintf("%s:%d", IPs[orgIdIndex], Ports[orgIdIndex])
	fmt.Println(url)
	if useTLS {
		tlsClient := ca.CAClient{
			ServerName: "techtradechain.com",
			CaPaths:    caPaths[orgIdIndex],
			CertFile:   userCrtPaths[orgIdIndex],
			KeyFile:    userKeyPaths[orgIdIndex],
		}
		c, err := tlsClient.GetCredentialsByCA()
		if err != nil {
			log.Fatalf("GetTLSCredentialsByCA err: %v", err)
			return nil, err
		}
		return grpc.Dial(url, grpc.WithTransportCredentials(*c))
	} else {
		return grpc.Dial(url, grpc.WithInsecure())
	}
}

func getSigner(sk3 crypto.PrivateKey, sender *acPb.Member) protocol.SigningMember {
	skPEM, err := sk3.String()
	if err != nil {
		log.Fatalf("get sk PEM failed, %s", err.Error())
	}
	signer, err := accesscontrol.NewCertSigningMember("", sender, skPEM, "")
	if err != nil {
		panic(err)
	}
	return signer
}
