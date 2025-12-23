/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"techtradechain.com/techtradechain/common/v2/ca"
	"techtradechain.com/techtradechain/pb-go/v2/archivecenter"
	archivePb "techtradechain.com/techtradechain/pb-go/v2/archivecenter"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
	"google.golang.org/grpc"
)

var _ ArchiveService = (*ArchiveCenterGrpcClient)(nil)

// ArchiveCenterGrpcClient 归档服务的归档中心GRPC客户端实现
type ArchiveCenterGrpcClient struct {
	logger      utils.Logger
	conn        *grpc.ClientConn
	client      archivePb.ArchiveCenterServerClient
	maxSendSize int
	config      *ArchiveCenterConfig
}

// GetChainConfigByBlockHeight 获得链配置
// @param blockHeight
// @return *config.ChainConfig
// @return error
func (a *ArchiveCenterGrpcClient) GetChainConfigByBlockHeight(blockHeight uint64) (*config.ChainConfig, error) {
	block, err := a.GetBlockByHeight(blockHeight, false)
	if err != nil {
		return nil, err
	}
	if block.Block.Header.BlockType == common.BlockType_CONFIG_BLOCK {
		return getChainConfig(block.Block.Txs[0])
	}
	h := block.Block.Header.PreConfHeight
	block, err = a.GetBlockByHeight(h, false)
	if err != nil {
		return nil, err
	}
	return getChainConfig(block.Block.Txs[0])
}

// getChainConfig 解析Result，成为ChainConfig
// @param tx
// @return *config.ChainConfig
// @return error
func getChainConfig(tx *common.Transaction) (*config.ChainConfig, error) {
	var cfg *config.ChainConfig
	err := cfg.Unmarshal(tx.Result.ContractResult.Result)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// NewArchiveCenterGrpcClient 构造ArchiveCenterGrpcClient
// @param config
// @param log
// @return *ArchiveCenterGrpcClient
func NewArchiveCenterGrpcClient(config *ArchiveCenterConfig, log utils.Logger) *ArchiveCenterGrpcClient {
	client, err := initGrpcClient(config, log)
	if err != nil {
		panic(err.Error())
	}
	return client
}

// ArchiveCenterConfig 获得配置参数
// @return *ArchiveCenterConfig
func (a *ArchiveCenterGrpcClient) ArchiveCenterConfig() *ArchiveCenterConfig {
	return a.config
}

// GrpcCallOption Grpc选项
// @return []grpc.CallOption
func (client *ArchiveCenterGrpcClient) GrpcCallOption() []grpc.CallOption {
	return []grpc.CallOption{grpc.MaxCallSendMsgSize(client.maxSendSize)}
}

// Close 关闭连接
// @return error
func (client *ArchiveCenterGrpcClient) Close() error {
	return client.conn.Close()
}

func initGrpcClient(archiveCenterCFG *ArchiveCenterConfig, log utils.Logger) (*ArchiveCenterGrpcClient, error) {
	// check archiveCenterCFG not nil
	if archiveCenterCFG == nil {
		return nil, fmt.Errorf("check sdk config ,make sure open archive_center_config segments")
	}
	var retClient ArchiveCenterGrpcClient
	retClient.maxSendSize = archiveCenterCFG.MaxSendMsgSize * 1024 * 1024
	retClient.logger = log
	var dialOptions []grpc.DialOption
	if archiveCenterCFG.TlsEnable {
		var caFiles []string
		for _, pemFile := range archiveCenterCFG.Tls.TrustCaList {
			rootBuf, rootError := ioutil.ReadFile(pemFile)
			if rootError != nil {
				return nil, fmt.Errorf("read trust-ca-list file %s got error %s",
					pemFile, rootError.Error())
			}
			caFiles = append(caFiles, string(rootBuf))
		}
		tlsClient := ca.CAClient{
			ServerName: archiveCenterCFG.Tls.ServerName,
			CertFile:   archiveCenterCFG.Tls.CertFile,
			KeyFile:    archiveCenterCFG.Tls.PrivKeyFile,
			CaCerts:    caFiles,
		}
		creds, credsErr := tlsClient.GetCredentialsByCA()
		if credsErr != nil {
			return nil, fmt.Errorf("GetCredentialsByCA error %s", credsErr.Error())
		}
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(*creds))
	} else {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}
	dialOptions = append(dialOptions,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(
				archiveCenterCFG.MaxRecvMsgSize*1024*1024)))
	conn, connErr := grpc.Dial(
		archiveCenterCFG.RpcAddress,
		dialOptions...)
	if connErr != nil {
		return nil, fmt.Errorf("dial grpc error %s", connErr.Error())
	}
	retClient.conn = conn
	retClient.client = archivePb.NewArchiveCenterServerClient(conn)
	retClient.config = archiveCenterCFG
	return &retClient, nil
}

// GetTxByTxId 根据TxId获得交易
// @param txId
// @return *common.TransactionInfo
// @return error
func (a *ArchiveCenterGrpcClient) GetTxByTxId(txId string) (*common.TransactionInfo, error) {
	ctx, ctxCancel := context.WithTimeout(context.Background(),
		time.Duration(a.config.ReqeustSecondLimit)*time.Second)
	defer ctxCancel()
	txResp, err := a.client.GetTxByTxId(ctx,
		&archivePb.BlockByTxIdRequest{
			ChainUnique: a.config.ChainGenesisHash,
			TxId:        txId,
		}, a.GrpcCallOption()...)
	if err != nil {
		return nil, err
	}
	if txResp.Transaction == nil {
		return nil, errors.New("no such transaction")
	}
	blk, blkError := a.GetBlockByTxId(txId, true)
	if blkError != nil {
		return nil, blkError
	}
	if blk == nil {
		return nil, errors.New("no such block by tx id")
	}
	txIndex := 0
	for i := 0; i < len(blk.Block.Txs); i++ {
		if blk.Block.Txs[i].Payload.TxId == txId {
			txIndex = i
			break
		}
	}
	return &common.TransactionInfo{
		Transaction:    txResp.Transaction,
		BlockHeight:    blk.Block.Header.BlockHeight,
		BlockHash:      blk.Block.Header.BlockHash,
		TxIndex:        uint32(txIndex),
		BlockTimestamp: int64(blk.Block.Header.BlockTimestamp),
	}, nil
}

// GetTxWithRWSetByTxId 根据TxId获得交易和读写集
// @param txId
// @return *common.TransactionInfoWithRWSet
// @return error
func (a *ArchiveCenterGrpcClient) GetTxWithRWSetByTxId(txId string) (*common.TransactionInfoWithRWSet, error) {
	ctx, ctxCancel := context.WithTimeout(context.Background(),
		time.Duration(a.config.ReqeustSecondLimit)*time.Second)
	defer ctxCancel()
	txResp, err := a.client.GetTxByTxId(ctx,
		&archivePb.BlockByTxIdRequest{
			ChainUnique: a.config.ChainGenesisHash,
			TxId:        txId,
		}, a.GrpcCallOption()...)
	if err != nil {
		return nil, err
	}
	if txResp.Transaction == nil {
		return nil, errors.New("no such transaction")
	}
	blk, blkError := a.GetBlockByTxId(txId, true)
	if blkError != nil {
		return nil, blkError
	}
	if blk == nil {
		return nil, errors.New("no such block by tx id")
	}
	txIndex := 0
	for i := 0; i < len(blk.Block.Txs); i++ {
		if blk.Block.Txs[i].Payload.TxId == txId {
			txIndex = i
			break
		}
	}
	rwSetResp, err := a.client.GetTxRWSetByTxId(ctx,
		&archivePb.BlockByTxIdRequest{
			ChainUnique: a.config.ChainGenesisHash,
			TxId:        txId,
		}, a.GrpcCallOption()...)
	if err != nil {
		return nil, err
	}
	return &common.TransactionInfoWithRWSet{
		Transaction:    txResp.Transaction,
		BlockHeight:    blk.Block.Header.BlockHeight,
		BlockHash:      blk.Block.Header.BlockHash,
		TxIndex:        uint32(txIndex),
		BlockTimestamp: blk.Block.Header.BlockTimestamp,
		RwSet:          rwSetResp.RwSet,
	}, nil
}

// GetBlockByHeight 根据高度获得区块
// @param blockHeight
// @param withRWSet
// @return *common.BlockInfo
// @return error
func (a *ArchiveCenterGrpcClient) GetBlockByHeight(blockHeight uint64, withRWSet bool) (*common.BlockInfo, error) {
	ctx, ctxCancel := context.WithTimeout(context.Background(),
		time.Duration(a.config.ReqeustSecondLimit)*time.Second)
	defer ctxCancel()
	grpcResp, err := a.client.GetBlockByHeight(ctx,
		&archivePb.BlockByHeightRequest{
			ChainUnique: a.config.ChainGenesisHash,
			Height:      blockHeight,
		}, a.GrpcCallOption()...)
	if err != nil {
		return nil, err
	}
	if grpcResp.BlockData == nil || grpcResp.BlockData.Block == nil {
		return nil, errors.New("block data is empty")
	}
	block := &common.BlockInfo{
		Block: grpcResp.BlockData.Block,
	}
	if withRWSet {
		block.RwsetList = grpcResp.BlockData.RwsetList
	}
	return block, nil
}

// GetBlockByHash 根据Hash获得区块
// @param blockHash
// @param withRWSet
// @return *common.BlockInfo
// @return error
func (a *ArchiveCenterGrpcClient) GetBlockByHash(blockHash string, withRWSet bool) (*common.BlockInfo, error) {
	ctx, ctxCancel := context.WithTimeout(context.Background(),
		time.Duration(a.config.ReqeustSecondLimit)*time.Second)
	defer ctxCancel()
	grpcResp, err := a.client.GetBlockByHash(ctx,
		&archivePb.BlockByHashRequest{
			ChainUnique: a.config.ChainGenesisHash,
			BlockHash:   blockHash,
			Operation:   archivePb.OperationByHash_OperationGetBlockByHash,
		}, a.GrpcCallOption()...)
	if err != nil {
		return nil, err
	}
	if grpcResp.BlockData == nil || grpcResp.BlockData.Block == nil {
		return nil, errors.New("block data is empty")
	}
	block := &common.BlockInfo{
		Block: grpcResp.BlockData.Block,
	}
	if withRWSet {
		block.RwsetList = grpcResp.BlockData.RwsetList
	}
	return block, nil
}

// GetBlockByTxId 根据TxId获得所在区块
// @param txId
// @param withRWSet
// @return *common.BlockInfo
// @return error
func (a *ArchiveCenterGrpcClient) GetBlockByTxId(txId string, withRWSet bool) (*common.BlockInfo, error) {
	ctx, ctxCancel := context.WithTimeout(context.Background(),
		time.Duration(a.config.ReqeustSecondLimit)*time.Second)
	defer ctxCancel()
	grpcResp, err := a.client.GetBlockByTxId(ctx,
		&archivePb.BlockByTxIdRequest{
			ChainUnique: a.config.ChainGenesisHash,
			TxId:        txId,
		}, a.GrpcCallOption()...)
	if err != nil {
		return nil, err
	}
	if grpcResp.BlockData == nil || grpcResp.BlockData.Block == nil {
		return nil, errors.New("block data is empty")
	}
	block := &common.BlockInfo{
		Block: grpcResp.BlockData.Block,
	}
	if withRWSet {
		block.RwsetList = grpcResp.BlockData.RwsetList
	}
	return block, nil
}

// Register 将创世区块注册到归档中心
// @param genesisBlock
// @return error
func (a *ArchiveCenterGrpcClient) Register(genesisBlock *common.BlockInfo) error {
	statusCtx, statusCancel := context.WithTimeout(context.Background(),
		time.Duration(a.config.ReqeustSecondLimit)*time.Second)
	defer statusCancel()
	//  检查是否已经注册过了
	archivedStatus, archiveStatusErr := a.client.GetArchivedStatus(
		statusCtx, &archivePb.ArchiveStatusRequest{
			ChainUnique: a.config.ChainGenesisHash,
		})
	if archiveStatusErr == nil && archivedStatus != nil && archivedStatus.Code == 0 {
		a.logger.Debugf("chain[%s] archive status %+v",
			a.config.ChainGenesisHash, *archivedStatus)
		return nil
	}

	genesisHash := genesisBlock.Block.GetBlockHashStr()
	ctx, ctxCancel := context.WithTimeout(context.Background(),
		time.Duration(a.config.ReqeustSecondLimit)*time.Second)
	defer ctxCancel()
	a.logger.Debugf("try to register genesis block[%s] to archive center", genesisHash)
	registerResp, registerError := a.client.Register(ctx,
		&archivecenter.ArchiveBlockRequest{
			ChainUnique: genesisHash,
			Block:       genesisBlock,
		}, a.GrpcCallOption()...)
	if registerError != nil {
		return fmt.Errorf("register genesis rpc error %s", registerError.Error())
	}
	if registerResp == nil {
		return fmt.Errorf("register genesis rpc no response")
	}
	if registerResp.Code == 0 &&
		registerResp.RegisterStatus == archivecenter.RegisterStatus_RegisterStatusSuccess {
		a.logger.Debugf("register genesis block[%s] to archive center success", genesisHash)
		return nil
	}
	return errors.New("register fail")
}

// ArchiveBlock 向归档中心提交一个归档区块
// @param block
// @return error
func (a *ArchiveCenterGrpcClient) ArchiveBlock(block *common.BlockInfo) error {
	sclient, err := a.client.SingleArchiveBlocks(context.Background(),
		a.GrpcCallOption()...)
	if err != nil {
		return err
	}

	singleSendErr := sclient.Send(&archivePb.ArchiveBlockRequest{
		ChainUnique: a.config.ChainGenesisHash,
		Block:       block,
	})
	if singleSendErr != nil {
		return fmt.Errorf("send height %d got error %s",
			block.Block.Header.BlockHeight, singleSendErr.Error())
	}
	archiveResp, archiveRespErr := sclient.CloseAndRecv()
	if archiveRespErr != nil {
		return fmt.Errorf("stream close recv error %s", archiveRespErr.Error())
	}
	if archiveResp != nil {
		fmt.Printf("archive resp code %d ,message %s , begin %d , end %d \n",
			archiveResp.Code, archiveResp.Message,
			archiveResp.ArchivedBeginHeight, archiveResp.ArchivedEndHeight)
	}
	return nil
}

// ArchiveBlocks 向归档中心Stream提交要归档的区块
// @param bi
// @return error
func (a *ArchiveCenterGrpcClient) ArchiveBlocks(bi BlockIterator,
	heightNoticeCallback func(ProcessMessage) error) error {
	sclient, err := a.client.ArchiveBlocks(context.Background(),
		a.GrpcCallOption()...)
	if err != nil {
		return err
	}
	if bi != nil {
		defer bi.Release()
	}
	sendCount := 0
	notice := func(h uint64, e error) {
		if heightNoticeCallback != nil {
			heightNoticeCallback(ProcessMessage{
				CurrentHeight: h,
				Total:         bi.Total(),
				Error:         e,
			})
		}
	}
	for bi.Next() {
		block, err1 := bi.Value()
		if err1 != nil {
			notice(bi.Current(), err1)
			return err1
		}
		sendErr := sclient.Send(&archivecenter.ArchiveBlockRequest{
			ChainUnique: a.config.ChainGenesisHash,
			Block:       block,
		})
		sendCount++
		if sendErr != nil {
			notice(block.Block.Header.BlockHeight, sendErr)
			return fmt.Errorf("send height %d got error %s",
				block.Block.Header.BlockHeight, sendErr.Error())
		}
		archiveResp, archiveRespErr := sclient.Recv()
		if archiveRespErr != nil {
			notice(block.Block.Header.BlockHeight, archiveRespErr)
			return fmt.Errorf("send height %d got error %s", block.Block.Header.BlockHeight, archiveRespErr.Error())
		}
		if archiveResp.ArchiveStatus == archivecenter.ArchiveStatus_ArchiveStatusFailed {
			return fmt.Errorf("send height %d failed %s ", block.Block.Header.BlockHeight, archiveResp.Message)
		}
		notice(block.Block.Header.BlockHeight, nil)
	}

	archiveRespErr := sclient.CloseSend()
	if archiveRespErr != nil {
		return fmt.Errorf("stream close recv error %s", archiveRespErr.Error())
	}
	if sendCount == 0 {
		return errors.New("no block to archive")
	}
	return nil
}

// GetArchivedStatus 获得归档中心的归档状态
// @return archivedHeight
// @return inArchive
// @return code
// @return err
func (a *ArchiveCenterGrpcClient) GetArchivedStatus() (archivedHeight uint64, inArchive bool, code uint32, err error) {
	ctx, ctxCancel := context.WithTimeout(context.Background(),
		time.Duration(a.config.ReqeustSecondLimit)*time.Second)
	defer ctxCancel()
	resp, err := a.client.GetArchivedStatus(ctx,
		&archivePb.ArchiveStatusRequest{
			ChainUnique: a.config.ChainGenesisHash,
		}, a.GrpcCallOption()...)
	if err != nil {
		return 0, false, 1, err
	}
	archivedHeight = resp.ArchivedHeight
	inArchive = resp.InArchive
	code = resp.Code
	if len(resp.Message) > 0 {
		err = errors.New(resp.Message)
	}
	return
}
