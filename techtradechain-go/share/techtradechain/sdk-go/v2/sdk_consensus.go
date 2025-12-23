package techtradechain_sdk_go

import (
	"context"

	"techtradechain.com/techtradechain/pb-go/v2/consensus"
)

// GetConsensusValidators returns consensus validators
func (cc *ChainClient) GetConsensusValidators() ([]string, error) {
	cc.logger.Debug("[SDK] begin GetConsensusValidators")
	client, err := cc.pool.getClient()
	if err != nil {
		return nil, err
	}
	req := &consensus.GetConsensusStatusRequest{
		ChainId: cc.chainId,
	}
	resp, err := client.rpcNode.GetConsensusValidators(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return resp.Nodes, nil
}

// GetConsensusHeight returns the current block height of consensus
func (cc *ChainClient) GetConsensusHeight() (uint64, error) {
	cc.logger.Debug("[SDK] begin GetConsensusHeight")
	client, err := cc.pool.getClient()
	if err != nil {
		return 0, err
	}
	req := &consensus.GetConsensusStatusRequest{
		ChainId: cc.chainId,
	}
	resp, err := client.rpcNode.GetConsensusHeight(context.Background(), req)
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

// GetConsensusStateJSON returns state json of consensus
func (cc *ChainClient) GetConsensusStateJSON() ([]byte, error) {
	cc.logger.Debug("[SDK] begin GetConsensusStateJSON")
	client, err := cc.pool.getClient()
	if err != nil {
		return nil, err
	}
	req := &consensus.GetConsensusStatusRequest{
		ChainId: cc.chainId,
	}
	resp, err := client.rpcNode.GetConsensusStateJSON(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}
