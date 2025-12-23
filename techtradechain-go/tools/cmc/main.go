/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"strings"

	"techtradechain.com/techtradechain-go/tools/cmc/address"
	"techtradechain.com/techtradechain-go/tools/cmc/archive"
	"techtradechain.com/techtradechain-go/tools/cmc/bulletproofs"
	"techtradechain.com/techtradechain-go/tools/cmc/cert"
	"techtradechain.com/techtradechain-go/tools/cmc/client"
	commandutil "techtradechain.com/techtradechain-go/tools/cmc/command_util"
	"techtradechain.com/techtradechain-go/tools/cmc/consensus"
	"techtradechain.com/techtradechain-go/tools/cmc/console"
	"techtradechain.com/techtradechain-go/tools/cmc/gas"
	"techtradechain.com/techtradechain-go/tools/cmc/hibe"
	"techtradechain.com/techtradechain-go/tools/cmc/key"
	"techtradechain.com/techtradechain-go/tools/cmc/node"
	"techtradechain.com/techtradechain-go/tools/cmc/paillier"
	"techtradechain.com/techtradechain-go/tools/cmc/parallel"
	"techtradechain.com/techtradechain-go/tools/cmc/payload"
	"techtradechain.com/techtradechain-go/tools/cmc/pubkey"
	"techtradechain.com/techtradechain-go/tools/cmc/query"
	"techtradechain.com/techtradechain-go/tools/cmc/tee"
	"techtradechain.com/techtradechain-go/tools/cmc/txpool"
	"techtradechain.com/techtradechain-go/tools/cmc/version"
	"github.com/spf13/cobra"
)

func main() {
	mainCmd := &cobra.Command{
		Use:   "cmc",
		Short: "TechTradeChain CLI",
		Long: strings.TrimSpace(`Command line interface for interacting with TechTradeChain daemon.
For detailed logs, please see ./sdk.log
`),
	}

	mainCmd.AddCommand(key.KeyCMD())
	mainCmd.AddCommand(cert.CertCMD())
	mainCmd.AddCommand(client.ClientCMD())
	mainCmd.AddCommand(hibe.HibeCMD())
	mainCmd.AddCommand(paillier.PaillierCMD())
	mainCmd.AddCommand(archive.NewArchiveCMD())
	mainCmd.AddCommand(query.NewQueryOnChainCMD())
	mainCmd.AddCommand(payload.NewPayloadCMD())
	mainCmd.AddCommand(console.NewConsoleCMD(mainCmd))
	mainCmd.AddCommand(bulletproofs.BulletproofsCMD())
	mainCmd.AddCommand(tee.NewTeeCMD())
	mainCmd.AddCommand(pubkey.NewPubkeyCMD())
	mainCmd.AddCommand(parallel.ParallelCMD())
	mainCmd.AddCommand(address.NewAddressCMD())
	mainCmd.AddCommand(gas.NewGasManageCMD())
	mainCmd.AddCommand(txpool.NewTxPoolCMD())
	mainCmd.AddCommand(version.VersionCMD())
	mainCmd.AddCommand(commandutil.NewUtilCMD())
	mainCmd.AddCommand(consensus.NewConsensusCMD())
	mainCmd.AddCommand(node.NewNodeCMD())

	// 后续改成go-sdk
	//mainCmd.AddCommand(payload.PayloadCMD())
	//mainCmd.AddCommand(log.LogCMD())

	mainCmd.Execute()
}
