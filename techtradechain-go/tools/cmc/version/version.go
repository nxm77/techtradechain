/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package version

import (
	"fmt"

	"techtradechain.com/techtradechain-go/module/blockchain"
	"techtradechain.com/techtradechain/protocol/v2"
	"github.com/common-nighthawk/go-figure"
	"github.com/spf13/cobra"
)

// VersionCMD show techtradechain client version
func VersionCMD() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show TechTradeChain Client version",
		Long:  "Show TechTradeChain Client version",
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Println(logo())
			return nil
		},
	}
}

func logo() string {
	fig := figure.NewFigure("TechTradeChain Client", "slant", true)
	s := fig.String()
	fragment := "==============================================================================================" +
		"============================="
	versionInfo := fmt.Sprintf("CMC Version: %s\n", blockchain.CurrentVersion)

	versionInfo += fmt.Sprintf("Block Version:%6s%d\n", " ", protocol.DefaultBlockVersion)

	if blockchain.BuildDateTime != "" {
		versionInfo += fmt.Sprintf("Build Time:%9s%s\n", " ", blockchain.BuildDateTime)
	}

	if blockchain.GitBranch != "" {
		versionInfo += fmt.Sprintf("Git Commit:%9s%s", " ", blockchain.GitBranch)
		if blockchain.GitCommit != "" {
			versionInfo += fmt.Sprintf("(%s)", blockchain.GitCommit)
		}
	}
	return fmt.Sprintf("\n%s\n%s%s\n%s\n", fragment, s, fragment, versionInfo)
}
