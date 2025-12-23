/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmd

import (
	"fmt"

	"techtradechain.com/techtradechain/localconf/v2"
	"github.com/spf13/cobra"
)

func ConfigCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Show techtradechain config",
		Long:  "Show techtradechain config",
		RunE: func(cmd *cobra.Command, _ []string) error {
			initLocalConfig(cmd)
			return showConfig()
		},
	}
	attachFlags(cmd, []string{flagNameOfConfigFilepath})
	return cmd
}

func showConfig() error {
	json, err := localconf.TechTradeChainConfig.PrettyJson()
	if err != nil {
		return err
	}

	fmt.Println(json)
	return nil
}
