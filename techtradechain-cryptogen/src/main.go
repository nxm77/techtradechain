/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"log"

	"techtradechain.com/techtradechain-cryptogen/command"
	"techtradechain.com/techtradechain-cryptogen/config"
	"github.com/spf13/cobra"
)

func main() {
	mainCmd := &cobra.Command{
		Use: "techtradechain-cryptogen",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			config.LoadCryptoGenConfig(command.ConfigPath, command.HSMKeysType)
			config.LoadHSMKeysConfig(command.HSMKeysPath, command.HSMKeysType)
		},
	}
	mainFlags := mainCmd.PersistentFlags()
	mainFlags.StringVarP(&command.ConfigPath, "config", "c", "../config/crypto_config_template.yml", "specify config file path")
	mainFlags.StringVarP(&command.HSMKeysPath, "hsm_keys", "p", "../config/hsm_keys.yml", "specify hsm keys file path")
	mainFlags.StringVarP(&command.HSMKeysType, "hsm_keys_types", "t", "", "specify hsm keys type")

	mainCmd.AddCommand(command.ShowConfigCmd())
	mainCmd.AddCommand(command.GenerateCmd())
	mainCmd.AddCommand(command.ExtendCmd())
	mainCmd.AddCommand(command.GeneratePWKCmd())
	mainCmd.AddCommand(command.ExtendPWKCmd())
	mainCmd.AddCommand(command.GeneratePKCmd())
	mainCmd.AddCommand(command.ExtendPKCmd())

	if err := mainCmd.Execute(); err != nil {
		log.Fatalf("failed to execute, err = %s", err)
	}
}
