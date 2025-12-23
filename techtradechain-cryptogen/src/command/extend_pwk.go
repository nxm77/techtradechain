/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package command

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"techtradechain.com/techtradechain-cryptogen/config"
	"techtradechain.com/techtradechain/common/v2/crypto"
	"github.com/spf13/cobra"
)

func ExtendPWKCmd() *cobra.Command {
	generateCmd := &cobra.Command{
		Use:   "extend-pwk",
		Short: "Extend existing existing",
		Long:  "Extend existing existing",
		RunE: func(cmd *cobra.Command, args []string) error {
			return extendPWK()
		},
	}
	generateCmd.Flags().StringVarP(&pwkOutputDir, "output", "o", "crypto-config", "specify the output directory in which to place key")
	return generateCmd
}

func extendPWK() error {
	//config.LoadPWKConfig(PWKConfigPath)
	config.LoadPWKConfig(ConfigPath)
	pwkConfig := config.GetPWKConfig()

	for _, item := range pwkConfig.Item {
		for i := 0; i < int(item.Count); i++ {
			orgName := fmt.Sprintf("%s%d.%s", item.HostName, i+1, item.Domain)
			if item.Count == 1 {
				orgName = fmt.Sprintf("%s.%s", item.HostName, item.Domain)
			}

			// ECC RSA2048 SM2
			keyType := crypto.AsymAlgoMap[strings.ToUpper(item.PKAlgo)]

			admin := filepath.Join(pwkOutputDir, orgName, "admin")
			userPath := filepath.Join(pwkOutputDir, orgName, "user")
			nodePath := filepath.Join(pwkOutputDir, orgName, "node")

			if _, err := os.Stat(admin); os.IsNotExist(err) {
				if err = generatePWKAdmin(admin, "admin", keyType); err != nil {
					return err
				}
			}

			for _, user := range item.User {
				for j := 0; j < int(user.Count); j++ {
					name := fmt.Sprintf("%s%d", user.Type, j+1)
					path := filepath.Join(userPath, name)
					if _, err := os.Stat(path); os.IsNotExist(err) {
						if err := generatePWKUser(path, name, keyType, item.HashAlgo); err != nil {
							return err
						}
					}
				}
			}

			for _, node := range item.Node {
				for j := 0; j < int(node.Count); j++ {
					name := fmt.Sprintf("%s%d", node.Type, j+1)
					path := filepath.Join(nodePath, name)
					if _, err := os.Stat(path); os.IsNotExist(err) {

						if err := generatePWKNode(path, name, keyType); err != nil {
							return err
						}
					}
				}
			}

			time.Sleep(time.Millisecond * 50)
		}
	}

	return nil
}
