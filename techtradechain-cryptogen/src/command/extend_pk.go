/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package command

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"techtradechain.com/techtradechain-cryptogen/config"
	"techtradechain.com/techtradechain/common/v2/crypto"
	"github.com/spf13/cobra"
)

func ExtendPKCmd() *cobra.Command {
	generateCmd := &cobra.Command{
		Use:   "extend-pk",
		Short: "Extend existing network",
		Long:  "Extend existing network",
		RunE: func(cmd *cobra.Command, args []string) error {
			return extendPK()
		},
	}
	generateCmd.Flags().StringVarP(&pkOutputDir, "output", "o", "crypto-config", "specify the output directory in which to place key")
	return generateCmd
}

func extendPK() error {
	config.LoadPKConfig(ConfigPath)
	pkConfig := config.GetPKConfig()

	for _, item := range pkConfig.Item {
		keyType := crypto.AsymAlgoMap[strings.ToUpper(item.PKAlgo)]
		hashType := crypto.HashAlgoMap[strings.ToUpper(item.HashAlgo)]

		nodePath := filepath.Join(pkOutputDir)

		privKeyPemList := make([]string, item.Admin.Count)
		pubKeyPemList := make([]string, item.Admin.Count)
		for i := 0; i < int(item.Admin.Count); i++ {
			privKeyPEM, pubKeyPEM, err := generatePKAdmin(keyType)
			if err != nil {
				return err
			}
			privKeyPemList[i] = privKeyPEM
			pubKeyPemList[i] = pubKeyPEM
		}

		for _, node := range item.Node {
			for j := 0; j < int(node.Count); j++ {
				name := fmt.Sprintf("%s%d", "node", j+1)
				path := filepath.Join(nodePath, name)

				if _, err := os.Stat(path); os.IsNotExist(err) {
					if err := generatePKNode(path, name, keyType, hashType); err != nil {
						return err
					}
				}

				for _, user := range item.User {
					for i := 0; i < int(user.Count); i++ {
						name := fmt.Sprintf("%s%d", user.Type, i+1)
						path := filepath.Join(path, "user", name)
						if _, err := os.Stat(path); os.IsNotExist(err) {
							if err := generatePKUser(path, name, keyType, item.HashAlgo); err != nil {
								return err
							}
						}
					}
				}

				adminName := fmt.Sprintf("%s%d", "node", j+1)
				adminPath := filepath.Join(nodePath, adminName)
				for i := 0; i < int(item.Admin.Count); i++ {
					fileName := fmt.Sprintf("%s%d", "admin", i+1)
					filePath := filepath.Join(adminPath, "admin", fileName)
					if _, err := os.Stat(filePath); os.IsNotExist(err) {
						err = savePKAdmin(filePath, fileName, []byte(privKeyPemList[i]), []byte(pubKeyPemList[i]))
						if err != nil {
							return err
						}
					} else if err == nil {
						fileName := fmt.Sprintf("%s%d", "admin", i+1)
						filePath := filepath.Join(adminPath, "admin", fileName)
						keyName := fmt.Sprintf("%s%s", fileName, ".key")
						pemName := fmt.Sprintf("%s%s", fileName, ".pem")
						privKeyPEM, err := ioutil.ReadFile(filepath.Join(filePath, keyName))
						if err != nil {
							return nil
						}
						pubKeyPEM, err := ioutil.ReadFile(filepath.Join(filePath, pemName))
						if err != nil {
							return nil
						}
						privKeyPemList[i] = string(privKeyPEM)
						pubKeyPemList[i] = string(pubKeyPEM)
					}
				}

				time.Sleep(time.Millisecond * 50)
			}
		}
	}

	return nil
}
