/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package command

import (
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"techtradechain.com/techtradechain-cryptogen/config"
	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"techtradechain.com/techtradechain/common/v2/helper"
	"github.com/mr-tron/base58/base58"
	"github.com/spf13/cobra"
)

func GeneratePWKCmd() *cobra.Command {
	generateCmd := &cobra.Command{
		Use:   "generate-pwk",
		Short: "Generate permission-with-key material",
		Long:  "Generate permission-with-key material",
		RunE: func(cmd *cobra.Command, args []string) error {
			return generatePWK()
		},
	}
	generateCmd.Flags().StringVarP(&pwkOutputDir, "output", "o", "crypto-config", "specify the output directory in which to place key")
	return generateCmd
}

func generatePWK() error {
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

			if err := generatePWKAdmin(admin, "admin", keyType); err != nil {
				return err
			}

			for _, user := range item.User {
				for j := 0; j < int(user.Count); j++ {
					name := fmt.Sprintf("%s%d", user.Type, j+1)
					path := filepath.Join(userPath, name)
					if err := generatePWKUser(path, name, keyType, item.HashAlgo); err != nil {
						return err
					}
				}
			}

			for _, node := range item.Node {
				for j := 0; j < int(node.Count); j++ {
					name := fmt.Sprintf("%s%d", node.Type, j+1)
					path := filepath.Join(nodePath, name)
					if err := generatePWKNode(path, name, keyType); err != nil {
						return err
					}
				}
			}
			time.Sleep(time.Millisecond * 50)
		}
	}

	return nil
}

func generatePWKAdmin(filePath, fileName string, keyType crypto.KeyType) error {
	keyName := fmt.Sprintf("%s%s", fileName, ".key")
	pemName := fmt.Sprintf("%s%s", fileName, ".pem")

	privKeyPEM, pubKeyPEM, err := asym.GenerateKeyPairPEM(keyType)
	if err != nil {
		return fmt.Errorf("GenerateKeyPairPEM failed, %s", err.Error())
	}

	if err = makeFile(filePath, keyName); err != nil {
		return err
	}

	if err = ioutil.WriteFile(
		filepath.Join(filePath, keyName), []byte(privKeyPEM), 0600,
	); err != nil {
		return fmt.Errorf("save key to file [%s] failed, %s", filePath, err.Error())
	}

	if err = makeFile(filePath, pemName); err != nil {
		return err
	}

	if err = ioutil.WriteFile(
		filepath.Join(filePath, pemName), []byte(pubKeyPEM), 0600,
	); err != nil {
		return fmt.Errorf("save key to file [%s] failed, %s", filePath, err.Error())
	}

	return nil
}

func makeFile(filePath, fileName string) error {
	if err := os.MkdirAll(filePath, os.ModePerm); err != nil {
		return fmt.Errorf("mk cert dir failed, %s", err.Error())
	}

	f, err := os.Create(filepath.Join(filePath, fileName))
	if err != nil {
		return fmt.Errorf("create file failed, %s", err.Error())
	}

	if err = f.Close(); err != nil {
		return err
	}

	return nil
}

func generatePWKUser(filePath, fileName string, keyType crypto.KeyType, hashType string) error {
	keyName := fmt.Sprintf("%s.key", fileName)
	pemName := fmt.Sprintf("%s.pem", fileName)

	privKeyPEM, pubKeyPEM, err := asym.GenerateKeyPairPEM(keyType)
	if err != nil {
		return fmt.Errorf("GenerateKeyPairPEM failed, %s", err.Error())
	}

	if err = makeFile(filePath, keyName); err != nil {
		return err
	}

	if err = makeFile(filePath, pemName); err != nil {
		return err
	}

	if err = ioutil.WriteFile(
		filepath.Join(filePath, keyName), []byte(privKeyPEM), 0600,
	); err != nil {
		return fmt.Errorf("save key to file [%s] failed, %s", filePath, err.Error())
	}

	if err = ioutil.WriteFile(
		filepath.Join(filePath, pemName), []byte(pubKeyPEM), 0600,
	); err != nil {
		return fmt.Errorf("save key to file [%s] failed, %s", filePath, err.Error())
	}

	// calc client address for DPoS using client's sign cert
	if strings.HasPrefix(fileName, "client") {
		keyPEM, err := ioutil.ReadFile(filepath.Join(filePath, keyName))
		if err != nil {
			return err
		}
		key, err := parseKey(keyPEM)
		if err != nil {
			return err
		}
		pubKey, err := key.PublicKey().Bytes()
		if err != nil {
			return err
		}

		var (
			addr   string
			hashBz []byte
		)

		switch strings.ToUpper(hashType) {
		case crypto.CRYPTO_ALGO_SM3:
			hashBz, err = hash.GetByStrType(crypto.CRYPTO_ALGO_SM3, pubKey)
		case crypto.CRYPTO_ALGO_SHA256:
			hashBz, err = hash.GetByStrType(crypto.CRYPTO_ALGO_SHA256, pubKey)
		}

		//hashBz, err = hash.GetByStrType(hashType, pubKey)
		if err != nil {
			return err
		}

		addr = base58.Encode(hashBz[:])
		userAddrFileName := filepath.Join(filePath, fmt.Sprintf("%s.addr", fileName))
		err = ioutil.WriteFile(userAddrFileName, []byte(addr), 0600)
		if err != nil {
			return err
		}
	}
	return nil
}

func generatePWKNode(filePath, fileName string, keyType crypto.KeyType) error {
	keyName := fmt.Sprintf("%s.key", fileName)
	pemName := fmt.Sprintf("%s.pem", fileName)

	privKeyPEM, pubKeyPEM, err := asym.GenerateKeyPairPEM(keyType)
	if err != nil {
		return fmt.Errorf("GenerateKeyPairPEM failed, %s", err.Error())
	}

	if err = makeFile(filePath, keyName); err != nil {
		return err
	}

	if err = makeFile(filePath, pemName); err != nil {
		return err
	}

	if err = ioutil.WriteFile(
		filepath.Join(filePath, keyName), []byte(privKeyPEM), 0600,
	); err != nil {
		return fmt.Errorf("save key to file [%s] failed, %s", filePath, err.Error())
	}

	if err = ioutil.WriteFile(
		filepath.Join(filePath, pemName), []byte(pubKeyPEM), 0600,
	); err != nil {
		return fmt.Errorf("save key to file [%s] failed, %s", filePath, err.Error())
	}

	publicKey, err := asym.PublicKeyFromPEM([]byte(pubKeyPEM))
	if err != nil {
		return err
	}

	nodeUid, err := helper.CreateLibp2pPeerIdWithPublicKey(publicKey)
	if err != nil {
		return err
	}
	nodeIdFileName := filepath.Join(filePath, fmt.Sprintf("%s.nodeid", fileName))
	return ioutil.WriteFile(nodeIdFileName, []byte(nodeUid), 0600)
}

// parseCert convert bytearray to certificate
func parseKey(keyPEM []byte) (crypto.PrivateKey, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, fmt.Errorf("decode pem failed, invalid keyPEM")
	}

	key, err := asym.PrivateKeyFromDER(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key failed, %s", err)
	}

	return key, nil
}
