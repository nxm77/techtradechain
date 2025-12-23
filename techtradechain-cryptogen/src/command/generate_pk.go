/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package command

import (
	"techtradechain.com/techtradechain/common/v2/pk"
	"fmt"
	"io/ioutil"
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

func GeneratePKCmd() *cobra.Command {
	generateCmd := &cobra.Command{
		Use:   "generate-pk",
		Short: "Generate public-key material",
		Long:  "Generate public-key material",
		RunE: func(cmd *cobra.Command, args []string) error {
			return generatePK()
		},
	}
	generateCmd.Flags().StringVarP(&pkOutputDir, "output", "o", "crypto-config", "specify the output directory in which to place key")
	return generateCmd
}

func generatePK() error {
	if HSMKeysType == config.KMS_KEY_TYPE_PK {
		return generatePKKms()
	}

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
				if err := generatePKNode(path, name, keyType, hashType); err != nil {
					return err
				}
				for _, user := range item.User {
					for i := 0; i < int(user.Count); i++ {
						name := fmt.Sprintf("%s%d", user.Type, i+1)
						path := filepath.Join(path, "user", name)
						if err := generatePKUser(path, name, keyType, item.HashAlgo); err != nil {
							return err
						}
					}
				}

				adminName := fmt.Sprintf("%s%d", "node", j+1)
				adminPath := filepath.Join(nodePath, adminName)
				for i := 0; i < int(item.Admin.Count); i++ {
					fileName := fmt.Sprintf("%s%d", "admin", i+1)
					filePath := filepath.Join(adminPath, "admin", fileName)
					err := savePKAdmin(filePath, fileName, []byte(privKeyPemList[i]), []byte(pubKeyPemList[i]))
					if err != nil {
						return err
					}

				}

				time.Sleep(time.Millisecond * 50)
			}
		}
	}

	return nil
}

func generatePKKms() error {
	pkConfig := config.GetPKConfig()

	for _, item := range pkConfig.Item {
		keyType := crypto.AsymAlgoMap[strings.ToUpper(item.PKAlgo)]
		hashType := crypto.HashAlgoMap[strings.ToUpper(item.HashAlgo)]

		nodePath := filepath.Join(pkOutputDir)
		adminPrivKeyPemList := make([]string, item.Admin.Count)
		adminPubKeyPemList := make([]string, item.Admin.Count)
		for i := 0; i < int(item.Admin.Count); i++ {
			err := config.SetPrivKeyContextWithPK(keyType, fmt.Sprintf("node%d", i+1), 0, "admin")
			if err != nil {
				panic(err)
			}
			privKey, err := pk.CreatePrivKey(keyType, "", "", false)
			if err != nil {
				panic(err)
				return err
			}

			privKeyPEM, err := privKey.String()
			if err != nil {
				panic(err)
				return err
			}

			pubKeyPEM, err := privKey.PublicKey().String()
			if err != nil {
				panic(err)
				return err
			}

			adminPrivKeyPemList[i] = privKeyPEM
			adminPubKeyPemList[i] = pubKeyPEM
		}

		for _, node := range item.Node {
			for j := 0; j < int(node.Count); j++ {
				name := fmt.Sprintf("%s%d", "node", j+1)
				path := filepath.Join(nodePath, name)
				err := config.SetPrivKeyContextWithPK(keyType,fmt.Sprintf("node%d", j+1), 0, "consensus")
				if err != nil {
					panic(err)
				}

				if err := generatePKNodeKMS(path, name, keyType, hashType); err != nil {
					panic(err)
					return err
				}
				for _, user := range item.User {
					for i := 0; i < int(user.Count); i++ {
						name := fmt.Sprintf("%s%d", user.Type, i+1)
						path := filepath.Join(path, "user", name)
						err := config.SetPrivKeyContextWithPK(keyType, fmt.Sprintf("node%d", j+1), 0, "client")
						if err != nil {
							panic(err)
						}
						if err := generatePKUserKMS(path, name, keyType, item.HashAlgo); err != nil {
							panic(err)
							return err
						}
					}
				}

				adminName := fmt.Sprintf("%s%d", "node", j+1)
				adminPath := filepath.Join(nodePath, adminName)
				for i := 0; i < int(item.Admin.Count); i++ {
					fileName := fmt.Sprintf("%s%d", "admin", i+1)
					filePath := filepath.Join(adminPath, "admin", fileName)
					err := savePKAdmin(filePath, fileName, []byte(adminPrivKeyPemList[i]), []byte(adminPubKeyPemList[i]))
					if err != nil {
						panic(err)
						return err
					}
				}

				time.Sleep(time.Millisecond * 50)
			}
		}
	}

	return nil
}

func savePKAdmin(filePath, fileName string, privKeyPEM, pubKeyPEM []byte) error {

	keyName := fmt.Sprintf("%s%s", fileName, ".key")
	pemName := fmt.Sprintf("%s%s", fileName, ".pem")
	if err := makeFile(filePath, keyName); err != nil {
		return err
	}

	if err := ioutil.WriteFile(
		filepath.Join(filePath, keyName), privKeyPEM, 0600,
	); err != nil {
		return fmt.Errorf("save key to file [%s] failed, %s", filePath, err.Error())
	}

	if err := makeFile(filePath, pemName); err != nil {
		return err
	}

	if err := ioutil.WriteFile(
		filepath.Join(filePath, pemName), pubKeyPEM, 0600,
	); err != nil {
		return fmt.Errorf("save key to file [%s] failed, %s", filePath, err.Error())
	}

	return nil
}

func generatePKAdmin(keyType crypto.KeyType) (string, string, error) {
	return asym.GenerateKeyPairPEM(keyType)
}

func generatePKUser(filePath, fileName string, keyType crypto.KeyType, hashType string) error {
	keyName := fmt.Sprintf("%s.key", fileName)
	pemName := fmt.Sprintf("%s.pem", fileName)

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

	// calc client address for DPoS using client's sign cert
	keyPEM, err := ioutil.ReadFile(filepath.Join(filePath, keyName))
	if err != nil {
		return err
	}
	key, err := parseKey(keyPEM)
	if err != nil {
		return err
	}
	pubKey, err := key.PublicKey().String()
	if err != nil {
		return err
	}

	var (
		addr   string
		hashBz []byte
	)

	if hashType == crypto.CRYPTO_ALGO_SM3 || hashType == crypto.CRYPTO_ALGO_SHA256 {
		if hashBz, err = hash.GetByStrType(hashType, []byte(pubKey)); err != nil {
			return err
		}
	}

	addr = base58.Encode(hashBz[:])
	userAddrFileName := filepath.Join(filePath, fmt.Sprintf("%s.addr", fileName))
	err = ioutil.WriteFile(userAddrFileName, []byte(addr), 0600)
	if err != nil {
		return err
	}

	return nil
}

func generatePKUserKMS(filePath, fileName string, keyType crypto.KeyType, hashType string) error {
	keyName := fmt.Sprintf("%s.key", fileName)
	pemName := fmt.Sprintf("%s.pem", fileName)

	privKey, err := pk.CreatePrivKey(keyType, "", "", false)
	if err != nil {
		return err
	}

	privKeyPEM, err := privKey.String()
	if err != nil {
		return err
	}

	pubKeyPEM, err := privKey.PublicKey().String()
	if err != nil {
		return err
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

	//// calc client address for DPoS using client's sign cert
	//keyPEM, err := ioutil.ReadFile(filepath.Join(filePath, pemName))
	//if err != nil {
	//	panic(err)
	//	return err
	//}
	//key, err := parsePubKey(keyPEM)
	//if err != nil {
	//	panic(err)
	//	return err
	//}
	//pubKey, err := key.String()
	//if err != nil {
	//	panic(err)
	//	return err
	//}

	var (
		addr   string
		hashBz []byte
	)

	if hashType == crypto.CRYPTO_ALGO_SM3 || hashType == crypto.CRYPTO_ALGO_SHA256 {
		if hashBz, err = hash.GetByStrType(hashType, []byte(pubKeyPEM)); err != nil {
			return err
		}
	}

	addr = base58.Encode(hashBz[:])
	userAddrFileName := filepath.Join(filePath, fmt.Sprintf("%s.addr", fileName))
	err = ioutil.WriteFile(userAddrFileName, []byte(addr), 0600)
	if err != nil {
		return err
	}

	return nil
}

func generatePKNode(filePath, fileName string, keyType crypto.KeyType, hashType crypto.HashType) error {
	keyName := fmt.Sprintf("%s.key", fileName)
	pemName := fmt.Sprintf("%s.pem", fileName)

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

func generatePKNodeKMS(filePath, fileName string, keyType crypto.KeyType, hashType crypto.HashType) error {
	keyName := fmt.Sprintf("%s.key", fileName)
	pemName := fmt.Sprintf("%s.pem", fileName)

	privKey, err := pk.CreatePrivKey(keyType, "", "", false)
	if err != nil {
		return err
	}

	privKeyPEM, err := privKey.String()
	if err != nil {
		return err
	}

	pubKeyPEM, err := privKey.PublicKey().String()
	if err != nil {
		return err
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

	nodeUid, err := helper.CreateLibp2pPeerIdWithPublicKey(privKey.PublicKey())
	if err != nil {
		return err
	}
	nodeIdFileName := filepath.Join(filePath, fmt.Sprintf("%s.nodeid", fileName))
	return ioutil.WriteFile(nodeIdFileName, []byte(nodeUid), 0600)
}