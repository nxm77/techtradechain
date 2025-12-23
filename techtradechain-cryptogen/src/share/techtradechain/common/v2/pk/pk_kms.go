/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pk

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/common/v2/kmsutils"
)

// CreatePrivKey - create private key file
func CreatePrivKey(keyType crypto.KeyType, keyPath, keyFile string, isTLS bool) (key crypto.PrivateKey, err error) {
	algoName, ok := crypto.KeyType2NameMap[keyType]
	if !ok {
		return nil, fmt.Errorf("unknown key algo type [%d]", keyType)
	}

	var privKeyPEM string
	if kmsutils.KMSContext != nil && kmsutils.KMSContext.KMSConfig.Enable && !isTLS {
		var keySpecBytes []byte
		keySpecBytes, key, err = kmsutils.CreateKMSKeyWithContext(kmsutils.KMSContext, nil)
		if err != nil {
			return nil, fmt.Errorf("generate kms key pair [%s] failed, %s", algoName, err.Error())
		}
		privKeyPEM = string(keySpecBytes)
	} else {
		key, err = asym.GenerateKeyPair(keyType)
		if err != nil {
			return nil, fmt.Errorf("generate key pair [%s] failed, %s", algoName, err.Error())
		}

		privKeyPEM, err = key.String()
		if err != nil {
			return nil, fmt.Errorf("key to pem failed, %s", err.Error())
		}
	}

	if keyPath != "" {
		if err = os.MkdirAll(keyPath, os.ModePerm); err != nil {
			return nil, fmt.Errorf("mk key dir failed, %s", err.Error())
		}

		if err = ioutil.WriteFile(filepath.Join(keyPath, keyFile),
			[]byte(privKeyPEM), 0600); err != nil {
			return nil, fmt.Errorf("save key to file [%s] failed, %s", keyPath, err.Error())
		}
	}

	return key, nil
}
