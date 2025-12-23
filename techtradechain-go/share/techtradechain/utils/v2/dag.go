/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"bytes"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"github.com/gogo/protobuf/proto"
)

// CalcDagHash calculate DAG hash
func CalcDagHash(hashType string, dag *commonPb.DAG) ([]byte, error) {
	if dag == nil {
		return nil, fmt.Errorf("calc hash block == nil")
	}

	dagBytes, err := proto.Marshal(dag)
	if err != nil {
		return nil, fmt.Errorf("marshal DAG error, %s", err)
	}

	hashByte, err := hash.GetByStrType(hashType, dagBytes)
	if err != nil {
		return nil, err
	}
	return hashByte, nil
}

// IsDagEqual compare two DAG by bytes
func IsDagEqual(dag1 *commonPb.DAG, dag2 *commonPb.DAG) (bool, error) {
	if dag1 == nil || dag2 == nil {
		return false, fmt.Errorf("dag is nil")
	}
	dagBytes1, err := proto.Marshal(dag1)
	if err != nil {
		return false, fmt.Errorf("marshal DAG error, %s", err)
	}
	dagBytes2, err := proto.Marshal(dag2)
	if err != nil {
		return false, fmt.Errorf("marshal DAG error, %s", err)
	}
	return bytes.Equal(dagBytes1, dagBytes2), nil
}
