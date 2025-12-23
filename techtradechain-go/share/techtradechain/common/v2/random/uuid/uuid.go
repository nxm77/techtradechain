/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package uuid

import (
	"math/rand"
	"strings"

	"github.com/google/uuid"
)

func getStandardUUID() string {
	return uuid.New().String()
}

// GetUUID 生成一个标准的UUID字符串
// @return string
func GetUUID() string {
	return strings.Replace(getStandardUUID(), "-", "", -1)
}

// GetUUIDWithSeed 基于输入的种子构建UUID字符串
// @param seed
// @return string
func GetUUIDWithSeed(seed int64) string {
	r := rand.New(rand.NewSource(seed)) // nolint: gosec
	uuid, _ := uuid.NewRandomFromReader(r)
	return strings.Replace(uuid.String(), "-", "", -1)
}
