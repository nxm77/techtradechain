/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"sync"

	"techtradechain.com/techtradechain/vm-engine/v2/pb/protogo"
)

var pbPool = sync.Pool{
	New: func() interface{} {
		return &protogo.DockerVMMessage{}
	},
}

// ReturnToPool return pb msg to pool
func ReturnToPool(m *protogo.DockerVMMessage) {
	if m != nil {
		m.Reset()
		pbPool.Put(m)
	}
}

// DockerVMMessageFromPool get pb msg from pool
func DockerVMMessageFromPool() *protogo.DockerVMMessage {
	msg, ok := pbPool.Get().(*protogo.DockerVMMessage)
	if ok {
		return msg
	}
	return nil
}
