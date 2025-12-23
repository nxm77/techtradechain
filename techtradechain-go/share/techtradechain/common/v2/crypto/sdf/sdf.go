/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sdf

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto/sdf/base"
	"github.com/pkg/errors"
)

const (
	defaultSessionSize = 10
)

// SDFHandle represents a sdf handle
type SDFHandle struct {
	ctx              *base.Ctx
	deviceHandle     base.SessionHandle
	sessions         chan base.SessionHandle
	sessionCacheSize int
}

// New returns a HSM SDFHandle to provide go-sdf functionalities.
func New(lib string, sessionCacheSize int) (*SDFHandle, error) {
	ctx := base.New(lib)
	if ctx == nil {
		libEnv := os.Getenv("HSM_LIB")
		log.Printf("lib[%s] invalid, use HSM_LIB[%s] from env\n", lib, libEnv)
		ctx = base.New(libEnv)
		if ctx == nil {
			return nil, fmt.Errorf("[SDF] error: fail to initialize [%s]", libEnv)
		}
	}

	if sessionCacheSize <= 0 {
		sessionSizeStr := os.Getenv("HSM_SESSION_CACHE_SIZE")
		sessionSize, err := strconv.Atoi(sessionSizeStr)
		if err == nil && sessionSize > 0 {
			log.Printf("sessionCacheSize[%d] invalid, use HSM_SESSION_CACHE_SIZE[%s] from env\n",
				sessionCacheSize, sessionSizeStr)
			sessionCacheSize = sessionSize
		} else {
			log.Printf("sessionCacheSize[%d] and HSM_SESSION_CACHE_SIZE[%s] invalid, use default size[%d]\n",
				sessionCacheSize, sessionSizeStr, defaultSessionSize)
			sessionCacheSize = defaultSessionSize
		}
	}

	var err error
	var deviceHandle base.SessionHandle
	for i := 0; i < 3; i++ {
		deviceHandle, err = ctx.SDFOpenDevice()
		if err != nil {
			continue
		}
		break
	}
	if err != nil {
		return nil, fmt.Errorf("[SDF] error: fail to open device after 3 times [%v]", err)
	}

	sessions := make(chan base.SessionHandle, sessionCacheSize)
	handle := &SDFHandle{
		ctx:              ctx,
		deviceHandle:     deviceHandle,
		sessions:         sessions,
		sessionCacheSize: sessionCacheSize,
	}
	return handle, nil
}

func (h *SDFHandle) getSession() (base.SessionHandle, error) {
	var session base.SessionHandle
	select {
	case session = <-h.sessions:
		return session, nil
	default:
		var err error
		for i := 0; i < 3; i++ {
			session, err = h.ctx.SDFOpenSession(h.deviceHandle)
			if err == nil {
				return session, nil
			}
			time.Sleep(time.Millisecond * 100)
		}
		return nil, errors.WithMessage(err, "failed to create new session after 3 times attempt")
	}
}

func (h *SDFHandle) returnSession(err error, session base.SessionHandle) {
	if err != nil {
		_ = h.ctx.SDFCloseSession(session)
	}
	select {
	case h.sessions <- session:
		return
	default:
		_ = h.ctx.SDFCloseSession(session)
		return
	}
}

// Close used to close sdf sessions
func (h *SDFHandle) Close() error {
	//close channel to avoid creating new session
	close(h.sessions)

	//close all sessions
	for session := range h.sessions {
		err := h.ctx.SDFCloseSession(session)
		if err != nil {
			return err
		}
	}

	//close device
	return h.ctx.SDFCloseDevice(h.deviceHandle)
}
