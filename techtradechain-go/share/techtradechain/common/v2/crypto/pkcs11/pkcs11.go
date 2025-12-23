/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkcs11

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/miekg/pkcs11"
	"github.com/pkg/errors"
)

const (
	defaultSessionSize = 10
)

// P11Handle represents a pkcs11 handle
type P11Handle struct {
	ctx              *pkcs11.Ctx
	sessions         chan pkcs11.SessionHandle
	slot             uint
	sessionCacheSize int
	hash             string

	pin string
}

// New is used to new a pkcs11 handle
func New(lib string, label string, password string, sessionCacheSize int, hash string) (*P11Handle, error) {
	ctx := pkcs11.New(lib)
	if ctx == nil {
		libEnv := os.Getenv("HSM_LIB")
		log.Printf("lib[%s] invalid, use HSM_LIB[%s] from env\n", lib, libEnv)
		ctx = pkcs11.New(libEnv)
		if ctx == nil {
			return nil, fmt.Errorf("[PKCS11] error: fail to initialize [%s]", libEnv)
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

	err := ctx.Initialize()
	if err != nil {
		return nil, err
	}

	slots, err := ctx.GetSlotList(true)
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to get slot list [%v]", err)
	}

	found := false
	var slot uint
	slot, found = findSlot(ctx, slots, label)
	if !found {
		labelEnv := os.Getenv("HSM_LABEL")
		log.Printf("label[%s] invalid, use HSM_LABEL[%s] from env\n", label, labelEnv)
		slot, found = findSlot(ctx, slots, labelEnv)
		if !found {
			return nil, fmt.Errorf("PKCS11 error: fail to find token with label[%s] or HSM_LABEL[%s]", label, labelEnv)
		}
	}

	var session pkcs11.SessionHandle
	for i := 0; i < 3; i++ {
		session, err = ctx.OpenSession(slot, pkcs11.CKF_SERIAL_SESSION|pkcs11.CKF_RW_SESSION)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to open session [%v]", err)
	}

	err = ctx.Login(session, pkcs11.CKU_USER, password)
	if err != nil {
		passEnv := os.Getenv("HSM_PASSWORD")
		log.Printf("password[%s] invalid, use HSM_PASSWORD[%s] from env\n",
			hex.EncodeToString([]byte(password)), hex.EncodeToString([]byte(passEnv)))
		err = ctx.Login(session, pkcs11.CKU_USER, passEnv)
		if err != nil {
			return nil, fmt.Errorf("PKCS11 error: fail to login session [%v]", err)
		}
	}

	sessions := make(chan pkcs11.SessionHandle, sessionCacheSize)
	p11Handle := &P11Handle{
		ctx:              ctx,
		sessions:         sessions,
		slot:             slot,
		sessionCacheSize: sessionCacheSize,
		hash:             hash,
		pin:              password,
	}
	p11Handle.returnSession(nil, session)

	return p11Handle, nil
}

func (p11 *P11Handle) getSession() (pkcs11.SessionHandle, error) {
	var session pkcs11.SessionHandle
	select {
	case session = <-p11.sessions:
		return session, nil
	default:
		var err error
		for i := 0; i < 3; i++ {
			session, err = p11.ctx.OpenSession(p11.slot, pkcs11.CKF_SERIAL_SESSION|pkcs11.CKF_RW_SESSION)
			if err == nil {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
		if err != nil {
			return 0, errors.WithMessage(err, "fail to open session after 3 times attempt")
		}

		err = p11.ctx.Login(session, pkcs11.CKU_USER, p11.pin)
		if err != nil && err != pkcs11.Error(pkcs11.CKR_USER_ALREADY_LOGGED_IN) {
			_ = p11.ctx.CloseSession(session)
			return 0, errors.WithMessage(err, "login failed")
		}
		return session, nil
	}
}

func (p11 *P11Handle) returnSession(err error, session pkcs11.SessionHandle) {
	if err != nil {
		log.Printf("PKCS11 session invalidated, closing session: %v", err)
		_ = p11.ctx.CloseSession(session)
		return
	}
	select {
	case p11.sessions <- session:
		return
	default:
		_ = p11.ctx.CloseSession(session)
		return
	}
}

func findSlot(ctx *pkcs11.Ctx, slots []uint, label string) (uint, bool) {
	var slot uint
	var found bool
	for _, s := range slots {
		info, err := ctx.GetTokenInfo(s)
		if err != nil {
			continue
		}
		if info.Label == label {
			found = true
			slot = s
			break
		}
	}
	return slot, found
}

func listSlot(ctx *pkcs11.Ctx) (map[string]string, error) {
	slots, err := ctx.GetSlotList(true)
	if err != nil {
		return nil, err
	}

	res := make(map[string]string)
	for i, s := range slots {
		info, err := ctx.GetTokenInfo(s)
		if err != nil {
			return nil, err
		}
		res[fmt.Sprintf("%d", i)] = info.Label
	}
	return res, nil
}
