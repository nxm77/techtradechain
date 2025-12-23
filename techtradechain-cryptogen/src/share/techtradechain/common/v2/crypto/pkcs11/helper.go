/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkcs11

import (
	"fmt"

	"github.com/miekg/pkcs11"
	"github.com/pkg/errors"
)

func (p11 *P11Handle) findObjects(template []*pkcs11.Attribute, max int) ([]pkcs11.ObjectHandle, error) {
	session, err := p11.getSession()
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to get session [%s]", err)
	}
	defer p11.returnSession(err, session)

	if err = p11.ctx.FindObjectsInit(session, template); err != nil {
		return nil, err
	}

	if max <= 0 {
		max = 100
	}

	objectHandles, _, err := p11.ctx.FindObjects(session, max)
	if err != nil {
		return nil, err
	}

	if err := p11.ctx.FindObjectsFinal(session); err != nil {
		return nil, err
	}

	if len(objectHandles) == 0 {
		return nil, errors.New("no objects found")
	}

	return objectHandles, nil
}

func (p11 *P11Handle) findObject(template []*pkcs11.Attribute) (*pkcs11.ObjectHandle, error) {
	objects, err := p11.findObjects(template, 1)
	if err != nil {
		return nil, err
	}
	if len(objects) > 1 {
		return nil, errors.New("too many objects matching template")
	}
	return &objects[0], nil
}

func (p11 *P11Handle) findPrivateKey(id []byte) (*pkcs11.ObjectHandle, error) {
	if obj, err := p11.findPrivateKeyByLabel(id); err == nil {
		return obj, nil
	}
	return p11.findPrivateKeyBySKI(id)
}

func (p11 *P11Handle) findPublicKey(id []byte) (*pkcs11.ObjectHandle, error) {
	if obj, err := p11.findPublicKeyByLabel(id); err == nil {
		return obj, nil
	}
	return p11.findPublicKeyBySKI(id)
}

func (p11 *P11Handle) findPrivateKeyByLabel(label []byte) (*pkcs11.ObjectHandle, error) {
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_CLASS, pkcs11.CKO_PRIVATE_KEY),
		pkcs11.NewAttribute(pkcs11.CKA_LABEL, label),
	}
	return p11.findObject(template)
}

func (p11 *P11Handle) findPublicKeyByLabel(label []byte) (*pkcs11.ObjectHandle, error) {
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_CLASS, pkcs11.CKO_PUBLIC_KEY),
		pkcs11.NewAttribute(pkcs11.CKA_LABEL, label),
	}
	return p11.findObject(template)
}

func (p11 *P11Handle) findPrivateKeyBySKI(ski []byte) (*pkcs11.ObjectHandle, error) {
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_CLASS, pkcs11.CKO_PRIVATE_KEY),
		pkcs11.NewAttribute(pkcs11.CKA_ID, ski),
	}
	return p11.findObject(template)
}

func (p11 *P11Handle) findPublicKeyBySKI(ski []byte) (*pkcs11.ObjectHandle, error) {
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_CLASS, pkcs11.CKO_PUBLIC_KEY),
		pkcs11.NewAttribute(pkcs11.CKA_ID, ski),
	}
	return p11.findObject(template)
}

func (p11 *P11Handle) findSecretKey(id []byte) (*pkcs11.ObjectHandle, error) {
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_CLASS, pkcs11.CKO_SECRET_KEY),
		pkcs11.NewAttribute(pkcs11.CKA_LABEL, id),
	}
	handle, err := p11.findObject(template)
	if err == nil {
		return handle, nil
	}

	template = []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_CLASS, pkcs11.CKO_SECRET_KEY),
		pkcs11.NewAttribute(pkcs11.CKA_ID, id),
	}
	handle, err = p11.findObject(template)
	if err == nil {
		return handle, nil
	}

	return nil, err
}

// getAttributes returns key's attribute which corresponds to id
func (p11 *P11Handle) getAttributes(id []byte, template []*pkcs11.Attribute) ([]*pkcs11.Attribute, error) {
	session, err := p11.getSession()
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to get session [%s]", err)
	}
	defer p11.returnSession(err, session)

	obj, err := p11.findPublicKey(id)
	if err != nil {
		return nil, err
	}

	return p11.ctx.GetAttributeValue(session, *obj, template)
}

//func (p11 *P11Handle) getAttributesByPubKey(id []byte, template []*pkcs11.Attribute) ([]*pkcs11.Attribute, error) {
//	session, err := p11.getSession()
//	if err != nil {
//		return nil, fmt.Errorf("PKCS11 error: fail to get session [%s]", err)
//	}
//	defer p11.returnSession(err, session)
//
//	obj, err := p11.findPublicKey(id)
//	if err != nil {
//		return nil, err
//	}
//
//	return p11.ctx.GetAttributeValue(session, *obj, template)
//}
