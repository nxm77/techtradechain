/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package abi

import (
	"bytes"
	"fmt"
	"io"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/common/v2/evmutils/abi/burrow/abi"
)

type ABI struct {
	spec *abi.Spec
}

// NewAbi create a new ABI instance by abi string
// @param abiData
// @return *ABI
// @return error
func NewAbi(abiJSON string) (*ABI, error) {
	spec, err := abi.ReadSpec([]byte(abiJSON))
	if err != nil {
		return nil, err
	}
	return &ABI{spec: spec}, nil
}

// NewAbiFromFile create an ABI instance by abi json file path
// @param fpath
// @return *ABI
// @return error
func NewAbiFromFile(fpath string) (*ABI, error) {
	spec, err := abi.ReadSpecFile(fpath)
	if err != nil {
		return nil, err
	}
	return &ABI{spec: spec}, nil
}

// JSON create an ABI instance by Json Reader
// @param reader
// @return *ABI
// @return error
func JSON(reader io.Reader) (*ABI, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(reader)
	if err != nil {
		return nil, err
	}
	abiString := buf.String()
	return NewAbi(abiString)
}

// Pack encode function name and arguments to a binary data
// @param funcName
// @param args
// @return []byte
// @return error
func (a *ABI) Pack(funcName string, args ...interface{}) ([]byte, error) {
	data, _, err := a.spec.Pack(funcName, convertArgs(args...)...)
	return data, err
}

// Unpack decode function out put binary data to interface(string) array
// @param funcName
// @param data
// @return []interface{}
// @return error
func (a *ABI) Unpack(funcName string, data []byte) ([]interface{}, error) {
	fspec, ok := a.spec.Functions[funcName]
	if !ok {
		return nil, fmt.Errorf("no such function %s", funcName)
	}
	args := make([]string, len(fspec.Outputs))
	intf := make([]interface{}, len(args))
	for i := range args {
		intf[i] = &args[i]
	}
	err := abi.Unpack(fspec.Outputs, data, intf...)
	if err != nil {
		return nil, err
	}
	returnArgs := make([]interface{}, len(args))
	for i, arg := range args {
		returnArgs[i] = arg
	}
	return returnArgs, nil
}

func convertArgs(args ...interface{}) []interface{} {
	returns := make([]interface{}, len(args))
	for i, arg := range args {
		returns[i] = convertType(arg)
	}
	return returns
}
func convertType(arg interface{}) interface{} {
	switch arg := arg.(type) {
	case evmutils.Address:
		addr := arg
		return addr[:]
	case *evmutils.Address:
		addr := *arg
		return addr[:]
	case []evmutils.Address:
		addrs := []string{}
		for _, addr := range arg {
			addrs = append(addrs, addr.String())
		}
		return addrs
	case []*evmutils.Address:
		addrs := []string{}
		for _, addr := range arg {
			addrs = append(addrs, addr.String())
		}
		return addrs
	default:
		//fmt.Printf("%#v\n", t)
		return arg
	}
}
