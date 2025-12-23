/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import "strings"

const (
	protocolSeparator = "::"
)

// CreateProtocolWithChainIdAndFlag will create a protocol string with a chain id string and a message flag string.
func CreateProtocolWithChainIdAndFlag(chainId, flag string) string {
	var builder strings.Builder
	builder.WriteString(chainId)
	builder.WriteString(protocolSeparator)
	builder.WriteString(flag)
	return builder.String()
}

// GetChainIdAndFlagWithProtocol will load the chain id string and the message flag string from a protocol string.
func GetChainIdAndFlagWithProtocol(p string) (chainId, flag string) {
	arr := strings.SplitN(p, protocolSeparator, 2)
	return arr[0], arr[1]
}
