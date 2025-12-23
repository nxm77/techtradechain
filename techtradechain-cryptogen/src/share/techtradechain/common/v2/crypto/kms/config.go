/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package kms config
package kms

// Config Kms Configuration
type Config struct {
	SecretId  string
	SecretKey string
	Address   string
	Region    string
	SDKScheme string

	IsPublic bool
}

// PrivateKey kms private key struct
type PrivateKey struct {
	KeyId     string
	KeyType   string
	KeyAlias  string
	ExtParams map[string]string
}
