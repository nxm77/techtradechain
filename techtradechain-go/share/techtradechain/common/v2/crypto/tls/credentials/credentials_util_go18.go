// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package credentials

import (
	cmtls "techtradechain.com/techtradechain/common/v2/crypto/tls"
)

// cloneTLSConfig returns a shallow clone of the exported
// fields of cfg, ignoring the unexported sync.Once, which
// contains a mutex and must not be copied.
//
// If cfg is nil, a new zero tls.Config is returned.
func cloneTLSConfig(cfg *cmtls.Config) *cmtls.Config {
	if cfg == nil {
		return &cmtls.Config{}
	}

	return cfg.Clone()
}
