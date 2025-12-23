// See https://github.com/golang/go/issues/26366.
package lib

import (
	_ "techtradechain.com/techtradechain/vm-wasmer/v2/wasmer-go/packaged/lib/darwin-aarch64"
	_ "techtradechain.com/techtradechain/vm-wasmer/v2/wasmer-go/packaged/lib/darwin-amd64"
	_ "techtradechain.com/techtradechain/vm-wasmer/v2/wasmer-go/packaged/lib/linux-aarch64"
	_ "techtradechain.com/techtradechain/vm-wasmer/v2/wasmer-go/packaged/lib/linux-amd64"
	_ "techtradechain.com/techtradechain/vm-wasmer/v2/wasmer-go/packaged/lib/windows-amd64"
)
