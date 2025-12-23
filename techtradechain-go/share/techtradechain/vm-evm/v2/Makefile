VERSION=v2.3.6

build:
	go mod tidy && go build ./...

gomod:
	go get techtradechain.com/techtradechain/common/v2@$(VERSION)
	go get techtradechain.com/techtradechain/logger/v2@v2.3.4
	go get techtradechain.com/techtradechain/pb-go/v2@$(VERSION)
	go get techtradechain.com/techtradechain/protocol/v2@v2.3.7
	go get techtradechain.com/techtradechain/utils/v2@$(VERSION)
	go mod tidy

lint:
	golangci-lint run ./... --skip-dirs="evm-go/opcodes" --skip-dirs="evm-go/precompiledContracts"