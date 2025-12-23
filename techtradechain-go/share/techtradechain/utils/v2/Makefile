VERSION=v2.3.6

gomod:
	go get techtradechain.com/techtradechain/common/v2@${VERSION}
	go get techtradechain.com/techtradechain/pb-go/v2@${VERSION}
	go get techtradechain.com/techtradechain/protocol/v2@v2.3.7
	go mod tidy

lint:
	golangci-lint run ./...