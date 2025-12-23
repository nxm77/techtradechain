VERSION=v2.3.5

build:
	go mod tidy && go build ./...

gomod:
	go get techtradechain.com/techtradechain/common/v2@v2.3.4
	go get techtradechain.com/techtradechain/localconf/v2@v2.3.4
	go get techtradechain.com/techtradechain/logger/v2@v2.3.4
	go get techtradechain.com/techtradechain/net-common@v1.2.5
	go get techtradechain.com/techtradechain/net-libp2p@v1.2.5
	go get techtradechain.com/techtradechain/net-liquid@v1.1.3
	go get techtradechain.com/techtradechain/pb-go/v2@$(VERSION)
	go get techtradechain.com/techtradechain/protocol/v2@$(VERSION)
	go get techtradechain.com/techtradechain/utils/v2@$(VERSION)
	go mod tidy

lint:
	golangci-lint run ./...
