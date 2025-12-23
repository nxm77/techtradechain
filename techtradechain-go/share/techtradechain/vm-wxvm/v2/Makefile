VERSION=v2.3.5

build:
	go mod tidy && go build ./...

gomod:
	go get techtradechain.com/techtradechain/common/v2@$(VERSION)
	go get techtradechain.com/techtradechain/protocol/v2@v2.3.6
	go get techtradechain.com/techtradechain/pb-go/v2@v2.3.6
	go get techtradechain.com/techtradechain/utils/v2@$(VERSION)
	go mod tidy

lint:
	golangci-lint run ./...