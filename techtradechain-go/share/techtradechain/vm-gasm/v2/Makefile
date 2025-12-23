VERSION=v2.3.5

gomod:
	go get techtradechain.com/techtradechain/common/v2@$(VERSION)
	go get techtradechain.com/techtradechain/logger/v2@v2.3.4
	go get techtradechain.com/techtradechain/pb-go/v2@v2.3.6
	go get techtradechain.com/techtradechain/protocol/v2@v2.3.6
	go get techtradechain.com/techtradechain/store/v2@v2.3.6
	go get techtradechain.com/techtradechain/vm/v2@v2.3.6
	go mod tidy

lint:
	golangci-lint run ./...

test:
	go test -v ./...