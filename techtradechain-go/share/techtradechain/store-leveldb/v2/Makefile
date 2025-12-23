VERSION=v2.3.5

build:
	go mod tidy && go build ./...

gomod:
	go get techtradechain.com/techtradechain/common/v2@v2.3.4
	go get techtradechain.com/techtradechain/protocol/v2@$(VERSION)
	go mod tidy

lint:
	golangci-lint run ./...

ut:
	go test -v ./...
