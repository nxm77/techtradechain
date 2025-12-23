VERSION=v2.3.6

build:
	go mod tidy && go build ./...

lint:
	golangci-lint run ./...

mockgen:
	mockgen -destination ./mock/sdk_mock.go -package mock -source sdk_interface.go

gomod:
	go get techtradechain.com/techtradechain/common/v2@v2.3.8_qc
	go get techtradechain.com/techtradechain/pb-go/v2@v2.3.6_qc
	go get techtradechain.com/techtradechain/protocol/v2@v2.3.9_qc
	go get techtradechain.com/techtradechain/utils/v2@v2.3.5_qc
	go mod tidy

.PHONY: build lint
