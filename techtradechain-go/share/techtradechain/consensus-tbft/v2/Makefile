VERSION=v2.3.6

build:
	go mod tidy && go build ./...

gomod:
	go get techtradechain.com/techtradechain/chainconf/v2@v2.3.5_qc
	go get techtradechain.com/techtradechain/common/v2@v2.3.8_qc
	go get techtradechain.com/techtradechain/consensus-utils/v2@v2.3.4_qc
	go get techtradechain.com/techtradechain/localconf/v2@v2.3.7_qc
	go get techtradechain.com/techtradechain/logger/v2@v2.3.4_qc
	go get techtradechain.com/techtradechain/lws@v1.2.1
	go get techtradechain.com/techtradechain/pb-go/v2@v2.3.6_qc
	go get techtradechain.com/techtradechain/protocol/v2@v2.3.9_qc
	go get techtradechain.com/techtradechain/utils/v2@v2.3.5_qc
	go mod tidy

lint:
	golangci-lint run ./...