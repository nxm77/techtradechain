VERSION=v2.3.6

build:
	go mod tidy && go build ./...

ut:
	mkdir -p ../ut
	go test -covermode=count -coverprofile=../ut/vm.out ./...
	go tool cover -html=../ut/vm.out -o=../ut/vm.html

lint:
	golangci-lint run ./...

gomod:
	go get techtradechain.com/techtradechain/pb-go/v2@v2.3.6_qc
	go get techtradechain.com/techtradechain/common/v2@v2.3.8_qc
	go get techtradechain.com/techtradechain/protocol/v2@v2.3.9_qc
	go get techtradechain.com/techtradechain/utils/v2@v2.3.5_qc
	go get techtradechain.com/techtradechain/chainconf/v2@v2.3.5_qc
	go get techtradechain.com/techtradechain/vm-native/v2@v2.3.7_qc
	go get techtradechain.com/techtradechain/logger/v2@v2.3.4_qc
	go mod tidy
