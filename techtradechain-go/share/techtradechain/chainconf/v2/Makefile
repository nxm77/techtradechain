VERSION=v2.3.5

build:
	go mod tidy && go build ./...

ut:
	mkdir -p ../ut
	go test -covermode=count -coverprofile=../ut/chainconf.out ./...
	go tool cover -html=../ut/chainconf.out -o=../ut/chainconf.html

lint:
	golangci-lint run ./...

gomod:
	go get techtradechain.com/techtradechain/common/v2@v2.3.8_qc
	go get techtradechain.com/techtradechain/logger/v2@v2.3.4_qc
	go get techtradechain.com/techtradechain/pb-go/v2@v2.3.7
	go get techtradechain.com/techtradechain/protocol/v2@v2.3.9
	go get techtradechain.com/techtradechain/utils/v2@v2.3.5_qc
	go mod tidy

comment:
	gocloc --include-lang=Go --output-type=json --not-match=_test.go . | jq '(.total.comment-.total.files*6)/(.total.code+.total.comment)*100'
