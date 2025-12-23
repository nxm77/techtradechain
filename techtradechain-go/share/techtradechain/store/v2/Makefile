VERSION=v2.3.6

build:
	go mod tidy && go build ./...

gomod:
	go get techtradechain.com/techtradechain/common/v2@v2.3.8_qc
	go get techtradechain.com/techtradechain/pb-go/v2@v2.3.6_qc
	go get techtradechain.com/techtradechain/protocol/v2@v2.3.9_qc
	go get techtradechain.com/techtradechain/store-badgerdb/v2@v2.3.4_qc
	go get techtradechain.com/techtradechain/store-leveldb/v2@v2.3.4_qc
	go get techtradechain.com/techtradechain/store-sqldb/v2@v2.3.4_qc
	go get techtradechain.com/techtradechain/store-tikv/v2@v2.3.4_qc
	go get techtradechain.com/techtradechain/utils/v2@v2.3.5_qc
	go get techtradechain.com/techtradechain/lws@v1.2.1
	go mod tidy

ut:
	go test ./...
	(gocloc --include-lang=Go --output-type=json . | jq '(.total.comment-.total.files*6)/(.total.code+.total.comment)*100')

mockgen:
	mockgen -destination ./statedb/statedb_mock.go -package statedb -source ./statedb/statedb.go
	mockgen -destination ./blockdb/blockdb_mock.go -package blockdb -source ./blockdb/blockdb.go
	mockgen -destination ./resultdb/resultdb_mock.go -package resultdb -source ./resultdb/resultdb.go

lint:
	golangci-lint run ./...