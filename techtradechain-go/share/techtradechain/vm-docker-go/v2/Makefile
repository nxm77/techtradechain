VERSION=v2.3.5
IMAGE_VERSION=v2.3.0

BUILD_TIME = $(shell date "+%Y%m%d%H%M%S")
GIT_BRANCH = $(shell git rev-parse --abbrev-ref HEAD)
GIT_COMMIT = $(shell git log --pretty=format:'%h' -n 1)

build-test:
	cd test/scripts && ./prepare.sh

build-image:
	cd vm_mgr && go mod vendor
	cd vm_mgr && docker build -t  techtradechain-vm-docker-go \
	--build-arg BUILD_TIME=${BUILD_TIME} \
	--build-arg GIT_BRANCH=${GIT_BRANCH} \
	--build-arg GIT_COMMIT=${GIT_COMMIT} \
	-f Dockerfile ./
	docker tag techtradechain-vm-docker-go techtradechainofficial/techtradechain-vm-docker-go:${IMAGE_VERSION}
	docker images | grep techtradechain-vm-docker-go
	docker image prune -f

image-push:
	docker push techtradechainofficial/techtradechain-vm-docker-go:${IMAGE_VERSION}

update-gomod:
	cd vm_mgr && rm -rf vendor
	cd scripts && ./gomod_update.sh

gen-cdm:
	cd pb/proto && protoc -I=. --gogofaster_out=plugins=grpc:../protogo --gogofaster_opt=paths=source_relative cdm_message.proto
	cd vm_mgr/pb/proto && protoc -I=. --gogofaster_out=plugins=grpc:../protogo --gogofaster_opt=paths=source_relative cdm_message.proto

gen-dms:
	cd vm_mgr/pb_sdk/proto && protoc -I=. --gogofaster_out=plugins=grpc:../protogo --gogofaster_opt=paths=source_relative dms_message.proto

clean-test:
	cd test/scripts && ./dockerclean.sh

clean:
	cd vm_mgr && rm -rf vendor
	cd test/scripts && ./dockerclean.sh
	docker image rm techtradechainofficial/techtradechain-vm-docker-go:${IMAGE_VERSION}
	docker image prune -f

ci:
	make build-test
	golangci-lint run ./...
	make clean

gomod:
	go get techtradechain.com/techtradechain/common/v2@$(VERSION)
	go get techtradechain.com/techtradechain/localconf/v2@$(VERSION)
	go get techtradechain.com/techtradechain/logger/v2@v2.3.4
	go get techtradechain.com/techtradechain/pb-go/v2@v2.3.6
	go get techtradechain.com/techtradechain/protocol/v2@v2.3.6
	go get techtradechain.com/techtradechain/utils/v2@$(VERSION)
	go mod tidy

ut:
	./test/scripts/prepare.sh
	make build-image
	docker run -itd --rm -p22359:22359 -e ENV_LOG_IN_CONSOLE=true --privileged --name chaimaker_vm_test techtradechainofficial/techtradechain-vm-docker-go:${IMAGE_VERSION}
	./ut_cover.sh
	docker stop chaimaker_vm_test
	make clean

version:
	docker inspect techtradechainofficial/techtradechain-vm-docker-go:${IMAGE_VERSION} | jq '.[].ContainerConfig.Labels'

solo:
	./scripts/solo.sh

solo-stop:
	docker stop techtradechain_vm_solo

lint:
	golangci-lint run ./... --skip-dirs="vm_mgr" --skip-dirs="test"

build:
	go mod tidy && go build ./...