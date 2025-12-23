PLATFORM=$(shell uname -m)
DATETIME=$(shell date "+%Y%m%d%H%M%S")
VERSION=v2.3.5


build: techtradechain-cryptogen

vendor:
	# 注意：执行此方法前需要切换common项目到对应分支或commit
	# 密码学 gmssl 相关
	@cp -a ../common/opencrypto/gmssl/gmssl/include/openssl/ ./src/vendor/techtradechain.com/techtradechain/common/v2/opencrypto/gmssl/gmssl/
	@cp -a ../common/opencrypto/gmssl/gmssl/lib/linux ./src/vendor/techtradechain.com/techtradechain/common/v2/opencrypto/gmssl/gmssl/lib/
	# 密码学 tencentsm 相关
	@cp -a ../common/opencrypto/tencentsm/tencentsm/include ./src/vendor/techtradechain.com/techtradechain/common/v2/opencrypto/tencentsm/tencentsm/
	@cp -a ../common/opencrypto/tencentsm/tencentsm/lib ./src/vendor/techtradechain.com/techtradechain/common/v2/opencrypto/tencentsm/tencentsm/
	@# 密码学 bulletproofs 相关
	@#cp -a ../common/crypto/bulletproofs/bulletproofs_cgo/c_include ./src/vendor/techtradechain.com/techtradechain/common/v2/crypto/bulletproofs/bulletproofs_cgo/
	@#cp -a ../common/crypto/bulletproofs/bulletproofs_cgo/c_lib ./src/ven/dor/techtradechain.com/techtradechain/common/v2/crypto/bulletproofs/bulletproofs_cgo
	@cd src && go build -mod=vendor -o ../bin/techtradechain-cryptogen
	@mkdir -p ./release
	@rm -rf ./tmp/techtradechain-cryptogen/
	@mkdir -p ./tmp/techtradechain-cryptogen/
	@mkdir ./tmp/techtradechain-cryptogen/bin
	@mkdir ./tmp/techtradechain-cryptogen/config
	@cp bin/techtradechain-cryptogen ./tmp/techtradechain-cryptogen/bin
	@cp -r config/crypto_config_template.yml ./tmp/techtradechain-cryptogen/config
	@cd ./tmp;tar -zcvf techtradechain-cryptogen-$(VERSION).$(DATETIME).$(PLATFORM).tar.gz techtradechain-cryptogen; mv techtradechain-cryptogen-$(VERSION).$(DATETIME).$(PLATFORM).tar.gz ../release
	@rm -rf ./tmp/

techtradechain-cryptogen:
	@cd src && go mod tidy && GOPATH=${GOPATH} go build -o ../bin/techtradechain-cryptogen
	@mkdir -p ./release
	@rm -rf ./tmp/techtradechain-cryptogen/
	@mkdir -p ./tmp/techtradechain-cryptogen/
	@mkdir ./tmp/techtradechain-cryptogen/bin
	@mkdir ./tmp/techtradechain-cryptogen/config
	@cp bin/techtradechain-cryptogen ./tmp/techtradechain-cryptogen/bin
	@cp -r config/crypto_config_template.yml ./tmp/techtradechain-cryptogen/config
	@cd ./tmp;tar -zcvf techtradechain-cryptogen-$(VERSION).$(DATETIME).$(PLATFORM).tar.gz techtradechain-cryptogen; mv techtradechain-cryptogen-$(VERSION).$(DATETIME).$(PLATFORM).tar.gz ../release
	@rm -rf ./tmp/

clean:
	@rm -rf ./bin

gomod:
	@cd src && go get techtradechain.com/techtradechain/common/v2@$(VERSION)
	@cd src && go mod tidy
