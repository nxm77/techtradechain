#!/usr/bin/env bash
#
# Copyright (C) BABEC. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
## deploy TechTradeChain and test

node_count=$1
chain_count=$2
alreadyBuild=$3
enableDocker=$4

if [ ! -d "../../tools/techtradechain-cryptogen" ]; then
  echo "not found techtradechain-go/tools/techtradechain-cryptogen"
  echo "  you can use "
  echo "              cd techtradechain-go/tools"
  echo "              ln -s ../../techtradechain-cryptogen ."
  echo "              cd techtradechain-cryptogen && make"
  exit 0
fi

CURRENT_PATH=$(pwd)
SCRIPT_PATH=$(dirname "${CURRENT_PATH}")
PROJECT_PATH=$(dirname "${SCRIPT_PATH}")

if  [[ ! -n $node_count ]] ;then
  echo "node cnt is empty"
  echo "  default port is 11391 12391, you can use "
  echo "             ./quick_deploy.sh nodeCount chainCount alreadyBuild enableDockerVm"
  echo "             ./quick_deploy.sh 4 1 false true"
    exit 1
fi
if  [ ! $node_count -eq 1 ] && [ ! $node_count -eq 4 ] && [ ! $node_count -eq 7 ]&& [ ! $node_count -eq 10 ]&& [ ! $node_count -eq 13 ]&& [ ! $node_count -eq 16 ];then
    echo "node cnt should be 1 or 4 or 7 or 10 or 13"
    exit 1
fi

function start_techtradechain() {
  cd $SCRIPT_PATH
  ./cluster_quick_stop.sh clean

  if [ "${alreadyBuild}" != "true" ]; then
    echo -e "\n\n【generate】 certs and config..."
    if [ "${enableDocker}" == "true" ]; then
      echo "【sh】 ./prepare.sh 4 1 11391 12391 32391 22391 -c 1 -l INFO -v true  --vtp=tcp --vlog=INFO"
      ./prepare.sh 4 1 11391 12391 32391 22391 -c 1 -l INFO -v true  --vtp=tcp --vlog=INFO
    else
      echo "【sh】 ./prepare.sh 4 1 11391 12391 32391 22391 -c 1 -l INFO -v false  --vtp=tcp --vlog=INFO"
      ./prepare.sh 4 1 11391 12391 32391 22391 -c 1 -l INFO -v false  --vtp=tcp --vlog=INFO
    fi
#    echo -e "\nINFO\n\n\n" | ./prepare.sh $node_count $chain_count
    echo -e "\n\n【build】 release..."
    ./build_release.sh
  fi
  echo -e "\n\n【start】 techtradechain..."
  ./cluster_quick_start.sh normal
  sleep 3

  echo "【techtradechain】 process..."
  ps -ef | grep techtradechain
  techtradechain_count=$(ps -ef | grep techtradechain | wc -l)
  if [ $techtradechain_count -lt 4 ]; then
    echo "build error"
    exit
  fi

  # backups *.gz
  cd $PROJECT_PATH/build
  mkdir -p bak
  mv release/*.gz bak/
}

function prepare_cmc() {
  if [ "${alreadyBuild}" != "true" ]; then
    echo "【build】 cmc start..."
    cd $PROJECT_PATH
    make cmc
  fi

  echo "【prepare】 cmc cert and sdk..."
  cd $PROJECT_PATH/bin
  pwd
  rm -rf testdata
  mkdir testdata
  cp $PROJECT_PATH/tools/cmc/testdata/sdk_config.yml testdata/
  sed -i 's/12301/12391/' testdata/sdk_config.yml
  cp -r $PROJECT_PATH/build/crypto-config/ testdata/
}

function cmc_test() {
  echo "【cmc】 send tx..."
  cd $PROJECT_PATH/bin
  pwd

  ## create contract
  ./cmc client contract user create \
    --contract-name=fact \
    --runtime-type=WASMER \
    --byte-code-path=../test/wasm/rust-fact-2.0.0.wasm \
    --version=1.0 \
    --sdk-conf-path=./testdata/sdk_config.yml \
    --admin-key-file-paths=./testdata/crypto-config/wx-org1.techtradechain.com/user/admin1/admin1.tls.key,./testdata/crypto-config/wx-org2.techtradechain.com/user/admin1/admin1.tls.key,./testdata/crypto-config/wx-org3.techtradechain.com/user/admin1/admin1.tls.key,./testdata/crypto-config/wx-org4.techtradechain.com/user/admin1/admin1.tls.key \
    --admin-crt-file-paths=./testdata/crypto-config/wx-org1.techtradechain.com/user/admin1/admin1.tls.crt,./testdata/crypto-config/wx-org2.techtradechain.com/user/admin1/admin1.tls.crt,./testdata/crypto-config/wx-org3.techtradechain.com/user/admin1/admin1.tls.crt,./testdata/crypto-config/wx-org4.techtradechain.com/user/admin1/admin1.tls.crt \
    --sync-result=true \
    --params="{}"

  ## invoke tx
  ./cmc client contract user invoke \
    --contract-name=fact \
    --method=save \
    --sdk-conf-path=./testdata/sdk_config.yml \
    --params="{\"file_name\":\"name007\",\"file_hash\":\"ab3456df5799b87c77e7f88\",\"time\":\"6543234\"}" \
    --sync-result=true \
    --result-to-string=true

  ## query tx
  ./cmc client contract user get \
    --contract-name=fact \
    --method=find_by_file_hash \
    --sdk-conf-path=./testdata/sdk_config.yml \
    --params="{\"file_hash\":\"ab3456df5799b87c77e7f88\"}" \
    --result-to-string=true
}

function cat_log() {
  sleep 1
  grep --color=auto "all necessary\|ERROR\|put block" $PROJECT_PATH/build/release/techtradechain-*1*/log/system.log
}

start_techtradechain
prepare_cmc
cmc_test
cat_log