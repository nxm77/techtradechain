#!/bin/bash

#
# Copyright (C) BABEC. All rights reserved.
# Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

export LD_LIBRARY_PATH=$(dirname $PWD)/:$LD_LIBRARY_PATH
export PATH=$(dirname $PWD)/prebuilt/linux:$(dirname $PWD)/prebuilt/win64:$PATH
export WASMER_BACKTRACE=1
cp -rf ../../bin ./
export CMC=$PWD/bin
echo "export CMC=" $CMC

cd bin


pid=`ps -ef | grep techtradechain | grep "\-c ../config/wx-org1.techtradechain.com/techtradechain.yml ci-chain2" | grep -v grep |  awk  '{print $2}'`
if [ -z ${pid} ];then
    nohup ./techtradechain start -c ../config/wx-org1.techtradechain.com/techtradechain.yml ci-chain2 > panic1.log 2>&1 &
    echo "wx-org1.techtradechain.com techtradechain is starting, pls check log..."
else
    echo "wx-org1.techtradechain.com techtradechain is already started"
fi

pid2=`ps -ef | grep techtradechain | grep "\-c ../config/wx-org2.techtradechain.com/techtradechain.yml ci-chain2" | grep -v grep |  awk  '{print $2}'`
if [ -z ${pid2} ];then
    nohup ./techtradechain start -c ../config/wx-org2.techtradechain.com/techtradechain.yml ci-chain2 > panic2.log 2>&1 &
    echo "wx-org2.techtradechain.com techtradechain is starting, pls check log..."
else
    echo "wx-org2.techtradechain.com techtradechain is already started"
fi



pid3=`ps -ef | grep techtradechain | grep "\-c ../config/wx-org3.techtradechain.com/techtradechain.yml ci-chain2" | grep -v grep |  awk  '{print $2}'`
if [ -z ${pid3} ];then
    nohup ./techtradechain start -c ../config/wx-org3.techtradechain.com/techtradechain.yml ci-chain2 > panic3.log 2>&1 &
    echo "wx-org3.techtradechain.com techtradechain is starting, pls check log..."
else
    echo "wx-org3.techtradechain.com techtradechain is already started"
fi


pid4=`ps -ef | grep techtradechain | grep "\-c ../config/wx-org4.techtradechain.com/techtradechain.yml ci-chain2" | grep -v grep |  awk  '{print $2}'`
if [ -z ${pid4} ];then
    nohup ./techtradechain start -c ../config/wx-org4.techtradechain.com/techtradechain.yml ci-chain2 > panic4.log 2>&1 &
    echo "wx-org4.techtradechain.com techtradechain is starting, pls check log..."
else
    echo "wx-org4.techtradechain.com techtradechain is already started"
fi

# nohup ./techtradechain start -c ../config/node5/techtradechain.yml ci-chain2 > panic.log &
echo "sleep 12s to wait Raft wakeup..."
sleep 12
ps -ef|grep techtradechain | grep "ci-chain2"
