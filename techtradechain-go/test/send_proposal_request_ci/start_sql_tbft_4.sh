#
# Copyright (C) BABEC. All rights reserved.
# Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

export LD_LIBRARY_PATH=$(dirname $PWD)/:$LD_LIBRARY_PATH
export PATH=$(dirname $PWD)/prebuilt/linux:$(dirname $PWD)/prebuilt/win64:$PATH
export WASMER_BACKTRACE=1
cp ../../main/techtradechain ./

pid=`ps -ef | grep techtradechain | grep "\-c ./config-sql/wx-org1/techtradechain.yml ci-sql-tbft" | grep -v grep |  awk  '{print $2}'`
if [ -z ${pid} ];then
    nohup ./techtradechain start -c ./config-sql/wx-org1/techtradechain.yml ci-sql-tbft > panic1.log 2>&1 &
    echo "wx-org1 techtradechain is starting, pls check log..."
else
    echo "wx-org1 techtradechain is already started"
fi

pid2=`ps -ef | grep techtradechain | grep "\-c ./config-sql/wx-org2/techtradechain.yml ci-sql-tbft" | grep -v grep |  awk  '{print $2}'`
if [ -z ${pid2} ];then
    nohup ./techtradechain start -c ./config-sql/wx-org2/techtradechain.yml ci-sql-tbft > panic2.log 2>&1 &
    echo "wx-org2 techtradechain is starting, pls check log..."
else
    echo "wx-org2 techtradechain is already started"
fi



pid3=`ps -ef | grep techtradechain | grep "\-c ./config-sql/wx-org3/techtradechain.yml ci-sql-tbft" | grep -v grep |  awk  '{print $2}'`
if [ -z ${pid3} ];then
    nohup ./techtradechain start -c ./config-sql/wx-org3/techtradechain.yml ci-sql-tbft > panic3.log 2>&1 &
    echo "wx-org3 techtradechain is starting, pls check log..."
else
    echo "wx-org3 techtradechain is already started"
fi


pid4=`ps -ef | grep techtradechain | grep "\-c ./config-sql/wx-org4/techtradechain.yml ci-sql-tbft" | grep -v grep |  awk  '{print $2}'`
if [ -z ${pid4} ];then
    nohup ./techtradechain start -c ./config-sql/wx-org4/techtradechain.yml ci-sql-tbft > panic4.log 2>&1 &
    echo "wx-org4 techtradechain is starting, pls check log..."
else
    echo "wx-org4 techtradechain is already started"
fi

# nohup ./techtradechain start -c ./config-sql/wx-org5/techtradechain.yml ci-sql-tbft > panic.log &

sleep 4
ps -ef|grep techtradechain | grep "ci-sql-tbft"
