#!/usr/bin/env bash
#
# Copyright (C) BABEC. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
## deploy TechTradeChain and test

CURRENT_PATH=$(pwd)
PROJECT_PATH=$(dirname $(dirname "${CURRENT_PATH}"))
#echo "PROJECT_PATH $PROJECT_PATH"

cd $PROJECT_PATH
if [ -e bin/techtradechain ]; then
    echo "skip make, techtradechain binary already exist"
else
    make
fi

cd bin
nohup ./techtradechain start -c ../config/wx-org1/techtradechain.yml start_four_node > panic1.log 2>&1 &
nohup ./techtradechain start -c ../config/wx-org2/techtradechain.yml start_four_node > panic2.log 2>&1 &
nohup ./techtradechain start -c ../config/wx-org3/techtradechain.yml start_four_node > panic3.log 2>&1 &
nohup ./techtradechain start -c ../config/wx-org4/techtradechain.yml start_four_node > panic4.log 2>&1 &
echo "start techtradechain..."