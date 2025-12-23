#!/usr/bin/env bash
#
# Copyright (C) BABEC. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
## deploy TechTradeChain and test

module=$1

CURRENT_PATH=$(pwd)
PROJECT_PATH=$(dirname $(dirname "${CURRENT_PATH}"))

# stop techtradechain node
for i in {1..5} ; do
  stoping=0
  pid1=`ps -ef | grep techtradechain | grep "\-c ../config/wx-org1/techtradechain.yml start_four_node" | grep -v grep |  awk  '{print $2}'`
  pid2=`ps -ef | grep techtradechain | grep "\-c ../config/wx-org2/techtradechain.yml start_four_node" | grep -v grep |  awk  '{print $2}'`
  pid3=`ps -ef | grep techtradechain | grep "\-c ../config/wx-org3/techtradechain.yml start_four_node" | grep -v grep |  awk  '{print $2}'`
  pid4=`ps -ef | grep techtradechain | grep "\-c ../config/wx-org4/techtradechain.yml start_four_node" | grep -v grep |  awk  '{print $2}'`
  if [ ! -z ${pid1} ];then
      kill $pid1
      echo "techtradechain wx-org1 is stopping..."
      stoping=1
  fi
  if [ ! -z ${pid2} ];then
      kill $pid2
      echo "techtradechain wx-org2 is stopping..."
      stoping=1
  fi
  if [ ! -z ${pid3} ];then
      kill $pid3
      echo "techtradechain wx-org3 is stopping..."
      stoping=1
  fi
  if [ ! -z ${pid4} ];then
      kill $pid4
      echo "techtradechain wx-org4 is stopping..."
      stoping=1
  fi
  if [ ${stoping} == 0 ]; then
      echo "techtradechain stopped"
      if [ ${module} == "clean" ];then
        cd $PROJECT_PATH
        rm -rf log data
        echo "rm -rf $PROJECT_PATH/log $PROJECT_PATH/data"
      fi
      exit 0
  fi
  sleep 1
done
echo
echo "techtradechain stop fail"
echo
ps -ef|grep "techtradechain.yml start_four_node"