#!/bin/bash

#export PD_HOST=172.16.8.120
source $HOME/.bashrc

hostIP=$PD_HOST
if [[ $hostIP == "" ]];then
  hostIP=`hostname -I | cut -d' ' -f1`
  #echo "error: please set env variable:PD_HOST by server IP address"
  #exit 2
fi

#docker pull pingcap/pd:v5.1.4
#docker pull pingcap/tikv:v5.1.4
docker stop pd1
docker stop tikv1
docker rm pd1 || true
docker rm tikv1 || true

docker run --rm=true -d --name pd1 -p 2379:2379 -p 2380:2380 -v /etc/localtime:/etc/localtime:ro pingcap/pd:v5.1.4 --name="pd1" --data-dir="/data/pd1" \
--client-urls="http://0.0.0.0:2379" --advertise-client-urls="http://${hostIP}:2379" --peer-urls="http://0.0.0.0:2380" \
--advertise-peer-urls="http://${hostIP}:2380" --initial-cluster="pd1=http://${hostIP}:2380"

docker run --rm=true -d --name tikv1 -p 20160:20160 -v /etc/localtime:/etc/localtime:ro pingcap/tikv:v5.1.4 \
--addr="0.0.0.0:20160" --advertise-addr="${hostIP}:20160" --data-dir="/data/tikv1" --pd="${hostIP}:2379"


#echo "finish start all redisbloom docker container"
exit 0

