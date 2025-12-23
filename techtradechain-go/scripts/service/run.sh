#!/usr/bin/env bash

start() {
	export LD_LIBRARY_PATH=$(dirname $PWD)/lib:$LD_LIBRARY_PATH
  export PATH=$(dirname $PWD)/lib:$PATH
  export WASMER_BACKTRACE=1
  pid=`ps -ef | grep techtradechain | grep "\-c ../config/{org_id}/techtradechain.yml" | grep -v grep |  awk  '{print $2}'`
  if [ -z ${pid} ];then
      nohup ./techtradechain start -c ../config/{org_id}/techtradechain.yml > panic.log &
      echo "techtradechain is starting, pls check log..."
  else
      echo "techtradechain is already started"
  fi
}

stop() {
  pid=`ps -ef | grep techtradechain | grep "\-c ../config/{org_id}/techtradechain.yml" | grep -v grep |  awk  '{print $2}'`
  if [ ! -z ${pid} ];then
      kill $pid
  fi
  echo "techtradechain is stopped"
}

case "$1" in
    start)
      start
    	;;
    stop)
      stop
    	;;
    restart)
    	echo "techtradechain restart"
    	stop
    	start
    	;;
    *)
        echo "you can use: $0 [start|stop|restart]"
	exit 1 
esac

exit 0
