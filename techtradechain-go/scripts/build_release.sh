#!/usr/bin/env bash
#
# Copyright (C) BABEC. All rights reserved.
# Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -e

CURRENT_PATH=$(pwd)
PROJECT_PATH=$(dirname "${CURRENT_PATH}")
BUILD_PATH=${PROJECT_PATH}/build
RELEASE_PATH=${PROJECT_PATH}/build/release
BACKUP_PATH=${PROJECT_PATH}/build/backup
BUILD_CRYPTO_CONFIG_PATH=${BUILD_PATH}/crypto-config
BUILD_CONFIG_PATH=${BUILD_PATH}/config
VERSION=v2.3.7
DATETIME=$(date "+%Y%m%d%H%M%S")
PLATFORM=$(uname -m)
system=$(uname)

function xsed() {

    if [ "${system}" = "Linux" ]; then
        sed -i "$@"
    else
        sed -i '' "$@"
    fi
}

function check_env() {
    if  [ ! -d $BUILD_CONFIG_PATH ] ;then
        echo $BUILD_CONFIG_PATH" is missing"
        exit 1
    fi

    if  [ ! -d $BUILD_CRYPTO_CONFIG_PATH ] ;then
        echo $BUILD_CRYPTO_CONFIG_PATH" is missing"
        exit 1
    fi
}

function build() {
    cd $PROJECT_PATH
    echo "build techtradechain ${PROJECT_PATH}..."
    make
}

function package() {
    if [ -d $RELEASE_PATH ]; then
        mkdir -p $BACKUP_PATH/backup_release
        mv $RELEASE_PATH $BACKUP_PATH/backup_release/release_$(date "+%Y%m%d%H%M%S")
    fi

    mkdir -p $RELEASE_PATH
    cd $RELEASE_PATH
    echo "tar zcf crypto-config..."
    tar -zcf crypto-config-$DATETIME.tar.gz ../crypto-config

    c=0
    dirNames[0]=""
    file1=""
    for file in `ls -v $BUILD_CRYPTO_CONFIG_PATH`
    do
        techtradechain_file=techtradechain-$VERSION-$file
        dirNames[$c]=$techtradechain_file
        c=$(($c+1))
        mkdir $techtradechain_file
        mkdir $techtradechain_file/bin
        mkdir $techtradechain_file/lib
        mkdir -p $techtradechain_file/config/$file
        mkdir $techtradechain_file/log
        cp $PROJECT_PATH/bin/techtradechain   $techtradechain_file/bin
        cp $CURRENT_PATH/bin/start.sh     $techtradechain_file/bin
        cp $CURRENT_PATH/bin/stop.sh      $techtradechain_file/bin
        cp $CURRENT_PATH/bin/restart.sh   $techtradechain_file/bin
        cp $CURRENT_PATH/bin/version.sh   $techtradechain_file/bin
        cp $CURRENT_PATH/bin/docker-vm-standalone-start.sh   $techtradechain_file/bin
        cp $CURRENT_PATH/bin/docker-vm-standalone-stop.sh   $techtradechain_file/bin
        cp $CURRENT_PATH/service/*        $techtradechain_file/bin
        if [ "${system}" = "Linux" ]; then
          cp -r $PROJECT_PATH/main/libwasmer_runtime_c_api.so     $techtradechain_file/lib/libwasmer.so
          cp -r $PROJECT_PATH/main/prebuilt/linux/wxdec           $techtradechain_file/lib/
        else
          cp -r $PROJECT_PATH/main/libwasmer.dylib                $techtradechain_file/lib/
          cp -r $PROJECT_PATH/main/prebuilt/mac/wxdec             $techtradechain_file/lib/
        fi
        chmod 644 $techtradechain_file/lib/*
        chmod 700 $techtradechain_file/lib/wxdec
        chmod 700 $techtradechain_file/bin/*
        cp -r $BUILD_CONFIG_PATH/node$c/* $techtradechain_file/config/$file
        xsed "s%{org_id}%$file%g"         $techtradechain_file/bin/start.sh
        xsed "s%{org_id}%$file%g"         $techtradechain_file/bin/stop.sh
        xsed "s%{org_id}%$file%g"         $techtradechain_file/bin/restart.sh
        xsed "s%{org_id}%$file%g"         $techtradechain_file/bin/run.sh
        d=$(date "+%Y%m%d%H%M%S")
        xsed "s%{tagName}%name-$d%g"         $techtradechain_file/bin/*.sh
        echo "tar zcf ${techtradechain_file}..."
        tar -zcf techtradechain-$VERSION-$file-$DATETIME-$PLATFORM.tar.gz $techtradechain_file &
#        rm -rf $techtradechain_file
    done
    echo "wait tar..."
    wait
    for dirName in ${dirNames[@]}
    do
      # echo "rm -rf $PROJECT_PATH/build/release/$dirName"
      rm -rf $PROJECT_PATH/build/release/$dirName
    done
}

check_env
build
package

