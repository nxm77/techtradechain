FROM golang:1.16.6 as builder
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.cn,direct
ENV GOPRIVATE=techtradechain.com
# you can modify hosts when develop
# RUN echo "${ipaddr} techtradechain.com" >> /etc/hosts
RUN git config --global url."git://".insteadOf https://
COPY . /consensus-utils
RUN cd /consensus-utils && go mod tidy && make

# the second stage
FROM ubuntu:20.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN apt-get update && apt-get install -y vim net-tools tree gcc g++
ENV TZ "Asia/Shanghai"
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata && \
    echo $TZ > /etc/timezone && \
    ln -fs /usr/share/zoneinfo/$TZ /etc/localtime && \
    dpkg-reconfigure tzdata -f noninteractive

WORKDIR /consensus-utils/testframework