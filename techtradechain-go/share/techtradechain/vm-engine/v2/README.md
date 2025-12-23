# vm-engine 单独部署

本Readme简要介绍了配置说明和部署方式，详情请参考技术贸易链官方文档。
## 1. 配置说明

```yml
vm:
  go:
    # 是否启用新版Golang容器
    enable: true
    # 数据挂载路径, 包括合约、sock文件（uds）
    data_mount_path: ../data/wx-org1.techtradechain.com/go
    # 日志挂载路径
    log_mount_path: ../log/wx-org1.techtradechain.com/go
    # techtradechain和合约引擎之间的通信协议（可选tcp/uds）
    protocol: tcp
    # 如果需要自定义高级配置，请将vm.yml文件放入dockervm_config_path中，优先级：techtradechain.yml > vm.yml > 默认配置
    # dockervm_config_path: /config_path/vm.yml
    # 是否在控制台打印日志
    log_in_console: false
    # docker合约引擎的日志级别
    log_level: DEBUG

    # 下面两个server的最大消息发送大小, 默认100MB
    max_send_msg_size: 100
    # 下面两个server的最大消息接收大小, 默认100MB
    max_recv_msg_size: 100
    # 下面两个server的最大连接超时时间, 默认10s
    dial_timeout: 10

    # 合约引擎最多启用的原始合约进程数，默认为20（跨合约调用会额外拉起新的进程）
    max_concurrency: 20

    # 运行时服务器配置 (与合约实例进程交互，进行信息交换)
    runtime_server:
      # 端口号，默认为 32351
      port: 32351

    # 合约引擎服务器配置 (与techtradechain交互，进行交易请求、合约请求等交互)
    contract_engine:
      # 合约引擎服务器ip, 默认为 127.0.0.1
      host: 127.0.0.1
      # 端口号，默认为 22351
      port: 22351
      # 与合约引擎服务器的最大连接数
      max_connection: 5
```

## 2. 部署启动流程


### 2.1. 启动合约服务容器

1. 打包合约服务的镜像

在`vm-engine`项目根目录下执行打包镜像操作
```shell
make build-image 
```

可以通过如下命令查看镜像的版本信息
```shell
docker inspect <image-name> | jq '.[].ContainerConfig.Labels'
```

2. 参数说明：
容器中的参数，如果不设置会采用默认参数，默认如下

``` yml
########### RPC ###########
rpc:
  chain_rpc_protocol: 1 # chain rpc protocol, 0 for unix domain socket, 1 for tcp(default)
  chain_host: 127.0.0.1 # chain tcp host
  chain_rpc_port: 22351 # chain rpc port, valid when protocol is tcp
  sandbox_rpc_port: 32351 # sandbox rpc port, valid when protocol is tcp
  max_send_msg_size: 100 # max send msg size(MiB)
  max_recv_msg_size: 100 # max recv msg size(MiB)
  server_min_interval: 60s # server min interval
  connection_timeout: 5s # connection timeout time
  server_keep_alive_time: 60s # idle duration before server ping
  server_keep_alive_timeout: 20s # ping timeout

########### Process ###########
process:
  # max original process num,
  # max_call_contract_process_num = max_original_process_num * max_contract_depth (defined in protocol)
  # max_total_process_num = max_call_contract_process_num + max_original_process_num
  max_original_process_num: 20
  exec_tx_timeout: 8s # process timeout while busy
  waiting_tx_time: 200ms # process timeout while tx completed (busy -> idle)
  release_rate: 0 # percentage of idle processes released periodically in total processes (0-100)
  release_period: 10m # period of idle processes released periodically in total processes

########### Log ###########
log:
  contract_engine:
    level: "info"
    console: true
  sandbox:
    level: "info"
    console: true

########### Pprof ###########
pprof:
  contract_engine:
    enable: false
    port: 21215
  sandbox:
    enable: false
    port: 21522

########### Contract ###########
contract:
  max_file_size: 20480 # contract size(MiB)
```

3. 启动命令

容器的运行需要privileged的权限，启动命令添加 --privileged 参数

3.1 以tcp方式启动：
参数中需要添加容器内服务端口以及链端服务端口

```shell
docker run -itd \
--net=host \
--privileged \
-e CHAIN_RPC_PORT=22351 \
-e SANDBOX_RPC_PORT=32351 \
--name VM-GO-node1 \
techtradechainofficial/techtradechain-vm-engine:v2.3.1
```

3.1 以uds方式启动：
参数中需要添加：
1. 启动uds的配置
2. 通过 -v 指定本地合约文件和socket文件的映射

```shell
docker run -itd \
--net=host \
--privileged \
-v /data/techtradechain/node1/go:/mount \
-v /data/techtradechain/node1/log:/log \
-e CHAIN_RPC_PROTOCOL="0" \
--name VM-GO-node1 \
techtradechainofficial/techtradechain-vm-engine:v2.3.1
```

### 2.2. 配置启动 techtradechain

修改`techtradechain`配置文件中`vm`配置中的相关配置

#### 2.2.1 tcp方式
保持 protocol 为 tcp，并配置runtime_server中port和contract_engine中port的值

#### 2.2.2 uds方式
保持 protocol 为 uds，并配置data_mount_path和log_mount_path

正常启动techtradechain: ./techtradechain start -c config 