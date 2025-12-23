## 基于Docker一键起链脚本使用说明

（1）编译techtradechain docker镜像

```bash
$ cd techtradechain-go
# 生成镜像名称为：techtradechain:v1.0.0_r，如需要修改版本，请修改Makefile文件
$ make docker-build
```

（2）启停solo节点

> 使用的配置文件在`techtradechain-go/scripts/docker/config/solo`
>
> 在`config`目录汇总提供了`crypto-config`，`SDK`可以直接引用

```bash
$ cd techtradechain-go/scripts/docker/
# 如镜像名称有调整，请修改solo.docker-compose.yml文件
$ ./solo_up.sh 
$ ./solo_down.sh
```

（3）启停4节点集群

> 使用的配置文件在`techtradechain-go/scripts/docker/config/four-nodes`

```bash
$ cd techtradechain-go/scripts/docker/
# 如镜像名称有调整，请修改four-nodes.docker-compose.yml文件
$ ./four-nodes_up.sh 
$ ./four-nodes_down.sh
```
