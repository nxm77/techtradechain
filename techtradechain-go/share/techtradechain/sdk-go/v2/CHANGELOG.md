<!--
Guiding Principles:

Changelogs are for humans, not machines.
There should be an entry for every single version.
The same types of changes should be grouped.
Versions and sections should be linkable.
The latest version comes first.
The release date of each version is displayed.
Mention whether you follow Semantic Versioning.

Usage:

Change log entries are to be added to the Unreleased section under the
appropriate stanza (see below). Each entry should ideally include a tag and
the Github issue reference in the following format:

* (<tag>) \#<issue-number> message

The issue numbers will later be link-ified during the release process so you do
not have to worry about including a link manually, but you can if you wish.

Types of changes (Stanzas):

"Features" for new features.
"Improvements" for changes in existing functionality.
"Deprecated" for soon-to-be removed features.
"Bug Fixes" for any bug fixes.
"API Breaking" for breaking exported APIs used by developers building on SDK.
Ref: https://keepachangelog.com/en/1.0.0/
-->

# Changelog

## [Unreleased]

### Features

### Improvements

### API Breaking Changes

### Bug Fixes

### Deprecated

## v2.3.7
### Features
##### 增加SubscribeBlockWithOptimize功能（优化订阅区块）
**参数说明**
- startBlock: 订阅起始区块高度，包含起始区块。若为-1，表示订阅实时最新区块
- endBlock: 订阅结束区块高度，包含结束区块。若为-1，表示订阅实时最新区块
- withRwSet: 是否返回读写集
- onlyHeader: 若设置为true，将忽略withRwSet选项，仅返回区块头（common.BlockHeader）,若设置为false，将返回common.BlockInfo
- optimizeDetection：优化检查的周期，单位：秒
- switchHeightDiff：连接切换时最小差别的区块高度，即如果系统发现最高的区块高度比当前超过该值，会切换连接
```go
	SubscribeBlockWithOptimize(ctx context.Context, startBlock, endBlock int64, withRWSet, onlyHeader bool,
optimizeDetection int, switchHeightDiff int64) (<-chan interface{}, error)
```

##### 增加SubscribeContractEventWithOptimize功能（优化订阅合约事件）
**参数说明**
- startBlock: 订阅起始区块高度，包含起始区块。若为-1，表示订阅实时最新区块
- endBlock: 订阅结束区块高度，包含结束区块。若为-1，表示订阅实时最新区块
- contractName ：指定订阅的合约名称
- topic ：指定订阅主题
- optimizeDetection：优化检查的周期，单位：秒
- switchHeightDiff：连接切换时最小差别的区块高度，即如果系统发现最高的区块高度比当前超过该值，会切换连接
```go
	SubscribeContractEventWithOptimize(ctx context.Context, startBlock, endBlock int64, contractName,
topic string, optimizeDetection int, switchHeightDiff int64) (<-chan interface{}, error)
```

##### 增加配置：optimize_detection
* (优化) 乐观检查连接的周期，单位：秒
* 每隔optimize_detection秒，系统会自动检查连接的情况，根据最新区块自动调整使用优先级

##### 增加配置：node_only_async
* (优化) 对端节点是否仅支持异步方式，而不支持同步方式
* 默认为false，表示可以支持同步，仅有老版本的底链才仅支持（小于v2.3.3）

## v2.3.4

### Features

* (订阅) 交易订阅支持别名/组织名/txid的前缀
* (grpc) pk 模式支持grpc tls的配置

## v2.3.3

### Features

##### 设置Payer
**参数说明**
- payerAddress: payer的地址
- contractName: 合约名称
- methodName: 方法名称
- requestId: 请求ID
- payerOrgId: payer的组织ID
- payerKeyPem: payer的私钥
- payerCertPem: payer的证书
- gasLimit: 燃料限制
```go
	SetContractMethodPayer(
        payerAddress string, contractName string, methodName string, requestId string,
        payerOrgId string, payerKeyPem []byte, payerCertPem []byte,
        gasLimit uint64) (
        *common.TxResponse, error)
```

##### 取消Payer
**参数说明**
- contractName: 合约名称
- methodName: 方法名称
- gasLimit: 燃料限制
```go
	UnsetContractMethodPayer(
        contractName string, methodName string,
        gasLimit uint64) (
        *common.TxResponse, error)
```

##### 查询设置的Payer
**参数说明**
- contractName: 合约名称
- methodName: 方法名称
- gasLimit: 燃料限制
```go
	QueryContractMethodPayer(
        contractName string, methodName string, gasLimit uint64) (*common.TxResponse, error)
```

##### 查询交易的Payer
**参数说明**
- txId: 交易ID
- gasLimit: 燃料限制
```go
	QueryTxPayer(txId string, gasLimit uint64) (*common.TxResponse, error) 
```

## v2.3.2

### Features

##### 归档系列API 兼容了`归档中心`作为链下存储
##### 获取节点归档状态详细信息
```
GetArchiveStatus
```

##### 发送合约管理请求（创建、更新、冻结、解冻、吊销）
```
SendContractManageRequestWithPayer
```

##### 发起多签请求(指定gas代扣账户)
```
MultiSignContractReqWithPayer
```

##### 触发执行多签请求(指定gas代扣账户)
```
MultiSignContractTrigWithPayer
```

##### 根据发起多签请求所需的参数构建payload
```
CreateMultiSignReqPayloadWithGasLimit
```

##### 发起多签投票
```
MultiSignContractVoteWithGasLimit
```

##### 发起多签投票(指定gas代扣账户)
```
MultiSignContractVoteWithGasLimitAndPayer
```

##### 根据txId查询多签状态
```
MultiSignContractQueryWithParams
```

##### 设置链配置的 default gas_price 参数
```
CreateSetInvokeGasPricePayload
```

##### 设置链配置的 install_base gas 参数
```
CreateSetInstallBaseGasPayload
```

##### 设置链配置的 install gas_price 参数
```
CreateSetInstallGasPricePayload
```

## v2.3.1

### Features

##### 发起多签请求
**参数说明**
- payload: 待签名payload
- endorsers: 背书签名信息列表
- timeout: 超时时间，单位：s，若传入-1，将使用默认超时时间：10s
- withSyncResult: 是否同步获取交易执行结果
  当为true时，若成功调用，common.TxResponse.ContractResult.Result为common.TransactionInfo
  当为false时，若成功调用，common.TxResponse.ContractResult为空，可以通过common.TxResponse.TxId查询交易结果
```go
	MultiSignContractReq(payload *common.Payload, endorsers []*common.EndorsementEntry, timeout int64,
		withSyncResult bool) (*common.TxResponse, error)
```

##### 发起多签投票
**参数说明**
- payload: 待签名payload
- endorser: 投票人对多签请求 payload 的签名信息
- isAgree: 投票人对多签请求是否同意，true为同意，false则反对
- timeout: 超时时间，单位：s，若传入-1，将使用默认超时时间：10s
- withSyncResult: 是否同步获取交易执行结果
  当为true时，若成功调用，common.TxResponse.ContractResult.Result为common.TransactionInfo
  当为false时，若成功调用，common.TxResponse.ContractResult为空，可以通过common.TxResponse.TxId查询交易结果
```go
	MultiSignContractVote(payload *common.Payload, endorser *common.EndorsementEntry, isAgree bool,
		timeout int64, withSyncResult bool) (*common.TxResponse, error)
```

##### 触发执行多签请求
**参数说明**
- payload: 待签名payload
- timeout: 超时时间，单位：s，若传入-1，将使用默认超时时间：10s
  //	 - limit: 本次执行多签请求支付的 gas 上限
- withSyncResult: 是否同步获取交易执行结果
  当为true时，若成功调用，common.TxResponse.ContractResult.Result为common.TransactionInfo
  当为false时，若成功调用，common.TxResponse.ContractResult为空，可以通过common.TxResponse.TxId查询交易结果
```go
	MultiSignContractTrig(multiSignReqPayload *common.Payload,
		timeout int64, limit *common.Limit, withSyncResult bool) (*common.TxResponse, error)
```

## v2.3.0 bugfix

## v2.2.1 bugfix

## v2.2.0 - 2021-12-17

### Features

* (gas) 新增启用/停用gas计费开关API
* (gas) 新增 attach `Limit` API
* (通用) 新增修改地址类型API
* (通用) 提供至信链地址生成相关API
* (Grpc client) grpc客户端发送消息时，可设置允许单条message大小的最大值(MB)

### Improvements

* (订阅) 支持订阅断线自动重连机制
* (订阅) 支持合约事件按照区块高度订阅历史事件

## v2.3.3

### Features

##### 设置Payer
**参数说明**
- payerAddress: payer的地址
- contractName: 合约名称
- methodName: 方法名称
- requestId: 请求ID
- payerOrgId: payer的组织ID
- payerKeyPem: payer的私钥
- payerCertPem: payer的证书
- gasLimit: 燃料限制
```go
	SetContractMethodPayer(
        payerAddress string, contractName string, methodName string, requestId string,
        payerOrgId string, payerKeyPem []byte, payerCertPem []byte,
        gasLimit uint64) (
        *common.TxResponse, error)
```

##### 取消Payer
**参数说明**
- contractName: 合约名称
- methodName: 方法名称
- gasLimit: 燃料限制
```go
	UnsetContractMethodPayer(
        contractName string, methodName string,
        gasLimit uint64) (
        *common.TxResponse, error)
```

##### 查询设置的Payer
**参数说明**
- contractName: 合约名称
- methodName: 方法名称
- gasLimit: 燃料限制
```go
	QueryContractMethodPayer(
        contractName string, methodName string, gasLimit uint64) (*common.TxResponse, error)
```

##### 查询交易的Payer
**参数说明**
- txId: 交易ID
- gasLimit: 燃料限制
```go
	QueryTxPayer(txId string, gasLimit uint64) (*common.TxResponse, error) 
```

## v2.3.4

### Features

* (订阅) 交易订阅支持别名/组织名/txid的前缀
* (grpc) pk 模式支持grpc tls的配置