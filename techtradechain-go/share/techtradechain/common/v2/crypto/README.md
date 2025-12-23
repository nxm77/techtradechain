# 简介

crypto 模块提供了一些密码学算法 (包括加密、签名、哈希等) 能力及其相关的协议的接口。

# 密码学算法

## 非对称密码学算法接口

定义了如下的非对称体系公私钥接口：
```go
// Signing options
type SignOpts struct {
	Hash HashType
	UID  string
}

// === 秘钥接口 ===
type Key interface {
	// 获取秘钥字节数组
	Bytes() ([]byte, error)

	// 获取秘钥类型
	Type() KeyType

	// 获取编码后秘钥(PEM格式)
	String() (string, error)
}

// === 非对称秘钥签名+验签接口 ===
// 私钥签名接口
type PrivateKey interface {
	Key

	// 私钥签名
	Sign(data []byte) ([]byte, error)

	SignWithOpts(data []byte, opts *SignOpts) ([]byte, error)

	// 返回公钥
	PublicKey() PublicKey

	// 转换为crypto包中的 PrivateKey 接口类
	ToStandardKey() crypto.PrivateKey
}

// 公钥验签接口
type PublicKey interface {
	Key

	// 公钥验签
	Verify(data []byte, sig []byte) (bool, error)

	VerifyWithOpts(data []byte, sig []byte, opts *SignOpts) (bool, error)

	// 转换为crypto包中的 PublicKey 接口类
	ToStandardKey() crypto.PublicKey
}

```
SignOpts 结构用于为一个签名、验签操作提供灵活的流程变化。其中，Hash 字段可以设置哈希算法，例如 SHA256、SM3 等。UID 字段是 SM2-SM3 签名套件专用字段，用于设置国密局规定的 user ID。

Key 接口定义了密码学公私钥通用的序列化接口，和一个返回密钥算法的 Type() 接口。

PrivateKey 接口用于签名私钥，通常使用的是 SighWithOpts() 接口，其中入参 data 是数据原文，opts是一个 SignOpts 类型的结构，用于指定哈希算法，在 SM2-SM3 签名套件中也用于指定 user ID。在 TechTradeChain中应用时，这个哈希算法可能读取自证书中指定的算法套件，也可能来自配置文件设置。

## 公私钥的序列化

在应用中，公钥、私钥通常会以字符串形式保存在配置文件中或用于传输。前面提到的 Key 接口中的 String() 为公钥提供了序列化为 PEM 格式字符串的能力。

要把字符串形式的公私钥反序列化为对象，可以调用 crypto/asym 包中的 PublicKeyFromPEM() 或 PrivateKeyFromPEM() 接口。TechTradeChain 支持的算法都可以用这两个通用接口反序列化公私钥。

# 证书

TechTradeChain 使用的节点、客户端证书需要满足一下要求：
1. O 字段需要指明节点或客户端所属的组织的名称。
2. OU 字段需要指明节点或客户端的身份，默认身份有四种：admin、client、consensus、common，分别代表管理员、普通用户、共识节点、普通节点。
