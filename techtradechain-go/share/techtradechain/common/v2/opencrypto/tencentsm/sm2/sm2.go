/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sm2

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	"techtradechain.com/techtradechain/common/v2/opencrypto/utils"

	"techtradechain.com/techtradechain/common/v2/opencrypto/tencentsm/tencentsm"
)

// GenerateKeyPair generate a private key
func GenerateKeyPair() (*PrivateKey, error) {
	var sk [SM2_PRIVATE_KEY_STR_LEN]byte
	var pk [SM2_PUBLIC_KEY_STR_LEN]byte

	var ctx tencentsm.SM2_ctx_t
	tencentsm.SM2InitCtx(&ctx)

	ret := tencentsm.GenerateKeyPair(&ctx, sk[:], pk[:])
	if ret != 0 {
		return nil, fmt.Errorf("fail to generate SM2 key pair: internal error")
	}

	skD := new(big.Int)
	skD, ok := skD.SetString(string(sk[0:SM2_PRIVATE_KEY_SIZE]), 16)
	if !ok {
		return nil, fmt.Errorf("fail to generate SM2 key pair: wrong private key")
	}

	pkX := new(big.Int)
	pkX, ok = pkX.SetString(string(pk[2:(SM2_PUBLIC_KEY_STR_LEN/2)]), 16)
	if !ok {
		return nil, fmt.Errorf("fail to generate SM2 key pair: wrong public key")
	}
	pkY := new(big.Int)
	pkY, ok = pkY.SetString(string(pk[(SM2_PUBLIC_KEY_STR_LEN/2):SM2_PUBLIC_KEY_SIZE]), 16)
	if !ok {
		return nil, fmt.Errorf("fail to generate SM2 key pair: wrong public key")
	}

	pkStruct := PublicKey{
		Curve: utils.P256Sm2(),
		X:     pkX,
		Y:     pkY,
		Text:  pk[:],
		pool:  NewCtxPoolWithPubKey(pk[:]),
	}

	skStruct := PrivateKey{
		pub:  pkStruct,
		D:    skD,
		Text: sk[:],
	}

	return &skStruct, nil
}

func signWithMode(sk *PrivateKey, msg, id []byte, mode tencentsm.SM2SignMode) ([]byte, error) {
	ctx := sk.pub.pool.GetCtx()
	defer sk.pub.pool.ReleaseCtx(ctx)

	var sigLen int
	sig := make([]byte, SM2_SIGNATURE_MAX_SIZE)
	skByte := sk.Text
	pkByte := sk.pub.Text
	ret := tencentsm.SM2SignWithMode(
		ctx,
		msg[:],
		len(msg),
		id[:],
		len(id),
		pkByte[:],
		SM2_PUBLIC_KEY_SIZE,
		skByte[:],
		SM2_PRIVATE_KEY_SIZE,
		sig[:],
		&sigLen,
		mode,
	)
	if ret != 0 {
		return nil, errors.New("SM2: fail to sign message")
	}
	return sig[0:sigLen], nil
}

// nolint
//CFCA证书若签名为31位，会补0，go本身是不补，长度写31
//兼容 去掉补0，长度改为31
func GetSignatureFromCFCA(signature []byte) []byte {
	dataLength := len(signature)
	dataIndex := 2 //当前下标，初始值为循环数据开始的位置

	//格式为 类型(1)+总长度(1)+[类型(1)+长度(1)+数据]
	//数据字节数为长度对应的大小，一般为32
	var signBuffer bytes.Buffer
	signBuffer.Write(signature[0:dataIndex])
	currentCount := signature[1]  //结构体总长度，用于减去补0后，总长度同样需要减
	currentDataCount := byte('0') //循环中有效数据实际长度
	dataCount := 0                //用于循环中记录每个数据的长度
	zeroCount := 0                //用于循环中记录出现的补0的个数
	for dataIndex+2 < dataLength {
		signBuffer.WriteByte(signature[dataIndex])
		dataCount = int(signature[dataIndex+1])
		if dataIndex+dataCount+2 > dataLength {
			signBuffer.Write(signature[dataIndex+1:])
			break
		}
		//只对长度为32字节的处理，如33字节表示正数但最高位为0需补符号，属于正常
		if 0 == signature[dataIndex+2] && 0 == signature[dataIndex+3]&0x80 {
			currentDataCount = signature[dataIndex+1] - 1
			zeroCount = 1
			//判断是否补多个0
			for {
				if 0 == signature[dataIndex+2+zeroCount] && 0 == signature[dataIndex+3+zeroCount]&0x80 {
					currentDataCount -= 1
					zeroCount += 1
				} else {
					break
				}
			}
			signBuffer.WriteByte(currentDataCount)
			signBuffer.Write(signature[dataIndex+2+zeroCount : dataIndex+2+dataCount])
			currentCount -= signature[dataIndex+1] - currentDataCount
		} else {
			signBuffer.Write(signature[dataIndex+1 : dataIndex+dataCount+2])
		}

		dataIndex += dataCount + 2
	}

	signature = signBuffer.Bytes()

	if 0 < signature[1]-currentCount {
		signature[1] = currentCount
	}

	return signature
}
