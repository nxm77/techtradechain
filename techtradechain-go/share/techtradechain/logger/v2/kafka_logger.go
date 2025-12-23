/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package logger

import (
	"github.com/Shopify/sarama"
)

// KafkaLogger 基于Kafka的Logger记录器
type KafkaLogger struct {
	Producer sarama.SyncProducer
	Topic    string
}

// Write 写入日志到Kafka
// @param p
// @return n
// @return err
func (lk *KafkaLogger) Write(p []byte) (n int, err error) {
	msg := &sarama.ProducerMessage{}
	msg.Topic = lk.Topic
	msg.Value = sarama.ByteEncoder(p)
	_, _, err = lk.Producer.SendMessage(msg)
	if err != nil {
		return
	}
	return
}

// NewKafkaProducer 基于配置，初始化一个新的Kafka生产者
// @param logConfig
// @param chainId
// @return *KafkaLogger
// @return error
func NewKafkaProducer(logConfig *KafkaLogConfig, chainId string) (*KafkaLogger, error) {
	var (
		kafkaLogger KafkaLogger
		err         error
	)
	kafkaLogger.Topic = logConfig.GetTopic(chainId)
	// 设置日志输入到Kafka的配置
	config := sarama.NewConfig()
	//等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForLocal
	//随机的分区类型
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//是否等待成功和失败后的响应,只有上面的RequireAcks设置不是NoReponse这里才有用.
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	//安全认证
	if logConfig.Sasl != nil {
		if len(logConfig.Sasl.Mechanism) == 0 {
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		} else {
			config.Net.SASL.Mechanism = sarama.SASLMechanism(logConfig.Sasl.Mechanism)
		}
		config.Net.SASL.Version = int16(logConfig.Sasl.Version)
		config.Net.SASL.Enable = logConfig.Sasl.Enable
		config.Net.SASL.User = logConfig.Sasl.UserName
		config.Net.SASL.Password = logConfig.Sasl.Password
	}
	if len(logConfig.KafkaVersion) != 0 {
		config.Version, _ = sarama.ParseKafkaVersion(logConfig.KafkaVersion)
	}
	config.Producer.Compression = sarama.CompressionCodec(logConfig.Compression)

	kafkaLogger.Producer, err = sarama.NewSyncProducer(logConfig.Servers, config)
	if err != nil {
		return nil, err
	}
	return &kafkaLogger, nil
}
