package kafka

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/common-go/mq"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	Reader       *kafka.Reader
	AckOnConsume bool
}

func NewConsumer(reader *kafka.Reader, ackOnConsume bool) (*Consumer, error) {
	return &Consumer{Reader: reader, AckOnConsume: ackOnConsume}, nil
}

func NewConsumerByConfig(c ConsumerConfig, ackOnConsume bool) (*Consumer, error) {
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	reader := NewReader(c, dialer)
	return NewConsumer(reader, ackOnConsume)
}

func (c *Consumer) Consume(ctx context.Context, caller mq.ConsumerCaller) {
	msg, err := c.Reader.FetchMessage(ctx)
	if err != nil {
		caller.Call(ctx, nil, err)
	}
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logrus.Debugf("Received message: %s", msg.Value)
	}
	attributes := HeaderToMap(msg.Headers)
	message := mq.Message{
		Id:         string(msg.Key),
		Data:       msg.Value,
		Attributes: attributes,
		Raw:        msg,
	}
	if c.AckOnConsume {
		c.Reader.CommitMessages(ctx, msg)
	}
	caller.Call(ctx, &message, nil)
}
