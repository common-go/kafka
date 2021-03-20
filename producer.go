package kafka

import (
	"context"
	"crypto/tls"
	"github.com/segmentio/kafka-go/sasl/scram"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	Writer *kafka.Writer
	Key    bool
}

func NewProducer(writer *kafka.Writer, generateKey bool) (*Producer, error) {
	return &Producer{Writer: writer, Key: generateKey}, nil
}

func NewProducerByConfig(c ProducerConfig) (*Producer, error) {
	generateKey := true
	if c.Key != nil {
		generateKey = *c.Key
	}
	dialer := GetDialer(c.Client.Username, c.Client.Password, scram.SHA512, &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	})
	writer := NewWriter(c.Topic, c.Brokers, dialer)
	return NewProducer(writer, generateKey)
}

func (p *Producer) Produce(ctx context.Context, data []byte, messageAttributes map[string]string) (string, error) {
	msg := kafka.Message{Value: data}
	if messageAttributes != nil {
		msg.Headers = MapToHeader(messageAttributes)
	}
	if p.Key {
		id := strings.Replace(uuid.New().String(), "-", "", -1)
		msg.Key = []byte(id)
		err := p.Writer.WriteMessages(ctx, msg)
		return id, err
	} else {
		err := p.Writer.WriteMessages(ctx, msg)
		return "", err
	}
}
