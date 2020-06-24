package kafka

import (
	"context"
	"strings"

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

func NewProducerByConfig(c ProducerConfig, generateKey bool) (*Producer, error) {
	writer := NewWriter(c.Topic, c.Brokers)
	return NewProducer(writer, generateKey)
}

func (p *Producer) Produce(ctx context.Context, data []byte, messageAttributes *map[string]string) (string, error) {
	msg := kafka.Message{Value: data}
	if messageAttributes != nil  {
		msg.Headers = MapToHeader(*messageAttributes)
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
