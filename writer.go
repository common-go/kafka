package kafka

import (
	"github.com/segmentio/kafka-go"
)

func NewWriter(topic string, brokers []string, dialer *kafka.Dialer) *kafka.Writer {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Dialer:   dialer,
		Balancer: &kafka.LeastBytes{},
	})
	return writer
}

func MapToHeader(messageAttributes map[string]string) []kafka.Header {
	headers := make([]kafka.Header, 0)
	for k, v := range messageAttributes {
		h := kafka.Header{Key: k, Value: []byte(v)}
		headers = append(headers, h)
	}
	return headers
}
