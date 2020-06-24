package kafka

import "github.com/segmentio/kafka-go"

func NewReader(c ConsumerConfig) *kafka.Reader {
	if c.CommitInterval > 0 {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        c.Brokers,
			GroupID:        c.GroupID,
			Topic:          c.Topic,
			MinBytes:       c.MinBytes,
			MaxBytes:       c.MaxBytes,
			CommitInterval: c.CommitInterval,
		})
		return reader
	} else {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  c.Brokers,
			GroupID:  c.GroupID,
			Topic:    c.Topic,
			MinBytes: c.MinBytes,
			MaxBytes: c.MaxBytes,
		})
		return reader
	}
}

func HeaderToMap(headers []kafka.Header) map[string]string {
	attributes := make(map[string]string, 0)
	for i := range headers {
		attributes[headers[i].Key] = string(headers[i].Value)
	}
	return attributes
}
