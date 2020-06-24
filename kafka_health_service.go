package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"time"
)

type KafkaHealthService struct {
	Brokers []string
	name     string
}

func NewDefaultKafkaHealthService(brokers []string) *KafkaHealthService {
	return NewKafkaHealthService(brokers, "kafka")
}

func NewKafkaHealthService(brokers []string, name string) *KafkaHealthService {
	return &KafkaHealthService{brokers, name}
}

func (s *KafkaHealthService) Name() string {
	return s.name
}

func (s *KafkaHealthService) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	for _, broker := range s.Brokers {
		conn, err := dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			return nil, err
		}
		conn.Close()
	}
	res["stats"] = "success"
	return res, nil
}

func (s *KafkaHealthService) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
