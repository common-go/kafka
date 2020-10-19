package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"time"
)

type KafkaHealthCheck struct {
	Brokers []string
	Service string
	Timeout int64
}

func NewDefaultKafkaHealthCheck(brokers []string) *KafkaHealthCheck {
	return NewKafkaHealthCheck(brokers, "kafka", 5)
}

func NewKafkaHealthCheck(brokers []string, name string, timeout int64) *KafkaHealthCheck {
	return &KafkaHealthCheck{brokers, name, timeout}
}

func (s *KafkaHealthCheck) Name() string {
	return s.Service
}

func (s *KafkaHealthCheck) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})

	dialer := &kafka.Dialer{
		Timeout:   time.Duration(s.Timeout) * time.Second,
		DualStack: true,
	}
	for _, broker := range s.Brokers {
		conn, err := dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			return nil, err
		}
		conn.Close()
	}
	res["status"] = "success"
	return res, nil
}

func (s *KafkaHealthCheck) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
