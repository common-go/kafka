package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"time"
)

type KafkaHealthChecker struct {
	Brokers []string
	Service string
	Timeout int64
}

func NewKafkaHealthChecker(brokers []string, options ...string) *KafkaHealthChecker {
	var name string
	if len(options) >= 1 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "kafka"
	}
	return NewKafkaHealthCheckerWithTimeout(brokers, name, 4)
}

func NewKafkaHealthCheckerWithTimeout(brokers []string, name string, timeout int64) *KafkaHealthChecker {
	return &KafkaHealthChecker{brokers, name, timeout}
}

func (s *KafkaHealthChecker) Name() string {
	return s.Service
}

func (s *KafkaHealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
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

func (s *KafkaHealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
