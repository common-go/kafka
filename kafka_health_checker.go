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

func NewHealthChecker(brokers []string, options ...string) *KafkaHealthChecker {
	var name string
	if len(options) >= 1 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "kafka"
	}
	return NewKafkaHealthChecker(brokers, name, 4)
}

func NewKafkaHealthChecker(brokers []string, name string, timeouts ...int64) *KafkaHealthChecker {
	var timeout int64
	if len(timeouts) >= 1 {
		timeout = timeouts[0]
	} else {
		timeout = 4
	}
	return &KafkaHealthChecker{Brokers: brokers, Service: name, Timeout: timeout}
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
