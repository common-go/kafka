package kafka

import "time"

type ConsumerConfig struct {
	Brokers        []string      `mapstructure:"brokers"`
	GroupID        string        `mapstructure:"group_id"`
	Topic          string        `mapstructure:"topic"`
	MinBytes       int           `mapstructure:"min_bytes"`
	MaxBytes       int           `mapstructure:"max_bytes"`
	CommitInterval time.Duration `mapstructure:"commit_interval"`
}
