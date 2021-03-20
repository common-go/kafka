package kafka

type ProducerConfig struct {
	Brokers []string     `mapstructure:"brokers"`
	Topic   string       `mapstructure:"topic"`
	Client  ClientConfig `mapstructure:"client"`
	Key     *bool        `mapstructure:"key"`
}
