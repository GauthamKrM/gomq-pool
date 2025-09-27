package config

import (
	"os"
	"time"

	_ "github.com/joho/godotenv/autoload"
)

type Config struct {
	RabbitMQ RabbitMQConfig
	Producer ProducerConfig
}

type RabbitMQConfig struct {
	URL   string
	Queue string
}

type ProducerConfig struct {
	PublishTimeout time.Duration
}

// LoadConfig reads environment variables and returns a typed Config struct
func LoadConfig() (*Config, error) {
	publishTimout, err := time.ParseDuration(getEnv("PUBLISH_TIMEOUT", "5s"))
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		RabbitMQ: RabbitMQConfig{
			URL:   getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
			Queue: getEnv("RABBITMQ_QUEUE", "test_queue"),
		},
		Producer: ProducerConfig{
			PublishTimeout: publishTimout,
		},
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
