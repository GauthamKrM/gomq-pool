package config

import (
	"os"
	"strconv"
	"time"

	_ "github.com/joho/godotenv/autoload"
)

type Config struct {
	RabbitMQ RabbitMQConfig
	Producer ProducerConfig
	Consumer ConsumerConfig
}

type RabbitMQConfig struct {
	URL           string
	Queue         string
	PrefetchCount int
}

type ProducerConfig struct {
	PublishTimeout time.Duration
}

type ConsumerConfig struct {
	PoolSize      int
	WorkerTimeout time.Duration
}

// LoadConfig reads environment variables and returns a typed Config struct
func LoadConfig() (*Config, error) {
	publishTimout, err := time.ParseDuration(getEnv("PUBLISH_TIMEOUT", "5s"))
	if err != nil {
		return nil, err
	}

	workerTimeout, err := time.ParseDuration(getEnv("WORKER_TIMEOUT", "30S"))
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		RabbitMQ: RabbitMQConfig{
			URL:           getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
			Queue:         getEnv("RABBITMQ_QUEUE", "test_queue"),
			PrefetchCount: getEnvInt("CONSUMER_PREFETCH_COUNT", 1),
		},
		Producer: ProducerConfig{
			PublishTimeout: publishTimout,
		},
		Consumer: ConsumerConfig{
			PoolSize:      getEnvInt("CONSUMER_POOL_SIZE", 5),
			WorkerTimeout: workerTimeout,
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

func getEnvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
