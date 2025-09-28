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
	MainExchange  string
	RetryExchange string
	DLQExchange   string
	RoutingKey    string
	RetryQueue    string
	DLQQueue      string
	Durable       bool
}

type ProducerConfig struct {
	PublishTimeout time.Duration
}

type ConsumerConfig struct {
	PoolSize       int
	WorkerTimeout  time.Duration
	MaxRetries     int
	RetryBaseDelay time.Duration
}

// LoadConfig reads environment variables and returns a typed Config struct
func LoadConfig() (*Config, error) {
	publishTimout, err := time.ParseDuration(getEnv("PUBLISH_TIMEOUT", "5s"))
	if err != nil {
		return nil, err
	}

	workerTimeout, err := time.ParseDuration(getEnv("WORKER_TIMEOUT", "30s"))
	if err != nil {
		return nil, err
	}

	retryBaseDelay, err := time.ParseDuration(getEnv("CONSUMER_RETRY_BASE_DELAY", "5s"))
	if err != nil {
		return nil, err
	}

	queue := getEnv("RABBITMQ_QUEUE", "test_queue")
	routingKey := getEnv("RABBITMQ_ROUTING_KEY", queue)

	cfg := &Config{
		RabbitMQ: RabbitMQConfig{
			URL:           getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
			Queue:         queue,
			PrefetchCount: getEnvInt("RABBITMQ_PREFETCH_COUNT", 1),

			MainExchange:  getEnv("RABBITMQ_MAIN_EXCHANGE", "app.main"),
			RetryExchange: getEnv("RABBITMQ_RETRY_EXCHANGE", "app.retry"),
			DLQExchange:   getEnv("RABBITMQ_DLQ_EXCHANGE", "app.dlq"),

			RoutingKey: routingKey,
			RetryQueue: getEnv("RABBITMQ_RETRY_QUEUE", queue+".retry"),
			DLQQueue:   getEnv("RABBITMQ_DLQ_QUEUE", queue+".dlq"),

			Durable: getEnvBool("RABBITMQ_DURABLE", true),
		},
		Producer: ProducerConfig{
			PublishTimeout: publishTimout,
		},
		Consumer: ConsumerConfig{
			PoolSize:       getEnvInt("CONSUMER_POOL_SIZE", 5),
			WorkerTimeout:  workerTimeout,
			MaxRetries:     getEnvInt("CONSUMER_MAX_RETRIES", 3),
			RetryBaseDelay: retryBaseDelay,
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

func getEnvBool(key string, fallback bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if v == "1" || v == "true" || v == "TRUE" || v == "True" {
			return true
		}
		if v == "0" || v == "false" || v == "FALSE" || v == "False" {
			return false
		}
	}
	return fallback
}
