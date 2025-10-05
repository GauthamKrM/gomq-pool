package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"time"

	"gomq-pool/config"
	"gomq-pool/internal/mq"
	"gomq-pool/internal/types"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Constants
const totalMessages = 100
const highPriorityCount = 50
const highPriority = uint8(10)
const defaultPriority = uint8(0)

func failOnError(err error, msg string) {
	if err != nil {
		slog.Error(msg, "error", err)
		panic(err)
	}
}

func main() {
	logger := slog.New(slog.NewTextHandler(
		os.Stdout,
		&slog.HandlerOptions{Level: slog.LevelInfo},
	))
	slog.SetDefault(logger)

	cfg, err := config.LoadConfig()
	failOnError(err, "Failed to load the env")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Producer.PublishTimeout)
	defer cancel()

	r, err := mq.NewRabbitMQ(ctx, cfg.RabbitMQ.URL, cfg.RabbitMQ.PrefetchCount)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer r.Close()

	// ensure exchanges/queues exist (idempotent)
	failOnError(r.DeclareExchange(cfg.RabbitMQ.MainExchange, "direct", cfg.RabbitMQ.Durable, false), "declare main exchange")
	qArgs := amqp.Table{
		"x-max-priority": int64(cfg.RabbitMQ.MaxPriority),
	}
	q, err := r.DeclareQueueWithArgs(cfg.RabbitMQ.Queue, cfg.RabbitMQ.Durable, false, qArgs)
	failOnError(err, "declare main queue")
	failOnError(r.BindQueue(q.Name, cfg.RabbitMQ.RoutingKey, cfg.RabbitMQ.MainExchange), "bind main queue")

	slog.Info("Starting to publish messages...", "total", totalMessages, "high_priority", highPriorityCount)

	for i := range totalMessages {
		priority := defaultPriority
		messageType := "Success Message from Producer"

		if i%2 == 0 {
			messageType = "Error Message from Producer"
			priority = highPriority
		}

		msg := types.Message{
			Timestamp: time.Now(),
			Data:      messageType,
		}
		body, err := json.Marshal(msg)
		failOnError(err, "failed to marshal message to JSON")

		pub := amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
			Priority:     priority,
		}

		err = r.Publish(ctx, cfg.RabbitMQ.MainExchange, cfg.RabbitMQ.RoutingKey, false, pub)
		failOnError(err, "Failed to publish message")

		slog.Info("Message sent",
			"priority", priority,
			"type", messageType)
	}
	slog.Info("Finished publishing", "messages", totalMessages)
}
