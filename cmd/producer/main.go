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
	q, err := r.DeclareQueue(cfg.RabbitMQ.Queue, cfg.RabbitMQ.Durable, false)
	failOnError(err, "declare main queue")
	failOnError(r.BindQueue(q.Name, cfg.RabbitMQ.RoutingKey, cfg.RabbitMQ.MainExchange), "bind main queue")

	msg := types.Message{
		Timestamp: time.Now(),
		Data:      "Hello from Producer!",
	}
	body, err := json.Marshal(msg)
	failOnError(err, "failed to marshal message to JSON")
	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}
	err = r.Publish(ctx, cfg.RabbitMQ.MainExchange, cfg.RabbitMQ.RoutingKey, false, pub)
	failOnError(err, "Failed to publish message")

	slog.Info("Message sent", "exchange", cfg.RabbitMQ.MainExchange, "routingKey", cfg.RabbitMQ.RoutingKey, "body", string(body))
}
