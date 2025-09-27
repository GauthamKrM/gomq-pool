package main

import (
	"context"
	"log/slog"
	"os"

	"gomq-pool/config"
	"gomq-pool/internal/mq"
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

	_, err = r.DeclareQueue(cfg.RabbitMQ.Queue, false, false)
	failOnError(err, "Failed to declare queue")

	body := []byte("Hello from Producer!")
	err = r.PublishWithContext(ctx, cfg.RabbitMQ.Queue, body)
	failOnError(err, "Failed to publish message")

	slog.Info(
		"Message sent",
		"queue", cfg.RabbitMQ.Queue,
		"body", string(body),
	)
}
