package main

import (
	"context"
	"log"

	"gomq-pool/config"
	"gomq-pool/internal/mq"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
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

	log.Printf(" [x] Sent %s\n", body)
}
