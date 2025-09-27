package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gomq-pool/config"
	"gomq-pool/internal/consumer"
	"gomq-pool/internal/mq"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	// load config
	cfg, err := config.LoadConfig()
	failOnError(err, "Failed to load the env")

	// top-level context cancels on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// connect to RabbitMQ with retry + backoff
	r, err := mq.NewRabbitMQ(ctx, cfg.RabbitMQ.URL, cfg.RabbitMQ.PrefetchCount)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer r.Close()

	// declare queue (configurable durable/autodelete)
	_, err = r.DeclareQueue(cfg.RabbitMQ.Queue, false, false)
	failOnError(err, "Failed to declare a queue")

	msgs, err := r.Consume(cfg.RabbitMQ.Queue, false)
	failOnError(err, "failed to start consuming")

	handler := func(ctx context.Context, d amqp.Delivery) error {
		log.Printf("handler: received message: %s", string(d.Body))
		// simulate work
		time.Sleep(100 * time.Millisecond)
		return nil // success -> Ack
	}

	// start worker pool
	pool := consumer.NewPool(ctx, cfg.Consumer.PoolSize, msgs, handler, cfg.Consumer.WorkerTimeout)

	// wait until shutdown signal
	<-ctx.Done()
	log.Println("main: signal received, stopping pool...")

	// stop workers gracefully
	pool.Stop()
	log.Println("main: shutdown complete")
}
