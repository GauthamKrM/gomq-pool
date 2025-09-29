package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gomq-pool/config"
	"gomq-pool/internal/consumer"
	"gomq-pool/internal/mq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Payload struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

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

	// exchanges (direct)
	failOnError(r.DeclareExchange(cfg.RabbitMQ.MainExchange, "direct", cfg.RabbitMQ.Durable, false), "declare main exchange")
	failOnError(r.DeclareExchange(cfg.RabbitMQ.RetryExchange, "direct", cfg.RabbitMQ.Durable, false), "declare retry exchange")
	failOnError(r.DeclareExchange(cfg.RabbitMQ.DLQExchange, "direct", cfg.RabbitMQ.Durable, false), "declare dlq exchange")

	// declare queue (configurable durable/autodelete)
	mainQ, err := r.DeclareQueue(cfg.RabbitMQ.Queue, cfg.RabbitMQ.Durable, false)
	failOnError(err, "declare main queue")
	failOnError(r.BindQueue(mainQ.Name, cfg.RabbitMQ.RoutingKey, cfg.RabbitMQ.MainExchange), "bind main queue")

	// retry queue: DLX -> main
	retryArgs := amqp.Table{
		"x-dead-letter-exchange":    cfg.RabbitMQ.MainExchange,
		"x-dead-letter-routing-key": cfg.RabbitMQ.RoutingKey,
	}
	retryQ, err := r.DeclareQueueWithArgs(cfg.RabbitMQ.RetryQueue, cfg.RabbitMQ.Durable, false, retryArgs)
	failOnError(err, "declare retry queue")
	failOnError(r.BindQueue(retryQ.Name, cfg.RabbitMQ.RetryQueue, cfg.RabbitMQ.RetryExchange), "bind retry queue")

	// DLQ queue
	dlqQ, err := r.DeclareQueue(cfg.RabbitMQ.DLQQueue, cfg.RabbitMQ.Durable, false)
	failOnError(err, "declare dlq queue")
	failOnError(r.BindQueue(dlqQ.Name, cfg.RabbitMQ.DLQQueue, cfg.RabbitMQ.DLQExchange), "bind dlq queue")

	// consume from main queue (manual ack)
	msgs, err := r.Consume(cfg.RabbitMQ.Queue, false)
	failOnError(err, "failed to start consuming")

	// business logic
	// TODO: use context in process
	process := func(_ context.Context, workerID int, body []byte) error {
		var msg Payload
		if err := json.Unmarshal(body, &msg); err != nil {
			slog.Error("failed to unmarshal message", "worker", workerID, "error", err)
			return err
		}
		slog.Info("process: received message",
			"worker", workerID,
			"body", string(body))
		// simulate work
		time.Sleep(100 * time.Millisecond)
		return nil // success -> Ack
	}

	// Handler with retry + DLQ
	handler := func(hctx context.Context, workerID int, d amqp.Delivery) error {
		// Extract retry count header (custom), default 0
		retries := 0
		if d.Headers != nil {
			if v, ok := d.Headers["x-retry-count"]; ok {
				switch t := v.(type) {
				case int32:
					retries = int(t)
				case int64:
					retries = int(t)
				case int:
					retries = t
				case string:
					if n, err := strconv.Atoi(t); err == nil {
						retries = n
					}
				}
			}
		}

		// Run business logic
		if err := process(hctx, workerID, d.Body); err != nil {
			// Decide retry vs DLQ
			if retries < cfg.Consumer.MaxRetries {
				// Exponential backoff: base * 2^retries
				delay := cfg.Consumer.RetryBaseDelay * (1 << retries)
				exp := strconv.FormatInt(delay.Milliseconds(), 10)

				// Republish to retry exchange with per-message TTL, increment retry header
				pub := amqp.Publishing{
					ContentType:  d.ContentType,
					DeliveryMode: amqp.Persistent,
					Headers:      amqp.Table{},
					Body:         d.Body,
					Expiration:   exp, // ms as string
				}
				// copy existing headers minimally
				maps.Copy(pub.Headers, d.Headers)
				pub.Headers["x-retry-count"] = retries + 1
				pub.Headers["x-error"] = fmt.Sprintf("%v", err)

				if perr := r.Publish(hctx, cfg.RabbitMQ.RetryExchange, cfg.RabbitMQ.RetryQueue, false, pub); perr != nil {
					slog.Error("retry publish failed", "worker", workerID, "error", perr)
					// As a fallback, Nack with requeue=true to avoid message loss
					_ = d.Nack(false, true)
					return perr
				}

				if aerr := d.Ack(false); aerr != nil {
					slog.Error("ack failed after scheduling retry", "worker", workerID, "error", aerr)
				} else {
					slog.Info("scheduled retry", "worker", workerID, "retries", retries+1, "after", delay.String())
				}
				return nil
			}

			// Max retries reached: send to DLQ
			pub := amqp.Publishing{
				ContentType:  d.ContentType,
				DeliveryMode: amqp.Persistent,
				Headers:      amqp.Table{},
				Body:         d.Body,
			}
			maps.Copy(pub.Headers, d.Headers)
			pub.Headers["x-retry-count"] = retries
			pub.Headers["x-final-error"] = fmt.Sprintf("%v", err)

			if perr := r.Publish(hctx, cfg.RabbitMQ.DLQExchange, cfg.RabbitMQ.DLQQueue, false, pub); perr != nil {
				slog.Error("dlq publish failed", "worker", workerID, "error", perr)
				// As a fallback, Nack without requeue to avoid hot loops
				_ = d.Nack(false, false)
				return perr
			}
			if aerr := d.Ack(false); aerr != nil {
				slog.Error("ack failed after DLQ", "worker", workerID, "error", aerr)
			} else {
				slog.Info("moved to DLQ", "worker", workerID, "retries", retries)
			}
			return nil
		}

		// Success: Ack
		if aerr := d.Ack(false); aerr != nil {
			slog.Error("ack failed", "worker", workerID, "error", aerr)
			return aerr
		}
		return nil
	}

	// start worker pool
	pool := consumer.NewPool(ctx, cfg.Consumer.PoolSize, msgs, handler, cfg.Consumer.WorkerTimeout)

	// wait until shutdown signal
	<-ctx.Done()
	slog.Info("main: signal received, stopping pool...")

	// stop workers gracefully
	pool.Stop()
	slog.Info("main: shutdown complete")
}
