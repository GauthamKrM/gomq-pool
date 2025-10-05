package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"gomq-pool/config"
	"gomq-pool/internal/consumer"
	"gomq-pool/internal/metrics"
	"gomq-pool/internal/mq"

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

	// load config
	cfg, err := config.LoadConfig()
	failOnError(err, "Failed to load the env")

	// top-level context cancels on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics.InitMetrics(cfg)
	metricsClient := metrics.GetClient()

	if cfg.Consumer.MetricsEnabled {
		metrics.StartServer(ctx, cfg.Consumer.MetricsBind, cfg.Consumer.MetricsPath, cfg.Consumer.LivePath, cfg.Consumer.ReadyPath)
	}

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

	metrics.SetReady(true)

	// watch for AMQP close to switch readiness off
	go func() {
		chClose := make(chan *amqp.Error, 1)
		connClose := make(chan *amqp.Error, 1)
		_ = r.ChannelNotifyClose(chClose)
		_ = r.ConnectionNotifyClose(connClose)
		select {
		case <-ctx.Done():
			metrics.SetReady(false)
			return
		case <-chClose:
			metrics.SetReady(false)
		case <-connClose:
			metrics.SetReady(false)
		}
	}()

	// consume from main queue (manual ack)
	msgs, err := r.Consume(cfg.RabbitMQ.Queue, false)
	failOnError(err, "failed to start consuming")

	// business logic
	processor := consumer.NewDefaultProcessor()

	// Handler with retry + DLQ
	handler := consumer.NewRabbitMQHandler(cfg, processor, r, metricsClient)

	poolHandler := handler.Handle

	// start worker pool
	pool := consumer.NewPool(ctx, cfg.Consumer.PoolSize, msgs, poolHandler, cfg.Consumer.WorkerTimeout)

	// wait until shutdown signal
	<-ctx.Done()
	slog.Info("main: signal received, stopping pool...")

	// stop workers gracefully
	pool.Stop()
	slog.Info("main: shutdown complete")
}
