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

	// 1. Setup connection
	connector := mq.NewConnector()
	err = connector.Connect(ctx, cfg.RabbitMQ.URL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connector.Close()

	conn, err := connector.Connection()
	failOnError(err, "Failed to get connection from connector")

	// 2. Setup consumer
	mqConsumer, err := mq.NewConsumer(conn, cfg.RabbitMQ.PrefetchCount)
	failOnError(err, "Failed to create consumer")
	defer mqConsumer.Close()

	// 3. Setup a separate producer for the handler to publish retries and DLQ messages
	handlerPublisher, err := mq.NewProducer(conn)
	failOnError(err, "Failed to create publisher for handler")
	defer handlerPublisher.Close()

	// 4. Declare topology using the consumer's channel
	failOnError(mqConsumer.DeclareExchange(cfg.RabbitMQ.MainExchange, "direct", cfg.RabbitMQ.Durable, false), "declare main exchange")
	failOnError(mqConsumer.DeclareExchange(cfg.RabbitMQ.RetryExchange, "direct", cfg.RabbitMQ.Durable, false), "declare retry exchange")
	failOnError(mqConsumer.DeclareExchange(cfg.RabbitMQ.DLQExchange, "direct", cfg.RabbitMQ.Durable, false), "declare dlq exchange")

	// declare queue (configurable durable/autodelete)
	mainQArgs := amqp.Table{
		"x-max-priority": int64(cfg.RabbitMQ.MaxPriority),
	}
	mainQ, err := mqConsumer.DeclareQueueWithArgs(cfg.RabbitMQ.Queue, cfg.RabbitMQ.Durable, false, mainQArgs)
	failOnError(err, "declare main queue")
	failOnError(mqConsumer.BindQueue(mainQ.Name, cfg.RabbitMQ.RoutingKey, cfg.RabbitMQ.MainExchange), "bind main queue")

	// retry queue: DLX -> main
	retryArgs := amqp.Table{
		"x-dead-letter-exchange":    cfg.RabbitMQ.MainExchange,
		"x-dead-letter-routing-key": cfg.RabbitMQ.RoutingKey,
	}
	retryQ, err := mqConsumer.DeclareQueueWithArgs(cfg.RabbitMQ.RetryQueue, cfg.RabbitMQ.Durable, false, retryArgs)
	failOnError(err, "declare retry queue")
	failOnError(mqConsumer.BindQueue(retryQ.Name, cfg.RabbitMQ.RetryQueue, cfg.RabbitMQ.RetryExchange), "bind retry queue")

	// DLQ queue
	dlqQ, err := mqConsumer.DeclareQueue(cfg.RabbitMQ.DLQQueue, cfg.RabbitMQ.Durable, false)
	failOnError(err, "declare dlq queue")
	failOnError(mqConsumer.BindQueue(dlqQ.Name, cfg.RabbitMQ.DLQQueue, cfg.RabbitMQ.DLQExchange), "bind dlq queue")

	metrics.SetReady(true)

	// watch for AMQP close to switch readiness off
	go func() {
		chClose := make(chan *amqp.Error, 1)
		connClose := make(chan *amqp.Error, 1)
		_ = mqConsumer.NotifyClose(chClose)
		_ = connector.NotifyClose(connClose)
		select {
		case <-ctx.Done():
			slog.Info("readiness watcher: context done")
		case <-chClose:
			slog.Warn("readiness watcher: consumer channel closed", "error", err)
		case <-connClose:
			slog.Warn("readiness watcher: connection closed", "error", err)
		}
		slog.Info("readiness watcher: setting readiness to false")
		metrics.SetReady(false)
	}()

	// 5. Start consuming from the main queue
	msgs, err := mqConsumer.Consume(cfg.RabbitMQ.Queue, false)
	failOnError(err, "failed to start consuming")

	// 6. Setup handler and worker pool
	processor := consumer.NewDefaultProcessor()
	// Handler with retry + DLQ
	handler := consumer.NewRabbitMQHandler(cfg, processor, handlerPublisher, metricsClient)
	// start worker pool
	pool := consumer.NewPool(ctx, cfg.Consumer.PoolSize, msgs, handler.Handle, cfg.Consumer.WorkerTimeout)

	// wait until shutdown signal
	<-ctx.Done()
	slog.Info("main: signal received, stopping pool...")

	// stop workers gracefully
	pool.Stop()
	slog.Info("main: shutdown complete")
}
