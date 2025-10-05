package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"strconv"
	"time"

	"gomq-pool/config"
	"gomq-pool/internal/metrics"
	"gomq-pool/internal/mq"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageHandler defines the contract for processing an AMQP delivery.
// This is the function signature required by the Worker struct.
type MessageHandler interface {
	Handle(ctx context.Context, workerID int, d amqp.Delivery) error
}

// RabbitMQHandler contains the business logic for processing, retries, and DLQ.
type RabbitMQHandler struct {
	cfg       *config.Config
	processor Processor
	publisher mq.Publisher
	metrics   metrics.Collector
}

// NewRabbitMQHandler initializes the handler with its dependencies.
func NewRabbitMQHandler(
	cfg *config.Config,
	processor Processor,
	publisher mq.Publisher,
	collector metrics.Collector,
) *RabbitMQHandler {
	return &RabbitMQHandler{
		cfg:       cfg,
		processor: processor,
		publisher: publisher,
		metrics:   collector,
	}
}

// Handle implements the core message processing, retry, and DLQ logic.
func (h *RabbitMQHandler) Handle(hctx context.Context, workerID int, d amqp.Delivery) error {
	start := time.Now()
	cid := metrics.ConsumerIDLabel(workerID)
	queue := h.cfg.RabbitMQ.Queue
	priority := strconv.Itoa(int(d.Priority))

	// Use defer to observe latency once, covering all exit paths (success, retry, DLQ).
	defer func() {
		h.metrics.ObserveLatency(cid, queue, time.Since(start), priority)
	}()

	// 1. Extract retry count
	reties := h.getRetryCount(d.Headers)

	// 2. Run business logic
	if err := h.processor.Process(hctx, workerID, d.Body); err != nil {
		// Business logic failed

		// 3. Handle failure (Retry or DLQ)
		var handlerError error

		if reties < h.cfg.Consumer.MaxRetries {
			// Retry logic: returns nil on success, error if fatal failure (e.g., can't publish retry message)
			handlerError = h.handleRetry(hctx, workerID, d, reties, err, cid, queue)

		} else {
			// DLQ logic: returns nil on success, error if fatal failure (e.g., can't publish to DLQ)
			handlerError = h.handleDLQ(hctx, workerID, d, reties, err, cid, queue)
		}

		// 4. Return error from handler
		if handlerError != nil {
			// The transaction failed fatally (e.g., failed to re-publish OR failed to Ack/Nack)
			// Return the fatal error to the worker.
			return fmt.Errorf("fatal failure during message handling: %w", handlerError)
		}

		// Successfully scheduled for retry or DLQ. Return a non-nil error
		// indicating the original process failed, but the transaction was handled.
		return fmt.Errorf("message failed and was handled: %w", err)
	}

	// 5. Success Path: Ack
	if aerr := d.Ack(false); aerr != nil {
		slog.Error("ack failed", "worker", workerID, "error", aerr)
		h.metrics.IncAckError("ack", queue)
		return aerr
	}
	h.metrics.IncProcessed(cid, queue, "success", priority)
	return nil
}

func (h *RabbitMQHandler) getRetryCount(headers amqp.Table) int {
	retries := 0
	if headers != nil {
		if v, ok := headers["x-retry-count"]; ok {
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
	return retries
}

func (h *RabbitMQHandler) handleRetry(hctx context.Context, workerID int, d amqp.Delivery, retries int, processErr error, cid string, queue string) error {
	delay := h.cfg.Consumer.RetryBaseDelay * (1 << retries)
	exp := strconv.FormatInt(delay.Milliseconds(), 10)

	pub := amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		Headers:      amqp.Table{},
		Body:         d.Body,
		Expiration:   exp, // ms as string
	}
	maps.Copy(pub.Headers, d.Headers)
	pub.Headers["x-retry-count"] = retries + 1
	pub.Headers["x-error"] = fmt.Sprintf("%v", processErr)

	if perr := h.publisher.Publish(hctx, h.cfg.RabbitMQ.RetryExchange, h.cfg.RabbitMQ.RetryQueue, false, pub); perr != nil {
		slog.Error("retry publish failed, Nacking message back to queue", "worker", workerID, "error", perr)
		// Fallback: Nack with requeue=true
		if err := d.Nack(false, true); err != nil {
			slog.Error("nack failed (retry fallback)", "worker", workerID, "error", err)
			h.metrics.IncAckError("nack", queue)
			return fmt.Errorf("failed to nack after publish failure: %w", err) // Fatal
		}
		return fmt.Errorf("publish to retry exchange failed: %w", perr) // Fatal: original message requeued
	}

	// Successfully published for retry: Ack original message
	if aerr := d.Ack(false); aerr != nil {
		slog.Error("ack failed after scheduling retry", "worker", workerID, "error", aerr)
		h.metrics.IncAckError("ack", queue)
		return fmt.Errorf("failed to ack after successful retry publish: %w", aerr) // Fatal
	}
	slog.Info("scheduled retry", "worker", workerID, "retries", retries+1, "after", delay.String())
	h.metrics.IncRetry(cid, queue)
	h.metrics.IncProcessed(cid, queue, "retry", strconv.Itoa(int(d.Priority)))
	return nil
}

func (h *RabbitMQHandler) handleDLQ(hctx context.Context, workerID int, d amqp.Delivery, retries int, processErr error, cid string, queue string) error {
	pub := amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		Headers:      amqp.Table{},
		Body:         d.Body,
	}
	maps.Copy(pub.Headers, d.Headers)
	pub.Headers["x-retry-count"] = retries
	pub.Headers["x-final-error"] = fmt.Sprintf("%v", processErr)

	if perr := h.publisher.Publish(hctx, h.cfg.RabbitMQ.DLQExchange, h.cfg.RabbitMQ.DLQQueue, false, pub); perr != nil {
		slog.Error("dlq publish failed, Nacking without requeue", "worker", workerID, "error", perr)
		// Fallback: Nack without requeue to avoid hot loops
		if err := d.Nack(false, false); err != nil {
			slog.Error("nack failed (dlq fallback)", "worker", workerID, "error", err)
			h.metrics.IncAckError("nack", queue)
			return fmt.Errorf("failed to nack after DLQ publish failure: %w", err) // Fatal
		}
		return fmt.Errorf("publish to DLQ exchange failed: %w", perr) // Fatal: original message discarded
	}

	// Successfully published to DLQ: Ack original message
	if aerr := d.Ack(false); aerr != nil {
		slog.Error("ack failed after DLQ", "worker", workerID, "error", aerr)
		h.metrics.IncAckError("ack", queue)
		return fmt.Errorf("failed to ack after successful DLQ publish: %w", aerr) // Fatal
	}
	slog.Info("moved to DLQ", "worker", workerID, "retries", retries)
	h.metrics.IncDLQ(cid, queue)
	h.metrics.IncProcessed(cid, queue, "failed", strconv.Itoa(int(d.Priority)))
	return nil
}
