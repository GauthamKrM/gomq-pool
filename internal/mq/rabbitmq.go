package mq

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ is a light wrapper around an amqp connection + channel.
type RabbitMQ struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

// GetMQ establishes a RabbitMQ connection with retries and backoff.
func NewRabbitMQ(ctx context.Context, url string, prefetch int) (*RabbitMQ, error) {
	const attempts = 5
	backoff := 200 * time.Millisecond

	var conn *amqp.Connection
	var ch *amqp.Channel
	var err error

	for i := range attempts {
		// Respect context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		if i == attempts-1 {
			return nil, fmt.Errorf("failed to dial RabbitMQ after %d attempts: %w", attempts, err)
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if conn == nil {
		return nil, errors.New("connection unexpectedly nil")
	}

	ch, err = conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &RabbitMQ{conn: conn, ch: ch}, nil
}

// DeclareExchange creates or connects to an exchange.
func (r *RabbitMQ) DeclareExchange(name, kind string, durable, autoDelete bool) error {
	if r == nil || r.ch == nil {
		return errors.New("channel not initialized")
	}
	return r.ch.ExchangeDeclare(name, kind, durable, autoDelete, false, false, nil)
}

// DeclareQueue creates or connects to a queue with configurable options.
func (r *RabbitMQ) DeclareQueue(name string, durable, autoDelete bool) (amqp.Queue, error) {
	if r == nil || r.ch == nil {
		return amqp.Queue{}, errors.New("channel not initialized")
	}
	return r.ch.QueueDeclare(
		name,
		durable,
		autoDelete,
		false, // exclusive
		false, // no-wait
		nil,
	)
}

// DeclareQueueWithArgs creates or connects to a queue with arguments (e.g., DLX).
func (r *RabbitMQ) DeclareQueueWithArgs(name string, durable, autoDelete bool, args amqp.Table) (amqp.Queue, error) {
	if r == nil || r.ch == nil {
		return amqp.Queue{}, errors.New("channel not initialized")
	}
	return r.ch.QueueDeclare(
		name,
		durable,
		autoDelete,
		false, // exclusive
		false, // no-wait
		args,
	)
}

// BindQueue binds a queue to an exchange with a routing key.
func (r *RabbitMQ) BindQueue(queue, key, exchange string) error {
	if r == nil || r.ch == nil {
		return errors.New("channel not initialized")
	}
	return r.ch.QueueBind(queue, key, exchange, false, nil)
}

// Consume sets up a consumer on the given queue.
func (r *RabbitMQ) Consume(queue string, autoAck bool) (<-chan amqp.Delivery, error) {
	if r == nil || r.ch == nil {
		return nil, errors.New("channel not initialized")
	}
	return r.ch.Consume(
		queue,
		"",      // consumer
		autoAck, // autoAck - false to manually Ack/Nack
		false,   // exclusive
		false,   // noLocal
		false,   // noWait
		nil,     // args
	)
}

// Close gracefully shuts down the channel and connection.
func (r *RabbitMQ) Close() error {
	if r == nil {
		return nil
	}

	var errs []error
	if r.ch != nil {
		if err := r.ch.Close(); err != nil {
			errs = append(errs, fmt.Errorf("channel close: %w", err))
		}
	}
	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("connection close: %w", err))
		}
	}
	return errors.Join(errs...)
}

// PublishWithContext publishes to a queue via the default exchange (backward compatible).
func (r *RabbitMQ) PublishWithContext(ctx context.Context, queue string, body []byte) error {
	if r == nil || r.ch == nil {
		return errors.New("channel not initialized")
	}

	return r.ch.PublishWithContext(ctx,
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		})
}

// Publish publishes to an exchange with full control over properties.
func (r *RabbitMQ) Publish(ctx context.Context, exchange, routingKey string, mandatory bool, pub amqp.Publishing) error {
	if r == nil || r.ch == nil {
		return errors.New("channel not initialized")
	}
	return r.ch.PublishWithContext(ctx, exchange, routingKey, mandatory, false, pub)
}
