package mq

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQConsumer handles consuming messages and setting up related topology.
type RabbitMQConsumer struct {
	ch *amqp.Channel
}

// NewConsumer creates a new Consumer and its dedicated channel with QoS prefetch settings.
func NewConsumer(conn *amqp.Connection, prefetch int) (*RabbitMQConsumer, error) {
	if conn == nil || conn.IsClosed() {
		return nil, errors.New("connection is not open")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel for consumer: %w", err)
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &RabbitMQConsumer{ch: ch}, nil
}

// Consume starts consuming messages from a queue.
func (c *RabbitMQConsumer) Consume(queue string, autoAck bool) (<-chan amqp.Delivery, error) {
	if c == nil || c.ch == nil {
		return nil, errors.New("consumer channel not initialized")
	}
	return c.ch.Consume(queue, "", autoAck, false, false, false, nil)
}

// DeclareExchange ensures an exchange exists.
func (c *RabbitMQConsumer) DeclareExchange(name, kind string, durable, autoDelete bool) error {
	if c == nil || c.ch == nil {
		return errors.New("consumer channel not initialized")
	}
	return c.ch.ExchangeDeclare(name, kind, durable, autoDelete, false, false, nil)
}

// DeclareQueue ensures a queue exists.
func (c *RabbitMQConsumer) DeclareQueue(name string, durable, autoDelete bool) (amqp.Queue, error) {
	if c == nil || c.ch == nil {
		return amqp.Queue{}, errors.New("consumer channel not initialized")
	}
	return c.ch.QueueDeclare(name, durable, autoDelete, false, false, nil)
}

// DeclareQueueWithArgs ensures a queue with specific argument exists.
func (c *RabbitMQConsumer) DeclareQueueWithArgs(name string, durable, autoDelete bool, args amqp.Table) (amqp.Queue, error) {
	if c == nil || c.ch == nil {
		return amqp.Queue{}, errors.New("consumer channel not initialized")
	}
	return c.ch.QueueDeclare(name, durable, autoDelete, false, false, args)
}

// BindQueue binds a queue to an exchange.
func (c *RabbitMQConsumer) BindQueue(queue, key, exchange string) error {
	if c == nil || c.ch == nil {
		return errors.New("consumer channel not initialized")
	}
	return c.ch.QueueBind(queue, key, exchange, false, nil)
}

// NofityClose registers a listener for channel close events.
func (c *RabbitMQConsumer) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	if c == nil || c.ch == nil {
		return nil
	}
	return c.ch.NotifyClose(ch)
}

// Close closes the consumer's channel.
func (c *RabbitMQConsumer) Close() error {
	if c == nil || c.ch == nil {
		return nil
	}
	return c.ch.Close()
}
