package mq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQProducer handles publishing messages and setting up related topology.
type RabbitMQProducer struct {
	ch *amqp.Channel
}

// NewProducer creates a new producer and its dedicated channel.
func NewProducer(conn *amqp.Connection) (*RabbitMQProducer, error) {
	if conn == nil || conn.IsClosed() {
		return nil, errors.New("connection is not open")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel for producer: %w", err)
	}

	return &RabbitMQProducer{ch: ch}, nil
}

// Publish publishes a message to an exchange. Implements the Publisher interface.
func (p *RabbitMQProducer) Publish(ctx context.Context, exchange, routingKey string, mandatory bool, pub amqp.Publishing) error {
	if p == nil || p.ch == nil {
		return errors.New("producer channel not initialized")
	}
	return p.ch.PublishWithContext(ctx, exchange, routingKey, mandatory, false, pub)
}

// DeclareExchange ensures an exchange exists.
func (p *RabbitMQProducer) DeclareExchange(name, kind string, durable, autoDelete bool) error {
	if p == nil || p.ch == nil {
		return errors.New("producer channel not initialized")
	}
	return p.ch.ExchangeDeclare(name, kind, durable, autoDelete, false, false, nil)
}

// DeclareQueueWithArgs ensures a queue with specific arguments exists.
func (p *RabbitMQProducer) DeclareQueueWithArgs(name string, durable, autoDelete bool, args amqp.Table) (amqp.Queue, error) {
	if p == nil || p.ch == nil {
		return amqp.Queue{}, errors.New("producer channel not initialized")
	}
	return p.ch.QueueDeclare(name, durable, autoDelete, false, false, args)
}

// BindQueue binds a queue to an exchange.
func (p *RabbitMQProducer) BindQueue(queue, key, exchange string) error {
	if p == nil || p.ch == nil {
		return errors.New("producer channel not initialized")
	}
	return p.ch.QueueBind(queue, key, exchange, false, nil)
}

// Close closes the producer's channel.
func (p *RabbitMQProducer) Close() error {
	if p == nil || p.ch == nil {
		return nil
	}
	return p.ch.Close()
}
