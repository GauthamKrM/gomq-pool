package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connector manages the lifecycle of a RabbitMQ connection.
type Connector struct {
	conn *amqp.Connection
}

// NewConnector creates a new RabbitMQ connector.
func NewConnector() *Connector {
	return &Connector{}
}

// Connect establishes a RabbitMQ connection with retries and backoff
func (c *Connector) Connect(ctx context.Context, url string) error {
	const attempts = 5
	backoff := 200 * time.Millisecond

	var err error
	for i := range attempts {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		slog.Info("connector: attempting to connect to RabbitMQ", "attempt", i+1)
		c.conn, err = amqp.Dial(url)
		if err == nil {
			slog.Info("connector: successfully connected to RabbitMQ")
			return nil
		}

		slog.Warn("connector: failed to dial RabbitMQ", "error", err, "attempt", i+1)
		if i == attempts-1 {
			break
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("failed to connect to RabbitMQ after %d attempts: %w", attempts, err)
}

// Close gracefully shuts down the connection.
func (c *Connector) Close() error {
	if c == nil || c.conn == nil || c.conn.IsClosed() {
		return nil
	}
	slog.Info("connector: closing RabbitMQ connection")
	return c.conn.Close()
}

// NotifyClose registers a listener for connection close events.
func (c *Connector) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.NotifyClose(ch)
}

// Connection returns the underlying amqp.Connection.
// It's exposed for creating producers and consumers which need their own channels.
func (c *Connector) Connection() (*amqp.Connection, error) {
	if c.conn == nil || c.conn.IsClosed() {
		return nil, errors.New("connection is not open")
	}
	return c.conn, nil
}
