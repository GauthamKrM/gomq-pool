package mq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher is an interface for publishing messages.
// It's used by the handler to abstract away the concrete producer implementation,
// enabling retries and DLQ functionality without depending on the main producer logic.
type Publisher interface {
	Publish(ctx context.Context, exchange, routingKey string, mandatory bool, pub amqp.Publishing) error
}
