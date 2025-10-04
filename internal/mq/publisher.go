package mq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher interface {
	Publish(ctx context.Context, exchange, routingKey string, mandatory bool, pub amqp.Publishing) error
}
