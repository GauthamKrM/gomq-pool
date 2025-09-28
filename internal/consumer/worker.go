package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// HandleFunc processes a single message
// return nil -> success (Ack)
// return non-nil -> failure (Nack without requeue for now)
type HandleFunc func(ctx context.Context, workerID int, d amqp.Delivery) error

type Worker struct {
	id      int
	msgs    <-chan amqp.Delivery
	handler HandleFunc
	wg      *sync.WaitGroup
	timeout time.Duration
}

func NewWorker(id int, msgs <-chan amqp.Delivery, handler HandleFunc, timeout time.Duration, wg *sync.WaitGroup) *Worker {
	return &Worker{
		id:      id,
		msgs:    msgs,
		handler: handler,
		wg:      wg,
		timeout: timeout}
}

// Start begins worker loop. Caller must have called wg.Add(1).
func (w *Worker) Start(ctx context.Context) {
	go func() {
		defer w.wg.Done()
		slog.Info("worker started", "workerID", w.id)
		for {
			select {
			case <-ctx.Done():
				slog.Info("worker exiting (context done)", "workerID", w.id)
				return
			case d, ok := <-w.msgs:
				if !ok {
					slog.Info("worker exiting (messages channel closed)", "workerID", w.id)
					return
				}

				msgCtx, cancel := context.WithTimeout(ctx, w.timeout)
				_ = w.handler(msgCtx, w.id, d) // handler must Ack/Nack
				cancel()
			}
		}
	}()
}
