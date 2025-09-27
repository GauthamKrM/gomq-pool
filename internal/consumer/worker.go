package consumer

import (
	"context"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// HandleFunc processes a single message
// return nil -> success (Ack)
// return non-nil -> failure (Nack without requeue for now)
type HandleFunc func(ctx context.Context, d amqp.Delivery) error

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
		log.Printf("worker-%d: started", w.id)
		for {
			select {
			case <-ctx.Done():
				log.Printf("worker-%d: context done, exiting", w.id)
				return
			case d, ok := <-w.msgs:
				if !ok {
					log.Printf("worker-%d: messages channel closed, exiting", w.id)
					return
				}

				msgCtx, cancel := context.WithTimeout(ctx, w.timeout)
				err := w.handler(msgCtx, d)
				cancel()

				if err != nil {
					// TODO: Phase 3 will replace this with retry logic.
					log.Printf("worker-%d: handler error: %v", w.id, err)
					if nackErr := d.Nack(false, false); nackErr != nil {
						log.Printf("worker-%d: nack failed: %v", w.id, nackErr)
					}
					continue
				}
				if ackErr := d.Ack(false); ackErr != nil {
					// ack failed - log and continue
					log.Printf("worker-%d: ack failed: %v", w.id, ackErr)
				}
			}
		}
	}()
}
