package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Pool coordinates workers
type Pool struct {
	workers []*Worker
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
}

func NewPool(parentCtx context.Context, poolSize int, msgs <-chan amqp.Delivery, handler HandleFunc, workerTimeout time.Duration) *Pool {
	ctx, cancel := context.WithCancel(parentCtx)
	p := &Pool{
		workers: make([]*Worker, 0, poolSize),
		ctx:     ctx,
		cancel:  cancel,
	}

	for i := range poolSize {
		p.wg.Add(1)
		w := NewWorker(i+1, msgs, handler, workerTimeout, &p.wg)
		p.workers = append(p.workers, w)
		w.Start(ctx)
	}
	p.started = true
	slog.Info("consumer pool started", "workers", poolSize)
	return p
}

func (p *Pool) Stop() {
	if !p.started {
		return
	}
	p.cancel()
	p.wg.Wait()
	p.started = false
	slog.Info("consumer pool: stopped")
}
