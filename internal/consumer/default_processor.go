package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"gomq-pool/internal/types"
)

// DefaultProcessor implements the Processor interface
type DefaultProcessor struct{}

// NewDefaultProcessor creates a new instance of the default message processor.
func NewDefaultProcessor() *DefaultProcessor {
	return &DefaultProcessor{}
}

func (p *DefaultProcessor) Process(ctx context.Context, workerID int, body []byte) error {
	var msg types.Message
	if err := json.Unmarshal(body, &msg); err != nil {
		slog.Error("failed to unmarshal message", "worker", workerID, "error", err)
		return err
	}
	slog.Info("process: received message",
		"worker", workerID,
		"body", msg.Data)
	// simulate work
	time.Sleep(100 * time.Millisecond)
	return nil // success -> Ack
}
