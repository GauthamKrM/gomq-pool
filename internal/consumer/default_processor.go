package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
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
	// failure simulation
	if strings.Contains(msg.Data, "Error") {
		slog.Warn("process: simulating business logic failure", "worker", workerID, "data", msg.Data)
		return errors.New("simulated business error: unable to process message data")
	}
	return nil // success -> Ack
}
