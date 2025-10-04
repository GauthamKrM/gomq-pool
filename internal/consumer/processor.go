package consumer

import "context"

type Processor interface {
	Process(ctx context.Context, workerID int, body []byte) error
}
