package types

import "time"

type Message struct {
	Timestamp time.Time `json:"timestamp"`
	Data      string    `json:"message"`
}
