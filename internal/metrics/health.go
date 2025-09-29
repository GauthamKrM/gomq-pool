package metrics

import (
	"net/http"
	"sync/atomic"
)

var (
	live  atomic.Bool
	ready atomic.Bool
)

func init() {
	live.Store(true)   // process is alive once started
	ready.Store(false) // set true after dependencies (RabbitMQ)
}

func SetReady(v bool) { ready.Store(v) }

func LivenessHandler(w http.ResponseWriter, r *http.Request) {
	if live.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}
	http.Error(w, "unhealthy", http.StatusServiceUnavailable)
}

func ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	if ready.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
		return
	}
	http.Error(w, "not ready", http.StatusServiceUnavailable)
}
