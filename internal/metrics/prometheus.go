package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Collector defines the interface for all metric recording functions.
// This allows the RabbitMQHandler to depend on an abstraction, not the concrete Prometheus implementation.
type Collector interface {
	IncProcessed(consumerID, queue, result string)
	IncRetry(consumerID, queue string)
	IncDLQ(consumerID, queue string)
	ObserveLatency(consumerID, queue string, d time.Duration)
	IncAckError(kind, queue string)
}

// Client implements the Collector interface using Prometheus metrics.
type Client struct {
	msgCounter    *prometheus.CounterVec
	retryCounter  *prometheus.CounterVec
	dlqCounter    *prometheus.CounterVec
	procHist      *prometheus.HistogramVec
	ackErrCounter *prometheus.CounterVec
}

// Global instance of the metrics client.
var globalClient *Client

func InitMetrics() {
	if globalClient != nil {
		return
	}

	globalClient = &Client{
		msgCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_messages_total",
				Help: "Total messages processed by result per consumer",
			},
			[]string{"consumer_id", "queue", "result"},
		),
		retryCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_retries_total",
				Help: "Total retries scheduled by consumer",
			},
			[]string{"consumer_id", "queue"},
		),
		dlqCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_dlq_total",
				Help: "Total messages routed to DLQ by consumer",
			},
			[]string{"consumer_id", "queue"},
		),
		procHist: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "consumer_processing_seconds",
				Help:    "Message processing time per consumer",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"consumer_id", "queue"},
		),
		ackErrCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_ack_errors_total",
				Help: "Total Ack/Nack errors by type and queue",
			},
			[]string{"type", "queue"}, // type: ack|nack
		),
	}
}

func GetClient() Collector {
	if globalClient == nil {
		InitMetrics()
	}
	return globalClient
}

// exported helpers to record metrics
func (c *Client) IncProcessed(consumerID, queue, result string) {
	c.msgCounter.WithLabelValues(consumerID, queue, result).Inc()
}
func (c *Client) IncRetry(consumerID, queue string) {
	c.retryCounter.WithLabelValues(consumerID, queue).Inc()
}
func (c *Client) IncDLQ(consumerID, queue string) {
	c.dlqCounter.WithLabelValues(consumerID, queue).Inc()
}
func (c *Client) ObserveLatency(consumerID, queue string, d time.Duration) {
	c.procHist.WithLabelValues(consumerID, queue).Observe(d.Seconds())
}
func (c *Client) IncAckError(kind, queue string) {
	c.ackErrCounter.WithLabelValues(kind, queue).Inc()
}

type Server struct {
	srv *http.Server
}

func StartServer(ctx context.Context, bind string, metricsPath, livenessPath, readinessPath string) *Server {
	mux := http.NewServeMux()
	// prometheus endpoint
	mux.Handle(metricsPath, promhttp.Handler())
	// health
	mux.HandleFunc(livenessPath, LivenessHandler)
	mux.HandleFunc(readinessPath, ReadinessHandler)

	srv := &http.Server{
		Addr:              bind,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("metrics server starting", "addr", bind, "metricsPath", metricsPath)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "err", err)
		}
	}()

	go func() {
		<-ctx.Done()
		// graceful shutdown
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	return &Server{srv: srv}
}

func ConsumerIDLabel(workerID int) string { return strconv.Itoa(workerID) }
