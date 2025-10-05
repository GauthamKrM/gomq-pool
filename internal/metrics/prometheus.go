package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"gomq-pool/config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Global instance name/ID, unique to the running container/process
var instanceName string

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

// InitMetrics initializes and registers the Prometheus metrics.
func InitMetrics(cfg *config.Config) {
	if globalClient != nil {
		return
	}

	instanceName = cfg.Consumer.ConsumerName

	globalClient = &Client{
		msgCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_messages_total",
				Help: "Total messages processed by result per consumer instance/worker",
			},
			[]string{"app_id_hash", "consumer_id", "queue", "result"},
		),
		retryCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_retries_total",
				Help: "Total retries scheduled by consumer instance/worker",
			},
			[]string{"app_id_hash", "consumer_id", "queue"},
		),
		dlqCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "consumer_dlq_total",
				Help: "Total messages routed to DLQ by consumer instance/worker",
			},
			[]string{"app_id_hash", "consumer_id", "queue"},
		),
		procHist: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "consumer_processing_seconds",
				Help:    "Message processing time per consumer instance/worker",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"app_id_hash", "consumer_id", "queue"},
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
		slog.Error("GetClient called before InitMetrics!")
		panic("Metrics client not initialized. Call InitMetrics first.")
	}
	return globalClient
}

// exported helpers to record metrics
func (c *Client) IncProcessed(consumerID, queue, result string) {
	c.msgCounter.WithLabelValues(instanceName, consumerID, queue, result).Inc()
}
func (c *Client) IncRetry(consumerID, queue string) {
	c.retryCounter.WithLabelValues(instanceName, consumerID, queue).Inc()
}
func (c *Client) IncDLQ(consumerID, queue string) {
	c.dlqCounter.WithLabelValues(instanceName, consumerID, queue).Inc()
}
func (c *Client) ObserveLatency(consumerID, queue string, d time.Duration) {
	c.procHist.WithLabelValues(instanceName, consumerID, queue).Observe(d.Seconds())
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
