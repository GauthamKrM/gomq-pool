# Go Message Queue Consumer Pool (gomq-pool)

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8.svg)](https://go.dev/dl/) [![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

An advanced and scalable **Message Queue Consumer Pool** implemented in Go.
This project demonstrates a robust system where producer push messages to a **RabbitMQ** broker, and a pool of concurrent Go consumers process them efficiently.
The system is designed for **high throughput**, **fault tolerance**, and **observability**, with built-in retry workflows, dead-letter queues, message prioritization, and Prometheus-Grafana-based monitoring.

---

## About

This project was developed as part of my journey to learn and apply **Go (Golang)** for building concurrent and scalable systems.  
It focuses on the practical implementation of **RabbitMQ** message queues, **worker pools**, and **metrics-based observability** using **Prometheus** and **Grafana**.

---

## High-Level Architecture

The system design follows a producer–consumer model with message reliability and observability at its core.

```text
+------------+        +--------------------+        +------------------+
| Producer(s)| -----> |   Message Broker   | -----> |  Consumer Pool   |
|    (Go)    |        | (RabbitMQ)         |        | (Go Goroutines)  |
+------------+        +--------------------+        +------------------+
                           |                     \
                           |                      \
                           v                       \
                   +----------------+            +------------------+
                   | Dead Letter    |            | Metrics Export   |
                   | Queue (DLQ)    |            |   (Prometheus)   |
                   +----------------+            +------------------+
                                                         |
                                                         v
                                                 +-------------------+
                                                 | Data Visualization|
                                                 |     (Grafana)     |
                                                 +-------------------+
```

---

## Key Features

* **Concurrent Consumer Pool:**
  Each consumer uses a configurable pool of goroutines to process messages concurrently, maximizing throughput and CPU utilization.

* **Retry & Dead-Letter Queue (DLQ):**
  Implements an exponential backoff retry mechanism. Messages failing after `N` retries are routed to a **DLQ** for later inspection or reprocessing.

* **Scalability (Vertical + Horizontal):**

  * **Vertical scaling:** Increase `CONSUMER_POOL_SIZE` to spawn more concurrent workers (goroutines) within a single consumer process.
  * **Horizontal scaling:** Increase `CONSUMER_REPLICAS` in Docker Compose to launch multiple consumer instances for distributed message processing.

* **Prioritized Message Processing:**
  Leverages RabbitMQ’s priority queues to ensure higher-priority messages are processed first.

* **Metrics & Monitoring:**
  Exposes Prometheus metrics on processing throughput, latency, retries, and DLQ counts.
  Includes a pre-configured **Grafana dashboard** that visualizes performance trends, queue depth, and error rates.

* **Graceful Shutdown:**
  Utilizes Go’s `context` package for safe shutdowns, ensuring all in-flight messages are acknowledged before exit.

* **Extensible Design:**
  Abstract producer and consumer interfaces make it simple to integrate with other message brokers (e.g., Kafka) or extend business logic.

---

## Tech Stack

| Component             | Technology                  | Purpose                                   |
| :-------------------- | :-------------------------- | :---------------------------------------- |
| Language              | **Go**                      | Core language for producers and consumers |
| Message Broker        | **RabbitMQ**                | Message routing, retries, DLQ             |
| Containerization      | **Docker & Docker Compose** | Multi-service orchestration               |
| Monitoring            | **Prometheus & Grafana**    | Metrics collection and visualization      |
| Dependency Management | **Go Modules**              | Package management                        |

---

## Getting Started

### Prerequisites

* [Docker & Docker Compose](https://docs.docker.com/get-docker/)

### 1. Clone the Repository

```bash
git clone https://github.com/GauthamKrM/gomq-pool
cd gomq-pool
```

### 2. Configure Environment

Copy and edit the environment configuration:

Below is a sample `.env` (pre-configured for Docker setup):

```env
RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/
RABBITMQ_QUEUE=test_queue
RABBITMQ_PREFETCH_COUNT=5
RABBITMQ_MAIN_EXCHANGE=app.main
RABBITMQ_RETRY_EXCHANGE=app.retry
RABBITMQ_DLQ_EXCHANGE=app.dlq
RABBITMQ_ROUTING_KEY=test_queue
RABBITMQ_RETRY_QUEUE=test_queue.retry
RABBITMQ_DLQ_QUEUE=test_queue.dlq
RABBITMQ_DURABLE=true
RABBITMQ_MAX_PRIORITY=10

PUBLISH_TIMEOUT=5s

CONSUMER_POOL_SIZE=5
WORKER_TIMEOUT=30s
CONSUMER_MAX_RETRIES=3
CONSUMER_RETRY_BASE_DELAY=2s
CONSUMER_METRICS_ENABLED=true
CONSUMER_METRICS_BIND=:2112
CONSUMER_METRICS_PATH=/metrics
CONSUMER_LIVE_PATH=/live
CONSUMER_READY_PATH=/ready
CONSUMER_REPLICAS=3
```

### 3. Run the System

Build and start all services (RabbitMQ, Prometheus, Grafana, Producer, and Consumer pool):

```bash
docker compose up -d --build
```

By default:

* **3 consumer replicas** are started (`CONSUMER_REPLICAS=3`)
* **Producer sends 100 messages**, with **50 simulated error messages** routed through the retry → DLQ pipeline to demonstrate system flow and metrics.

You can modify:

* Producer logic in `/cmd/producer/main.go`
* Consumer logic in `/internal/consumer/default_processor.go`

---

## Accessing Services

| Service               | URL                                              | Default Credentials      |
| :-------------------- | :----------------------------------------------- | :----------------------- |
| **RabbitMQ UI**       | [http://localhost:15672](http://localhost:15672) | guest / guest            |
| **Prometheus**        | [http://localhost:9090](http://localhost:9090)   | —                        |
| **Grafana Dashboard** | [http://localhost:3000](http://localhost:3000)   | admin / admin123         |

---

## Metrics Dashboard (Go MQ Consumers)

The pre-configured **Grafana dashboard** (`Go MQ Consumers`) visualizes system behavior and health across multiple dimensions:

* **Total Throughput (success/s):**
  Success rate of message processing across all consumer instances.

* **Failures and DLQ (events/s):**
  Tracks failed messages and DLQ forwarding rates, giving visibility into fault patterns.

* **Retries (events/s):**
  Monitors retry frequency and helps analyze message reprocessing behavior.

* **Processing Latency (P50 / P95 / P99) – By Priority:**
  Visualizes latency percentiles segmented by message priority for deeper performance insight.

* **Ack/Nack Errors (events/s):**
  Displays acknowledgment and negative acknowledgment errors per instance.

* **Main Queue Depth (messages):**
  Shows the total number of messages currently in the main queue.

* **Queue Ready vs Unacked (messages):**
  Compares ready vs unacknowledged messages, helping evaluate consumer backpressure and processing speed.

All dashboards are templated by **queue name** (default: `test_queue`).

---

## Core Implementation Highlights

* **Goroutines & Channels:** Concurrent worker pool for parallel message handling
* **Context & Cancellation:** Graceful shutdown and timeout control
* **Error Handling:** Retry with exponential backoff
* **Interfaces & Extensibility:** Abstract producers and consumers
* **Observability:** Integrated Prometheus metrics endpoints
* **Containerization:** Multi-service orchestration with Docker Compose

---

## Contributing

Contributions, issues, and feature requests are welcome!
Please open an issue or PR in the repository.

---

## License

This project is licensed under the **Apache-2.0 License**.
See the [LICENSE](./LICENSE) file for details.
