# gomq-pool

A **Message Queue Consumer Pool** implementation in Go, designed for scalable and fault-tolerant message processing.  
The project demonstrates how to build a pool of concurrent consumers that handle messages from **RabbitMQ** broker, with support for retries, dead-letter queues, and monitoring.

---

## Features
- **Concurrent Consumer Pool** – Multiple Go workers (goroutines) consuming messages in parallel.  
- **Retry Mechanism** – Automatic retries with exponential backoff for failed tasks.  
- **Dead Letter Queue (DLQ)** – Failed messages after max retries are routed to a DLQ.  
- **Graceful Shutdown** – Safe consumer termination using Go contexts.  
- **Scalability** – Run multiple consumer instances via Docker Compose.  
- **Metrics & Monitoring** – Prometheus metrics endpoint for throughput and error tracking (planned).  

---

## Architecture
```text
+------------+        +--------------------+        +------------+
| Producer(s)| -----> | Message Broker     | -----> | Consumer(s)|
|  (Go)      |        | (RabbitMQ/Kafka)   |        |   (Go)     |
+------------+        +--------------------+        +------------+
                           |                     \
                           |                      \
                           v                       \
                   +----------------+            +----------------+
                   | Dead Letter    |            | Metrics Export |
                   | Queue (DLQ)    |            | (Prometheus)   |
                   +----------------+            +----------------+
```

---

## Getting Started

### Prerequisites
- [Go 1.22+](https://go.dev/dl/)  
- [Docker & Docker Compose](https://docs.docker.com/get-docker/)  

---
