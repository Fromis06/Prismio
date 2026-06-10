# Prismio: Lightweight & Adaptive CDC Engine in Go

Prismio is a standalone, single-language Change Data Capture (CDC) engine built entirely in Golang. Designed as a high-efficiency alternative to heavy, infrastructure-dependent frameworks like Kafka Connect or Debezium, Prismio implements an intelligent self-optimizing data pipeline to capture and replicate real-time database changes with a minimal hardware footprint.

## 🚀 Key Features

- **Zero-Dependency Core Architecture:** Operates as a single binary without requiring external message brokers or complex multi-language runtime environments.
- **Adaptive Dynamic Batching Engine:** Automatically tunes processing batch sizes and linger timeouts based on real-time traffic volume, resolving static configuration bottlenecks.
- **Medium-High Performance Handling:** Leverages Go's native concurrency primitives (`Goroutines` and `Channels`) to process intense data workloads efficiently.
- **Resilient Memory Management:** Features custom buffering and overflow logic mechanisms to protect system stability and prevent memory exhaustion during sudden traffic spikes.
- **Integrated Monitoring Dashboard:** A built-in user interface delivering real-time telemetry on pipeline health, queue sizes, throughput, and replication latency.

---

## 📊 Core Performance Metrics & Benchmarks

The system was rigorously stress-tested using continuous transaction generations to simulate production-level data fluctuations.

- **Throughput:** Achieves a sustained processing speed of **~40,000 EPS** (Events Per Second).
- **Data Bitrate:** Equivalent to **~20 MB/s** data throughput (calculated based on an average ~0.5KB JSON payload size per database event).
- **Latency Reduction:** The adaptive engine successfully cuts **Data Propagation Delay** by **~60%** during low-traffic periods and sudden data stream shifts compared to traditional static configurations.

---

## 🛠️ Deep Dive into Technical Innovations

### 1. Adaptive Dynamic Batching Engine
Unlike industry standards (e.g., Debezium) where parameters like `batch.size` or `linger.ms` are locked statically at runtime, Prismio monitors incoming data velocity in real-time. 
- **High Traffic:** Automatically expands the batch size to maximize throughput.
- **Low Traffic:** Instantly shrinks the batch window and flashes data down the pipeline, ensuring events do not get stuck waiting for a full batch buffer.

### 2. Overflow Logic & Memory Guard
To achieve high resilience without heavy cluster dependencies, a proprietary overflow logic is built directly over Go channels. When downstream sinks face transient blockages or sudden input spikes exceed processing capacity, the system triggers memory-safe backpressure management, preventing standard memory exhaustion (OOM crashes).

---

## 🏗️ Architecture & Technology Stack

- **Core Engine:** Golang (Leveraging high-performance Go Concurrency primitives)
- **Source Database:** PostgreSQL
- **Monitoring Layer:** Built-in Web UI Dashboard (API integration via independent ports)

---

## ⚡ Quick Start & Setup

### Prerequisites
- Go 1.21 or higher
- A running PostgreSQL instance with logical replication enabled (`wal_level = logical`)

### Installation
```bash
# Clone the repository
git clone [https://github.com/Fromis06/Prismio.git](https://github.com/Fromis06/Prismio.git)
cd Prismio

# Build the standalone binary
go build -o prismio main.go

# Run the engine
./prismio --config=config.yaml
