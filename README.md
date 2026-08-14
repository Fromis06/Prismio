# Prismio: Lightweight & Adaptive CDC Engine in Go

Prismio is a standalone, single-language Change Data Capture (CDC) engine built entirely in Golang. Designed as a high-efficiency alternative to heavy, infrastructure-dependent frameworks like Kafka Connect or Debezium, Prismio implements an intelligent self-optimizing data pipeline to capture and replicate real-time database changes with a minimal hardware footprint.

![alt text](https://i.imgur.com/yPcRqRt.gif)

## Key Features

- **Zero-Dependency Core Architecture:** Operates as a single binary without requiring external message brokers or complex multi-language runtime environments.
- **Adaptive Dynamic Batching Engine:** Automatically tunes processing batch sizes and linger timeouts based on real-time traffic volume, resolving static configuration bottlenecks.
- **Medium-High Performance Handling:** Leverages Go's native concurrency primitives (`Goroutines` and `Channels`) to process intense data workloads efficiently.
- **Resilient Memory Management:** Features custom buffering and overflow logic mechanisms to protect system stability and prevent memory exhaustion during sudden traffic spikes.
- **Integrated Monitoring Dashboard:** A built-in terminal UI delivering real-time telemetry on pipeline health, queue sizes, throughput, and replication latency.

---

## Core Performance Metrics & Benchmarks

The system was rigorously stress-tested using continuous transaction generations to simulate production-level data fluctuations.

- **Throughput:** Achieves a sustained processing speed of **~40,000 EPS** (Events Per Second).
- **Data Bitrate:** Equivalent to **~20 MB/s** data throughput (calculated based on an average ~0.5KB JSON payload size per database event).
- **Latency Reduction:** The adaptive engine successfully cuts **Data Propagation Delay** by **~60%** during low-traffic periods and sudden data stream shifts compared to traditional static configurations.

---

## Deep Dive into Technical Innovations

### 1. Adaptive Dynamic Batching Engine

While standard streaming engines typically rely on pre-configured batch parameters at runtime, Prismio introduces an adaptive approach by continuously monitoring data velocity in real-time.

- **High Traffic:** Automatically expands the batch size to maximize throughput.

  ![alt text](https://i.imgur.com/hanaCK4.gif)

- **Low Traffic:** Instantly shrinks the batch window and flushes data down the pipeline, ensuring events do not get stuck waiting for a full batch buffer.

  ![alt text](https://i.imgur.com/ZRZ1J7a.gif)

### 2. Overflow Logic & Memory Guard

To achieve high resilience without heavy cluster dependencies, a proprietary overflow logic is built directly over Go channels. When downstream sinks face transient blockages or sudden input spikes exceed processing capacity, the system triggers memory-safe backpressure management, preventing standard memory exhaustion (OOM crashes).

---

## Architecture & Technology Stack

- **Core Engine:** Golang (leveraging native Go concurrency primitives)
- **Source Database:** PostgreSQL (logical replication via `pglogrepl`)
- **Interface:** Terminal UI (`tview`), with a built-in HTTP endpoint for live config updates and pprof profiling

---

## Setup & Installation

### Requirements

- Go 1.26 or higher
- A PostgreSQL instance to use as the source, with logical replication enabled (`wal_level = logical`)
- `protoc` and `protoc-gen-go` — only needed if you plan to modify `internal/pb/event.proto` and regenerate `event.pb.go`

### 1. Clone and build

```bash
git clone https://github.com/Fromis06/Prismio.git
cd Prismio
go build -o my-cdc main.go
```

Or run it directly without building a binary:

```bash
go run main.go
```

This opens the terminal UI directly — there is no separate server mode or `--mode` flag; the TUI is the only entry point.

### 2. Prepare the source PostgreSQL database

On the source database:

```sql
-- Enable logical replication (requires a Postgres restart after changing wal_level)
ALTER SYSTEM SET wal_level = logical;

-- Make sure the connecting user has replication privileges
ALTER ROLE your_user WITH REPLICATION;

-- Create a publication for the tables you want to track
CREATE PUBLICATION my_pub FOR TABLE table1, table2;

-- For tables that receive UPDATE/DELETE, set REPLICA IDENTITY so the
-- "before" image of the row is available
ALTER TABLE table1 REPLICA IDENTITY FULL;
```

The replication slot (`my_slot` in the sample URL below) is created automatically on first run — you don't need to create it manually.

On the destination side, the target tables must already exist. Prismio only writes data; it does not create destination schemas.

### 3. First run — create an account

On the first `go run main.go`, if `accounts.yaml` doesn't exist yet, it will be created automatically (empty). From the login screen:

1. Click **Create API Key**, enter a username, then click **Create**.
2. Prismio generates an API key — **copy and save it**, since it is only shown once.
3. Return to the login screen and log in with the username and API key you just created.

Each account has its own operational config (`configs/<username>.yaml`) and its own checkpoint directory (`local_checkpoints/<username>/`), fully isolated from other accounts.

### 4. Configure source and destinations in the TUI

After logging in, you'll land on the configuration screen:

1. **Choose a data source**: select a driver type (PostgreSQL is currently available), then fill in the Source URL, e.g.:

   ```
   postgres://user:password@host:5432/dbname?sslmode=disable&slot_name=my_slot&publication_names=my_pub
   ```

2. Click the source's connection-check action row — it must show OK (green) before you can run.
3. **Add a new destination**: select a sink type, fill in the destination URL, then click its connection-check action row.
4. Repeat step 3 to add as many destinations as needed.
5. Once every check row shows OK, click **Run CDC** to start the pipeline.

Performance parameters (Worker Count, Batch Size, Batch Timeout, etc.) can be edited directly in the same configuration table before running.

### 5. Remote / multi-node deployment (optional)

1. **Network setup:** Join all nodes into the same virtual private network using [Tailscale](https://tailscale.com/kb/1017/install) or [ZeroTier](https://docs.zerotier.com/getting-started/).
2. **PostgreSQL remote access:**
   - Configure `postgresql.conf` (`listen_addresses = '*'`) and `pg_hba.conf` to allow remote connections and replication slots. See the [Postgres client authentication guide](https://www.postgresql.org/docs/current/auth-pg-hba-conf.html).
   - Enable logical replication on the source DB following the [official Postgres replication setup guide](https://www.postgresql.org/docs/current/logical-replication-config.html).

---

## Notes

This is currently a learning/experimental project. Local-only connections are assumed, and security hardening (TLS enforcement, secrets management, etc.) is intentionally deprioritized in favor of architectural clarity and extensibility.