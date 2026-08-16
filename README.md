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

### Benchmark Matrix

The following matrix compares Prismio's throughput in local and Internet-based
environments. Values represent measured throughput in events per second (EPS)

| Environment | No spike | Scattered events<br>`< 100k event` | Medium load<br>`1M event` | Heavy load<br>`10M event` |
| --- | ---: | ---: | ---: | ---: |
| Local | BatchSize |~ 50k |~ 45k |~ 45k |
| Internet | Batchsize |~ 45 to 50k  |~ 38 to 45k |~ 33 to 40k |

---

## Deep Dive into Technical Innovations

### 1. Adaptive Dynamic Batching Engine

Instead of committing to one fixed batch size, Prismio behaves like someone balancing a stick. It takes a small step to one side, watches whether the pipeline performs better, and keeps moving in that direction when it does. If performance gets worse, it steps back and tries the other side. After repeated flushes, the system gradually settles near the batch size that keeps the pipeline balanced.
  
  ![alt text](https://files.catbox.moe/17k9ez.gif)

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
- **Interface:** Terminal UI (`tview`)
- **Database drivers:** PostgreSQL is currently supported for both CDC sources and destinations. Additional drivers can be added in the future through the driver registry.

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

Create the logical replication slot used by Prismio:

```sql
SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
```

The slot (`my_slot` in the sample URL below) can also be created automatically by Prismio on first run if it does not already exist. The connecting user must have replication privileges and permission to create the slot.

On the destination side, the target tables must already exist. Prismio only writes data; it does not create destination schemas or tables. Destination tables should have matching primary keys so that INSERT upserts and UPDATE/DELETE statements can be applied correctly.

### 3. First run — create an account

On the first `go run main.go`, if `accounts.yaml` doesn't exist yet, it will be created automatically (empty). From the login screen:

1. Click **Create API Key**, enter a username, then click **Create**.
2. Prismio generates an API key — **copy and save it**, since it is only shown once.
3. Return to the login screen and log in with the username and API key you just created.

Each account has its own operational config (`configs/<username>.yaml`) and its own checkpoint directory (`local_checkpoints/<username>/`), fully isolated from other accounts. The shared `accounts.yaml` stores account authentication data, while operational settings are stored in the per-account configuration file. Keep the generated API key safe: it is displayed only once.

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

### 5. Configuration reference

The following values are initialized with these defaults. Values that control channel or pipeline creation are read during startup and require a restart to take effect.

| Parameter | Meaning |
| --- | --- |
| Feedback Interval | Interval in seconds between PostgreSQL standby-status feedback messages. Default: `10`. |
| Pipeline Max Size | Capacity of the central event channel. Default: `1000`. |
| Bag Max Size | Standard number of events collected before processing. Default: `10,000`. |
| Bag Max Multiple | Maximum bag-size multiplier used during bursts. Default: `5`. |
| Data Processing Worker Count | Number of concurrent workers that decode events and build SQL statements. Default: `10`. |
| Batch Max Size | Maximum number of SQL statements written in one flush batch. Default: `5,000`. |
| Batch Timeout | Maximum wait before flushing an incomplete batch, in milliseconds. Default: `200`. |
| Flush Timeout | Maximum duration of one database flush, in milliseconds. Default: `120,000`. |
| Max Retries | Maximum number of retries for failed connections or operations. Default: `3`. |
| Base Retry Delay | Initial retry delay, in milliseconds. Default: `2,000`. |
| Max Retry Delay | Maximum retry delay, in milliseconds. Default: `30,000`. |
| Monitor Interval | Interval between monitoring log updates, in seconds. Default: `5`. |

### 6. Remote / multi-node deployment (optional)

1. **Network setup:** Join all nodes into the same virtual private network using [Tailscale](https://tailscale.com/kb/1017/install) or [ZeroTier](https://docs.zerotier.com/getting-started/).
2. **PostgreSQL remote access:**
   - Configure `postgresql.conf` (`listen_addresses = '*'`) and `pg_hba.conf` to allow remote connections and replication slots. See the [Postgres client authentication guide](https://www.postgresql.org/docs/current/auth-pg-hba-conf.html).
   - Enable logical replication on the source DB following the [official Postgres replication setup guide](https://www.postgresql.org/docs/current/logical-replication-config.html).

### 7. Advanced Auto-Tuner Configuration

The following settings control the automatic tuning behavior. They are intended for advanced users who need to adjust Prismio for a specific workload.

| Parameter | Meaning |
| --- | --- |
| Tuning Mode | `manual` keeps user-configured values fixed. `automatic` enables runtime tuning. Default: `manual`. |
| RAM Ceiling | RAM usage percentage at which the tuner enters throttling mode and reduces the batch size. Default: `95%`. |
| RAM Safe Resume | RAM usage must fall below this percentage before normal probing resumes. Default: `85%`. |
| Backlog High Watermark | Backlog duration, in seconds, above which the tuner adds a worker. Default: `1.5`. |
| Backlog Low Watermark | Backlog duration, in seconds, below which the tuner removes a worker, down to the minimum. Default: `0.1`. |
| Minimum Workers | Lowest number of data-processing workers. Default: `1`. |
| Minimum Batch Timeout | Lower bound for the automatically calculated batch timeout. Default: `20 ms`. |
| Maximum Batch Timeout | Upper bound for the automatically calculated batch timeout. Default: `5,000 ms`. |
| Timeout Margin Factor | Safety multiplier applied to the estimated time needed to fill a batch. Default: `1.3`. |
| Idle Stale Factor | Number of tuner intervals without a flush before the system is treated as idle. Default: `2`. |

---

## Notes

This is currently a learning/experimental project. Security hardening such as TLS enforcement and secrets management is not enabled by default. Prometheus metrics and the remote HTTP configuration endpoint are not currently active. Prismio does not create destination schemas or tables, so those must be prepared in advance. Long periods of RAM pressure may interrupt replication feedback while WAL intake is paused.

The examples and benchmark figures in this README are illustrative and may not represent the system's behavior with complete accuracy. Use them as general references rather than guaranteed results.
