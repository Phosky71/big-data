# Hadoop Spark Airflow Stack

> Production-grade, fully containerised Big Data platform built with Docker Compose. Covers the complete data engineering pipeline: distributed storage, large-scale processing, workflow orchestration, S3-compatible data lake, full-stack observability, and business intelligence.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Services & Profiles](#services--profiles)
  - [Hadoop Core — `batch` profile](#hadoop-core--batch-profile)
  - [JupyterLab + Spark — `core` profile](#jupyterlab--spark--core-profile)
  - [MinIO Data Lake — `core` profile](#minio-data-lake--core-profile)
  - [Apache Airflow — `orchestration` profile](#apache-airflow--orchestration-profile)
  - [Monitoring Stack — `monitoring` profile](#monitoring-stack--monitoring-profile)
  - [Apache Superset — `bi` profile](#apache-superset--bi-profile)
- [Directory Structure](#directory-structure)
- [Requirements](#requirements)
- [Deployment](#deployment)
- [Service Endpoints](#service-endpoints)
- [Use Cases](#use-cases)
- [Configuration](#configuration)
- [License](#license)

---

## Overview

This repository delivers a self-contained Big Data platform that replicates the core components of a modern data engineering architecture. The entire stack is orchestrated through Docker Compose profiles, enabling selective service activation depending on workload requirements.

The platform addresses the full data lifecycle:

1. **Ingestion & Storage** — HDFS distributed filesystem and MinIO S3-compatible object store
2. **Processing** — Apache Spark running on YARN, accessible via JupyterLab
3. **Orchestration** — Apache Airflow with LocalExecutor for pipeline scheduling and management
4. **Observability** — Prometheus, Grafana, cAdvisor, Node Exporter and Telegraf for metrics collection and visualisation
5. **Business Intelligence** — Apache Superset for interactive dashboards and data exploration

All services are version-pinned, resource-constrained and configuration-driven, making the platform reproducible across environments.

---

## Architecture

```
+---------------------+     +----------------------+     +---------------------+
|   JupyterLab        |---->|   HDFS NameNode      |<----|   YARN Resource     |
|   + PySpark         |     |   + 2x DataNodes     |     |   Manager           |
+---------------------+     +----------------------+     +---------------------+
         |                                                          |
         v                                                          v
+---------------------+     +----------------------+     +---------------------+
|   MinIO             |     |   Apache Airflow     |     |   Apache Superset   |
|   (S3 / Data Lake)  |     |   (Orchestration)    |     |   (BI & Analytics)  |
+---------------------+     +----------------------+     +---------------------+
                                       |
                       +---------------+---------------+
                       |               |               |
                  Prometheus       Grafana         cAdvisor
               Node Exporter     Telegraf
```

---

## Services & Profiles

The stack is defined in `docker-compose.yml` and structured across five independent profiles that can be composed as needed.

### Hadoop Core — `batch` profile

A multi-node HDFS + YARN cluster based on the official `apache/hadoop:3.4.0` image.

| Service | Role | Exposed Port |
|---|---|---|
| `namenode` | HDFS NameNode — filesystem namespace and metadata management | `9870` |
| `datanode1` | HDFS DataNode — block storage node 1 | — |
| `datanode2` | HDFS DataNode — block storage node 2 | — |
| `resourcemanager` | YARN ResourceManager — cluster resource allocation and scheduling | `8088` |
| `nodemanager` | YARN NodeManager — container execution on worker nodes | — |

**Implementation notes:**
- All Hadoop containers use a unified custom entrypoint (`scripts/entrypoint.sh`) that normalises line endings and delegates startup to the appropriate Hadoop role.
- XML configuration (`core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`) is injected at runtime from `./configs/hadoop/`, keeping images environment-agnostic.
- Persistent storage is backed by named Docker volumes: `namenode_data`, `datanode1_data`, `datanode2_data`.
- Each container is memory-constrained to **512 MB** via `deploy.resources.limits`.

---

### JupyterLab + Spark — `core` profile

A custom JupyterLab image (built from `src/jupyter/Dockerfile`) pre-configured with PySpark and HDFS/MinIO connectivity.

| Parameter | Value |
|---|---|
| Exposed port | `8888` |
| Authentication | Disabled (token-free) |
| Working directory | `/home/jovyan/work` — entire repository mounted |
| `HADOOP_CONF_DIR` | `/opt/hadoop/etc/hadoop` |

- `core-site.xml` and `hdfs-site.xml` are mounted read-only, enabling Spark to resolve HDFS URIs (`hdfs://namenode:9000`).
- The container runs as `root` to avoid permission conflicts with Docker-managed volumes.
- JupyterLab state is persisted to the `jupyter_data` volume.

---

### MinIO Data Lake — `core` profile

An S3-compatible object store exposing the standard S3 API, usable from Spark via the `s3a://` connector.

| Service | Role | Exposed Ports |
|---|---|---|
| `minio` | Object storage server | `9000` (S3 API), `9001` (Web Console) |
| `minio-init` | One-shot initialisation: creates the `datalake` bucket and sets public read access | — |

**Default credentials:**
```
Access Key : admin
Secret Key : adminadmin
```

Object data is persisted to the `minio_data` volume.

---

### Apache Airflow — `orchestration` profile

Apache Airflow 2.8.3 with LocalExecutor, backed by a dedicated PostgreSQL 13 metadata database.

| Service | Role | Exposed Port |
|---|---|---|
| `postgres` | Airflow metadata backend | — |
| `airflow-init` | Executes DB migrations (`airflow db migrate`) and creates the initial admin user | — |
| `airflow-webserver` | Airflow UI and REST API | `8081` |
| `airflow-scheduler` | DAG parsing and task scheduling | — |

**Default credentials:** `admin` / `admin`

**Architecture decision:** Airflow containers do not bundle Spark or HDFS clients. Compute-intensive tasks are delegated to the `jupyter` container through `docker exec`, for which the scheduler mounts `/var/run/docker.sock`. This keeps the Airflow images lightweight and separates orchestration concerns from execution.

DAGs, plugins and additional configs are hot-reloaded from `./src/airflow/`.

---

### Monitoring Stack — `monitoring` profile

A full observability layer built on the Prometheus ecosystem.

| Service | Image | Port | Role |
|---|---|---|---|
| `prometheus` | `prom/prometheus:v2.51.1` | `9090` | Metrics scraping and time-series storage (6 h retention) |
| `grafana` | `grafana/grafana:10.4.1` | `3000` | Metrics visualisation and alerting |
| `cadvisor` | `ghcr.io/google/cadvisor:v0.53.0` | `8080` | Per-container resource utilisation metrics |
| `node-exporter` | `prom/node-exporter:v1.7.0` | `9100` | Host-level OS and hardware metrics |
| `telegraf` | `telegraf:1.33` | `9273` | Docker daemon metrics via Unix socket |

Grafana datasources and dashboards are provisioned automatically from `./configs/grafana/provisioning/` and `./configs/grafana/dashboards/`.

**Default Grafana credentials:** `admin` / `admin`

---

### Apache Superset — `bi` profile

Apache Superset built from `src/superset/Dockerfile`, sharing the PostgreSQL instance provisioned for Airflow.

| Parameter | Value |
|---|---|
| Exposed port | `8089` |
| Default credentials | `admin` / `admin` |
| Metadata backend | PostgreSQL — `superset` database on the shared `postgres` service |

On container startup, the entrypoint automatically executes `superset db upgrade`, creates the admin user and runs `superset init` before serving.

---

## Directory Structure

```
big-data/
|-- configs/
|   |-- grafana/
|   |   |-- dashboards/        # Grafana dashboard definitions (JSON)
|   |   `-- provisioning/      # Automated datasource and dashboard provisioning
|   |-- hadoop/
|   |   |-- core-site.xml      # HDFS NameNode URI and common settings
|   |   |-- hdfs-site.xml      # Replication factor and storage paths
|   |   |-- mapred-site.xml    # MapReduce execution framework config
|   |   `-- yarn-site.xml      # YARN ResourceManager address and settings
|   |-- prometheus/
|   |   `-- prometheus.yml     # Scrape targets, intervals and global config
|   `-- telegraf/
|       `-- telegraf.conf      # Docker socket input plugin + Prometheus output
|-- scripts/
|   `-- entrypoint.sh          # Unified Hadoop service entrypoint
|-- src/
|   |-- airflow/
|   |   |-- configs/           # Supplementary Airflow configuration
|   |   |-- dags/              # DAG definitions (auto-discovered)
|   |   |-- logs/              # Scheduler and task execution logs
|   |   `-- plugins/           # Custom Airflow operators and hooks
|   |-- data/                  # Source datasets and ingestion utilities
|   |-- jobs/                  # Spark batch job definitions
|   |-- jupyter/
|   |   `-- Dockerfile         # JupyterLab + PySpark image definition
|   `-- superset/
|       `-- Dockerfile         # Apache Superset image definition
|-- .gitignore
`-- docker-compose.yml         # Full platform definition with profiles and volumes
```

---

## Requirements

- [Docker Engine](https://docs.docker.com/engine/install/) >= 24.0 with Docker Compose plugin >= 2.20
- Minimum **8 GB RAM** allocated to Docker (4 GB for partial profile deployments)
- Minimum **20 GB** of available disk space for images and persistent volumes
- Git

---

## Deployment

### Clone the repository

```bash
git clone https://github.com/Phosky71/big-data.git
cd big-data
```

### Start the core platform (Hadoop + Spark + MinIO)

```bash
docker compose --profile batch --profile core up -d
```

### Add workflow orchestration

```bash
docker compose --profile orchestration up -d
```

### Add observability

```bash
docker compose --profile monitoring up -d
```

### Add business intelligence

```bash
docker compose --profile bi up -d
```

### Start the full platform

```bash
docker compose --profile batch --profile core --profile orchestration --profile monitoring --profile bi up -d
```

### Tear down

```bash
docker compose --profile batch --profile core --profile orchestration --profile monitoring --profile bi down
```

To also remove all persistent volumes:

```bash
docker compose ... down -v
```

---

## Service Endpoints

| Service | URL | Default Credentials |
|---|---|---|
| JupyterLab | http://localhost:8888 | No token |
| HDFS NameNode UI | http://localhost:9870 | — |
| YARN ResourceManager UI | http://localhost:8088 | — |
| MinIO Console | http://localhost:9001 | admin / adminadmin |
| MinIO S3 API | http://localhost:9000 | admin / adminadmin |
| Airflow UI | http://localhost:8081 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| cAdvisor | http://localhost:8080 | — |
| Node Exporter | http://localhost:9100/metrics | — |
| Telegraf metrics | http://localhost:9273/metrics | — |
| Superset | http://localhost:8089 | admin / admin |

---

## Use Cases

- **Distributed batch processing** — Run PySpark jobs on a YARN-managed cluster reading from and writing to HDFS.
- **Data lake pipelines** — Implement Bronze / Silver / Gold medallion architecture persisting Parquet and Delta files on MinIO via the `s3a://` connector.
- **Pipeline orchestration** — Schedule and monitor multi-step data workflows with Airflow, delegating Spark execution to the compute container.
- **Infrastructure observability** — Collect, store and visualise container and host metrics using Prometheus and Grafana with pre-provisioned dashboards.
- **Business intelligence** — Build interactive dashboards in Apache Superset on top of curated datasets produced by the data pipelines.

---

## Configuration

| Component | Configuration path |
|---|---|
| Hadoop (HDFS + YARN) | `configs/hadoop/*.xml` |
| Prometheus scrape targets | `configs/prometheus/prometheus.yml` |
| Grafana provisioning | `configs/grafana/provisioning/` |
| Grafana dashboards | `configs/grafana/dashboards/` |
| Telegraf inputs/outputs | `configs/telegraf/telegraf.conf` |
| Airflow DAGs | `src/airflow/dags/` |
| Spark jobs | `src/jobs/` |

All Hadoop XML files are mounted read-only into every Hadoop and JupyterLab container, ensuring consistent cluster configuration across services without rebuilding images.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
