# Big Data Educational Stack

> A Docker-based lab environment for learning modern data engineering end-to-end: from raw ingestion into HDFS and an S3-compatible data lake, through workflow orchestration and monitoring, to interactive BI dashboards.

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
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Service URLs](#service-urls)
- [Learning Scenarios](#learning-scenarios)
- [Notes & Caveats](#notes--caveats)

---

## Overview

This repository provides a fully containerised **Big Data** learning environment built with Docker Compose. It is designed for students and data engineers who want hands-on experience with the tools used in real-world data pipelines — without requiring a cloud account or a production cluster.

The stack covers the full data lifecycle:

1. **Storage** — HDFS distributed filesystem + MinIO S3-compatible object store
2. **Processing** — Apache Spark via JupyterLab notebooks
3. **Orchestration** — Apache Airflow with LocalExecutor
4. **Observability** — Prometheus, Grafana, cAdvisor, Node Exporter, Telegraf
5. **Visualisation** — Apache Superset BI dashboards

All components are activated via **Docker Compose profiles**, so you can start only the subset of services you need.

---

## Architecture

```
+------------------+     +-------------------+     +------------------+
|   JupyterLab     |---->|   HDFS NameNode   |<----|   YARN Resource  |
|   + PySpark      |     | + 2x DataNodes    |     |   Manager        |
+------------------+     +-------------------+     +------------------+
        |                                                    |
        v                                                    v
+------------------+     +-------------------+     +------------------+
|   MinIO          |     |  Apache Airflow   |     |  Apache Superset |
|   (S3 / datalake)|     |  (orchestration)  |     |  (BI dashboards) |
+------------------+     +-------------------+     +------------------+
                                    |
                    +---------------+---------------+
                    |               |               |
             Prometheus         Grafana         cAdvisor
             Node Exporter    Telegraf
```

---

## Services & Profiles

The entire stack is defined in `docker-compose.yml` and organised into five Docker Compose profiles.

### Hadoop Core — `batch` profile

Implements a minimal HDFS + YARN cluster using the official `apache/hadoop:3.4.0` image.

| Service | Description | Port |
|---|---|---|
| `namenode` | HDFS NameNode — manages filesystem metadata | `9870` |
| `datanode1` | HDFS DataNode 1 — stores data blocks | — |
| `datanode2` | HDFS DataNode 2 — stores data blocks | — |
| `resourcemanager` | YARN ResourceManager — global resource scheduling | `8088` |
| `nodemanager` | YARN NodeManager — executes containers and tasks | — |

**Key details:**
- All Hadoop containers use a custom entrypoint (`scripts/entrypoint.sh`) that fixes Windows line-endings and starts the correct Hadoop role.
- Hadoop XML configuration (`core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`) is injected from `./configs/hadoop/`.
- Each node persists its data to a named Docker volume (`namenode_data`, `datanode1_data`, `datanode2_data`).
- Memory limit per container: **512 MB** (tuned for laptops).

---

### JupyterLab + Spark — `core` profile

A custom JupyterLab image (built from `src/jupyter/Dockerfile`) that includes PySpark and is pre-configured to talk to HDFS and MinIO.

| Setting | Value |
|---|---|
| Port | `8888` |
| Token | Disabled (no auth for local dev) |
| Working directory | `/home/jovyan/work` (entire repo is mounted) |
| HADOOP_CONF_DIR | `/opt/hadoop/etc/hadoop` |

- Hadoop `core-site.xml` and `hdfs-site.xml` are mounted read-only so Spark can resolve the NameNode.
- Runs as `root` inside the container to simplify Docker volume permissions.
- State is persisted to the `jupyter_data` volume.

---

### MinIO Data Lake — `core` profile

MinIO provides an S3-compatible object store, usable from Spark via the `s3a://` connector.

| Service | Description | Port |
|---|---|---|
| `minio` | Object store server | `9000` (API), `9001` (Console) |
| `minio-init` | One-shot init container: creates the `datalake` bucket | — |

**Default credentials:**

```
Access Key : admin
Secret Key : adminadmin
```

---

### Apache Airflow — `orchestration` profile

Apache Airflow 2.8.3 with LocalExecutor backed by PostgreSQL 13.

| Service | Description | Port |
|---|---|---|
| `postgres` | Airflow metadata database | — |
| `airflow-init` | Runs DB migrations and creates the admin user | — |
| `airflow-webserver` | Airflow UI | `8081` |
| `airflow-scheduler` | DAG scheduler | — |

**Default credentials:** `admin` / `admin`

**Design note:** Airflow does not bundle Spark or HDFS clients. Heavy processing is delegated to the `jupyter` container via `docker exec` — the scheduler mounts `/var/run/docker.sock` for this purpose.

DAGs, logs and plugins are hot-reloaded from `./src/airflow/`.

---

### Monitoring Stack — `monitoring` profile

A lightweight observability layer using industry-standard open-source tools.

| Service | Image | Port | Description |
|---|---|---|---|
| `prometheus` | `prom/prometheus:v2.51.1` | `9090` | Metrics collection & storage (6 h retention) |
| `grafana` | `grafana/grafana:10.4.1` | `3000` | Dashboards & alerting |
| `cadvisor` | `ghcr.io/google/cadvisor:v0.53.0` | `8080` | Container resource metrics |
| `node-exporter` | `prom/node-exporter:v1.7.0` | `9100` | Host-level OS metrics |
| `telegraf` | `telegraf:1.33` | `9273` | Docker daemon metrics via socket |

Grafana provisioning (datasources + dashboards) is loaded automatically from `./configs/grafana/provisioning/` and `./configs/grafana/dashboards/`.

**Default Grafana credentials:** `admin` / `admin` (anonymous access enabled).

---

### Apache Superset — `bi` profile

Apache Superset built from `src/superset/Dockerfile`, connected to the shared PostgreSQL instance.

| Setting | Value |
|---|---|
| Port | `8089` |
| Default credentials | `admin` / `admin` |
| Database backend | PostgreSQL (`superset` database on the shared `postgres` service) |

On first start, the container automatically runs `superset db upgrade`, creates the admin user and calls `superset init`.

---

## Directory Structure

```
big-data/
|-- configs/
|   |-- grafana/
|   |   |-- dashboards/        # Pre-built Grafana dashboard JSON files
|   |   `-- provisioning/      # Datasource and dashboard provisioning
|   |-- hadoop/
|   |   |-- core-site.xml      # HDFS connection settings
|   |   |-- hdfs-site.xml      # HDFS replication and paths
|   |   |-- mapred-site.xml    # MapReduce framework config
|   |   `-- yarn-site.xml      # YARN resource manager config
|   |-- prometheus/
|   |   `-- prometheus.yml     # Scrape targets and global settings
|   `-- telegraf/
|       `-- telegraf.conf      # Docker socket input + Prometheus output
|-- scripts/
|   `-- entrypoint.sh          # Custom entrypoint for all Hadoop services
|-- src/
|   |-- airflow/
|   |   |-- configs/           # Airflow extra config files
|   |   |-- dags/              # DAG definitions (auto-reloaded)
|   |   |-- logs/              # Scheduler and task logs
|   |   `-- plugins/           # Custom Airflow plugins
|   |-- data/                  # Datasets and data utilities
|   |-- jobs/                  # Spark batch jobs
|   |-- jupyter/
|   |   `-- Dockerfile         # Custom JupyterLab + PySpark image
|   `-- superset/
|       `-- Dockerfile         # Custom Superset image
|-- .gitignore
`-- docker-compose.yml         # Full stack definition with profiles
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine + Docker Compose plugin)
- At least **8 GB of RAM** available to Docker (4 GB minimum for partial stacks)
- Git

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/Phosky71/big-data.git
cd big-data
```

### 2. Start the Hadoop + Jupyter + MinIO stack

```bash
docker compose --profile batch --profile core up -d
```

### 3. (Optional) Add workflow orchestration

```bash
docker compose --profile orchestration up -d
```

### 4. (Optional) Add monitoring

```bash
docker compose --profile monitoring up -d
```

### 5. (Optional) Add BI dashboards

```bash
docker compose --profile bi up -d
```

### Stop everything

```bash
docker compose --profile batch --profile core --profile orchestration --profile monitoring --profile bi down
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| JupyterLab | http://localhost:8888 | No token |
| HDFS NameNode UI | http://localhost:9870 | — |
| YARN ResourceManager UI | http://localhost:8088 | — |
| MinIO Console | http://localhost:9001 | admin / adminadmin |
| Airflow UI | http://localhost:8081 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| cAdvisor | http://localhost:8080 | — |
| Node Exporter | http://localhost:9100/metrics | — |
| Superset | http://localhost:8089 | admin / admin |

---

## Learning Scenarios

This stack is suitable for a variety of hands-on learning exercises:

- **HDFS exploration** — Upload, read and manage files on a real distributed filesystem from Jupyter notebooks.
- **Spark on YARN** — Submit PySpark jobs that run on the YARN cluster using `spark-submit` or the Spark session inside Jupyter.
- **S3A + MinIO** — Implement a Bronze / Silver / Gold medallion data lake architecture storing Parquet files on MinIO.
- **Airflow pipelines** — Build DAGs that orchestrate Spark jobs running inside the Jupyter container via Docker operator.
- **Observability** — Explore container metrics in Grafana, set up alerting rules in Prometheus, and understand cAdvisor and Node Exporter dashboards.
- **BI dashboards** — Connect Superset to a PostgreSQL dataset produced by your pipelines and build interactive charts.

---

## Notes & Caveats

- **Not for production.** Passwords, tokens and security settings are intentionally simple for local development and classroom use.
- **Resource limits** are tuned for a typical developer laptop. Adjust `deploy.resources.limits` in `docker-compose.yml` if you have more RAM available.
- The Airflow scheduler and Telegraf containers run as `root` and mount the Docker socket — this is expected in a local lab but would be a security concern in any shared environment.
- Prometheus retention is set to **6 hours** to keep disk usage low. Increase `--storage.tsdb.retention.time` if you need longer history.
- Custom Hadoop configuration files in `./configs/hadoop/` are mounted read-only into every Hadoop and Jupyter container so all services share the same cluster settings.

---

## License

This project is provided for educational purposes. Feel free to use and adapt it for learning or teaching.
Add your preferred open-source license (e.g. MIT, Apache-2.0) as needed.
