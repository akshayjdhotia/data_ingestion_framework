# Data Ingestion Pipeline — Bronze / Silver Layer

A production-grade, **metadata-driven PySpark data ingestion pipeline** that extracts data from multiple heterogeneous source systems and lands it into a data lakehouse following the **Medallion Architecture** (Bronze → Silver). Designed to run on **AWS EMR** and integrate with **Snowflake**, **AWS S3**, and **Apache Iceberg**.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Supported Source Systems](#supported-source-systems)
- [Project Structure](#project-structure)
- [Module Breakdown](#module-breakdown)
- [Configuration](#configuration)
- [Ingestion Patterns](#ingestion-patterns)
- [Infrastructure](#infrastructure)
- [Dependencies](#dependencies)
- [How to Run](#how-to-run)

---

## Overview

This pipeline is responsible for the **Bronze ingestion layer** — pulling raw source data into S3 / Iceberg and staging it in Snowflake's raw database. The Silver layer (`silver_raw_ingest.py`) then processes unprocessed files tracked in the Snowflake table `FILE_INGEST_LOG`.

All pipeline behaviour is controlled by metadata stored in the **Snowflake table `PIPELINE_CONFIG`** (or an Iceberg config table when `config_table_source=iceberg`), making it fully **configuration-driven** with zero code changes required for onboarding new source objects.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Source Systems                                │
│  Cassandra │ Azure SQL │ PostgreSQL │ MySQL │ MongoDB                │
└──────────────────────┬──────────────────────────────────────────────┘
                       │  spark-submit (bronze_ingest.py)
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Bronze Ingestion                              │
│                                                                      │
│  ┌──────────────┐   ┌────────────────┐   ┌──────────────────────┐   │
│  │ bronze_ingest│──▶│  bronze_core   │──▶│  bronze_connector    │   │
│  │  (entrypoint)│   │ (orchestration)│   │  (source adapters)   │   │
│  └──────────────┘   └───────┬────────┘   └──────────────────────┘   │
│                             │                                         │
│                  ┌──────────▼──────────┐                             │
│                  │     bronze_lib       │                             │
│                  │  (S3, Snowflake,     │                             │
│                  │   Iceberg utilities) │                             │
│                  └──────────┬──────────┘                             │
└─────────────────────────────┼───────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
          AWS S3          Snowflake     Apache Iceberg
        (Parquet)        (Raw DB)     (Glue Catalog)
              │
              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                Silver Layer (silver_raw_ingest.py)                   │
│   Reads unprocessed files from FILE_INGEST_LOG → Snowflake │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key Features

| Feature | Description |
|---|---|
| **Metadata-driven** | All source objects, extraction modes, and checkpoints are configured in a central metadata table — no code changes for new sources |
| **Multi-source connectors** | Single pipeline handles Cassandra, Azure SQL, PostgreSQL, MySQL, and MongoDB |
| **Dual config store** | Metadata can be sourced from Snowflake or Apache Iceberg depending on deployment |
| **Full & Incremental loads** | Supports full extract, key-based incremental, and hybrid `FULL_EXTRACT_INCR` modes |
| **MD5 change detection** | Computes row-level MD5 hashes for efficient change data detection in hybrid loads |
| **Iceberg support** | Native Apache Iceberg table writes with snapshot rollback for safe data recovery |
| **Secrets management** | All credentials fetched from AWS Secrets Manager at runtime — no plaintext secrets |
| **Ground Station** | Auto-discovers and ingests source schema/column metadata into Snowflake |
| **Multi-EMR compatibility** | Bootstrap scripts provided for EMR 5.24.1, 6.7.0, 6.9.0, and 6.12.0 |

---

## Supported Source Systems

| Source | Full Extract | Incremental (Key-based) |
|---|:---:|:---:|
| Apache Cassandra | ✅ | ✅ |
| Azure SQL (SQL Server) | ✅ | ✅ |
| PostgreSQL | ✅ | ✅ |
| MySQL | ✅ | ✅ |
| MongoDB | ✅ | — |

---

## Project Structure

```
data_ingestion/
├── src/
│   ├── bronze_ingest.py        # Spark entrypoint — parses args, starts session, drives Bronze ingestion
│   ├── bronze_core.py          # Core orchestration — metadata read, per-table ingestion loop
│   ├── bronze_connector.py     # Source adapters — reads from all supported source types
│   ├── bronze_lib.py           # Utilities — S3 paths, Snowflake I/O, Iceberg ops, secrets
│   ├── silver_raw_ingest.py    # Silver layer — processes unprocessed raw S3 files
│   └── grnd_stn_util.py        # Ground Station — source column metadata discovery
├── config/
│   └── install_python_modules.sh          # EMR bootstrap (default)
└── emr-6.12.0/
    └── install_python_modules_6_12_0.sh   # EMR 6.12.0 bootstrap
```

---

## Module Breakdown

### `bronze_ingest.py` — Entrypoint
- Parses `spark-submit` arguments: runtime config JSON, Snowflake credentials, database, warehouse, role, and URL.
- Fetches Snowflake credentials from **AWS Secrets Manager**.
- Initialises a Spark session — with or without **Iceberg** extensions based on runtime config.
- Optionally runs **Ground Station** metadata discovery (`get_meta_table_info=true`).
- Delegates to `bronze_work()` for the main ingestion loop.

### `bronze_core.py` — Orchestration
- Reads source object metadata from the Snowflake table `PIPELINE_CONFIG` (or Iceberg).
- Iterates over all configured tables and calls the appropriate source connector.
- Routes output to **S3 Parquet**, **Snowflake raw tables**, or **Iceberg tables**.
- Updates audit/downstream metadata after each successful write.
- Handles full loads, incremental loads, and MD5-based change detection loads.

### `bronze_connector.py` — Source Adapters
- **Cassandra**: Full table reads and key-based incremental reads using Spark Cassandra Connector.
- **Azure SQL**: JDBC reads with push-down predicates for incremental extraction.
- **PostgreSQL**: JDBC reads with full and key-based incremental support.
- **MySQL**: JDBC reads with full and key-based incremental support.
- **MongoDB**: Spark MongoDB Connector reads.
- Dynamically builds SQL `WHERE` clauses from metadata-stored checkpoint field configs.

### `bronze_lib.py` — Utilities
- S3 path generation (full-load and partitioned incremental paths).
- Snowflake read/write helpers (metadata tables, raw target tables).
- Iceberg table writes and **snapshot rollback** for fault-tolerant loads.
- AWS Secrets Manager helper (`get_secret`).
- MD5 hash computation for change detection (`prepare_data_md5`).
- Bronze and Silver control-field collection from metadata.

### `silver_raw_ingest.py` — Silver Ingest
- Reads the Snowflake table `FILE_INGEST_LOG` to discover unprocessed S3 file paths.
- Serialises source dataframes to raw JSON (`SRC` column) for landing in Snowflake.
- Resolves parquet file IDs and S3 paths for the current load batch.
- Writes processed records back to Snowflake Silver target tables.

### `grnd_stn_util.py` — Ground Station
- Dispatches schema/column metadata discovery to source-specific readers.
- Supports Cassandra, Azure SQL, MongoDB, PostgreSQL, and MySQL.
- Writes column metadata to Snowflake for use by downstream config tooling.

---

## Configuration

The pipeline is fully metadata-driven. Source objects are registered in the Snowflake table **`PIPELINE_CONFIG`** with the following key fields:

| Field | Description |
|---|---|
| `SOURCE_TABLE_NAME` | Source table or collection name |
| `TARGET_TABLE_NAME` | Target table name in Snowflake or Iceberg |
| `LAYER_TYPE` | `BRONZE` (full/incremental) or `SILVER` (silver layer) |
| `PULL_METHOD` | `FULL_EXTRACT`, `INCREMENTAL`, or `FULL_EXTRACT_INCR` |
| `FLATTEN_MODE` | Output serialisation strategy |
| `CHECKPOINT_FIELD_DEF` | Pipe-separated checkpoint field definitions (`field::type::operator`) |
| `CHECKPOINT_TO_VALUE` | Pipe-separated checkpoint values |

### Runtime JSON Config (passed via `spark-submit`)

```json
{
  "aws_bucket": "my-data-lake-bucket",
  "cs_keyspace": "my_source_namespace",
  "processing_grp": "GROUP_A",
  "no_of_out_files": "10",
  "sf_schema": "RAW",
  "global_region_name": "us-east-1",
  "source_name": "cassandra",
  "is_iceberg_enabled": "false",
  "config_table_source": "snowflake"
}
```

---

## Ingestion Patterns

### Full Extract
Reads the entire source table and writes to the full-load S3 prefix:
```
s3a://<bucket>/<table>/full_load/
```

### Incremental (Key-based)
Builds a dynamic SQL `WHERE` clause from the checkpoint metadata and reads only new/changed rows. Output lands in a time-partitioned S3 prefix:
```
s3a://<bucket>/<table>/incr_load/<yyyy>/<mm>/<dd>/<hh>/
```

### Hybrid (`FULL_EXTRACT_INCR`)
Performs a full extract, computes an MD5 hash per row, and uses the hash to detect changes against the previous snapshot — minimising downstream reprocessing.

---

## Infrastructure

The pipeline runs on **AWS EMR** using `spark-submit`. Python dependencies are bootstrapped via the provided shell scripts.

| EMR Version | Bootstrap Script |
|---|---|
| Default | `config/install_python_modules.sh` |
| 6.12.0 | `emr-6.12.0/install_python_modules_6_12_0.sh` |

### Example `spark-submit`

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files bronze_lib.py,bronze_connector.py,bronze_core.py,silver_raw_ingest.py,grnd_stn_util.py \
  bronze_ingest.py \
  '{"aws_bucket":"my-bucket","cs_keyspace":"my_ns","processing_grp":"GRP1","no_of_out_files":"5","sf_schema":"RAW","global_region_name":"us-east-1"}' \
  my-sf-secret \
  RAW_DB \
  MY_WAREHOUSE \
  MY_ROLE \
  https://myaccount.snowflakecomputing.com
```

---

## Dependencies

| Package | Purpose |
|---|---|
| `pyspark` | Distributed data processing |
| `boto3` | AWS S3 and Secrets Manager access |
| `pymongo` | MongoDB source connector |
| `pandas` | Metadata manipulation |
| `numpy` | Numerical utilities |
| `hvac` | HashiCorp Vault client |
| `great_expectations` | Data quality validation |
| `setuptools` | Python packaging |

Snowflake and Cassandra Spark connectors are provided as EMR-level JARs.

---

## How to Run

1. **Bootstrap EMR cluster** using the appropriate `install_python_modules.sh` for your EMR version.
2. **Register source objects** in the `PIPELINE_CONFIG` config table with the correct `PULL_METHOD` and checkpoint fields.
3. **Store credentials** in AWS Secrets Manager (Snowflake, Azure SQL, PostgreSQL, MongoDB as applicable).
4. **Submit the job** via `spark-submit` as shown above, passing the runtime JSON config.
5. **Monitor** via Spark UI and audit records written back to Snowflake.
