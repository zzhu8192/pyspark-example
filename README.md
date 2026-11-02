# PySpark 4.x Example Project

[![Python 3.12](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://www.python.org/)
[![PySpark 4.2.x](https://img.shields.io/badge/PySpark-4.2.x-orange.svg)](https://spark.apache.org/)
[![Delta Lake 4.2.0+](https://img.shields.io/badge/Delta%20Lake-4.2.0%2B-00ADD8.svg)](https://delta.io/)
[![Java 21](https://img.shields.io/badge/Java-21%20LTS-red.svg)](https://adoptium.net/)
[![uv](https://img.shields.io/badge/managed%20by-uv-DE5FE9.svg)](https://astral.sh/uv)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

A production-ready project template for **PySpark 4.x** ETL pipelines featuring **Delta Lake 4.2.x**, **Click CLI**, **Pytest** testing fixtures, remote **Apache Derby Hive Metastore** configuration, and rapid environment management using **[uv](https://docs.astral.sh/uv/)**.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [System Requirements](#system-requirements)
- [Prerequisites & Installation](#prerequisites--installation)
- [Delta Lake Integration Guide](#delta-lake-integration-guide)
- [Hive Metastore with Remote Derby Setup](#hive-metastore-with-remote-derby-setup)
- [Quick Start](#quick-start)
- [Running the Application](#running-the-application)
- [Running & Debugging in IDEs](#running--debugging-in-ides)
- [Troubleshooting & Common Errors](#troubleshooting--common-errors)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

Setting up PySpark 4.x with Delta Lake and Hive metastore integration often presents several common challenges:
- **Dependency Conflicts**: Delta Lake wheel packages on PyPI declare strict upper bounds on PySpark versions.
- **Delta Parser Wiring**: Missing SQL extension registration prevents Spark from recognizing Delta commands (`USING delta`, `MERGE INTO`, `VACUUM`).
- **Hive Metastore Locks**: By default, embedded Derby locks the local database (`metastore_db/`), preventing multiple test processes, shells, or notebooks from running concurrently.
- **Java 21 Module Encapsulation**: Modern Java versions require explicit `--add-opens` JVM flags for Spark reflection and memory management.

This skeleton project provides a **clean, structured foundation** resolving these challenges so you can immediately build, test, and package robust PySpark pipelines.

---

## Key Features

- **PySpark 4.x & Delta Lake**: Pre-configured for Spark 4.2.x and Delta Lake 4.2.0+.
- **CLI Architecture with Click**: Modular command-line interface in [`src/etl/main.py`](src/etl/main.py) with typed parameters.
- **Shared Pytest Fixtures**: Session-scoped SparkSession in [`tests/etl/conftest.py`](tests/etl/conftest.py) with automated cache cleanup and isolated temporary catalogs.
- **Sample Spark & Hive Configurations**: Ready-to-use template files in [`conf/`](conf/) and [`resources/`](resources/):
  - [`conf/spark-defaults.conf.template`](conf/spark-defaults.conf.template): Delta catalog, SQL extensions, and adaptive query execution.
  - [`conf/core-site.xml.template`](conf/core-site.xml.template): Remote Derby Network Server connection for concurrent multi-process access.
  - [`resources/derby_hive_3.2.0.sql`](resources/derby_hive_3.2.0.sql): Schema definition script for initializing Hive metastore tables in Derby.
- **Fast Package Management**: Managed with [`uv`](https://docs.astral.sh/uv/) for lightning-fast dependency resolution and virtual environments.
- **Cross-Platform Execution**: Includes `./run.sh` for Linux/macOS and `run.bat` for Windows.

---

## Tech Stack

| Component | Version | Notes |
|:---|:---|:---|
| **Python** | 3.10 - 3.12 | Python 3.12 recommended |
| **Java** | 21 (LTS) | Required by Apache Spark 4.x runtime |
| **PySpark** | 4.2.x (4.2.0 / 4.2.1) | Distributed DataFrame engine |
| **Delta Lake** | 4.2.0+ (4.2.0 / 4.4.1) | ACID transactions and time travel |
| **Click** | 8.1+ | CLI command parsing |
| **Pytest & pytest-xdist** | 8.x / 3.x | Unit testing framework |
| **Package Manager** | uv | Fast Python packaging and runner |

---

## System Requirements

Running PySpark jobs locally and executing tests requires adequate CPU and memory:

| Tier | RAM | CPU Cores | Storage | Experience |
|:---|:---|:---|:---|:---|
| **Recommended** | 16 GB - 32 GB | 8+ cores / 16 threads | SSD | Fast JVM startup, smooth test execution |
| **Minimum** | 8 GB - 16 GB | 4 cores / 8 threads | SSD | Functional for local development |
| **Not Recommended** | < 8 GB | 2 cores | HDD | Prone to OutOfMemory errors and slow JVM initialization |

---

## Prerequisites & Installation

### 1. Java 21 (LTS)
PySpark 4.x runs atop the JVM. Install OpenJDK 21 (such as [Eclipse Temurin](https://adoptium.net/)):
- **Linux (Ubuntu/Debian)**:
  ```bash
  sudo apt update && sudo apt install -y openjdk-21-jdk
  ```
- **macOS (Homebrew)**:
  ```bash
  brew install openjdk@21
  ```
- **Windows**: Download installer from Adoptium. Set `JAVA_HOME` and add `%JAVA_HOME%\bin` to `PATH`.

Verify:
```bash
java -version
```

### 2. uv Package Manager
Install `uv` (recommended for fastest setup):
```bash
# Linux / macOS
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Verify:
```bash
uv --version
```

### 3. Windows Hadoop Winutils (Windows Only)
On Windows, Hadoop file operations require native binaries:
1. Download Hadoop 3.3/3.4 compatible binaries from [cdarlint/winutils](https://github.com/cdarlint/winutils).
2. Set `HADOOP_HOME=C:\hadoop` where `C:\hadoop\bin\winutils.exe` exists.
3. Add `%HADOOP_HOME%\bin` to system `PATH`.

---

## Delta Lake Integration Guide

Integrating Delta Lake with PySpark requires **both** runtime JARs and Spark SQL configurations.

### 1. Understanding PyPI vs. Cluster JAR Dependencies
- **In Python / uv**: The `delta-spark` package from PyPI installs Python bindings and bundles the Delta Lake Java JARs for local execution.
  > [!NOTE]
  > On PyPI, `delta-spark` wheel releases declare strict dependency bounds on `pyspark`:
  > - `delta-spark==4.2.0` specifies `pyspark<=4.1.1,>=4.0.1`.
  > - `delta-spark==4.4.1` specifies `pyspark<=4.2.0,>=4.0.1`.
  > 
  > This project's [`pyproject.toml`](pyproject.toml) configures `"delta-spark>=4.2.0"` and `"pyspark>=4.0.1"`, allowing `uv` to automatically resolve compatible versions.
- **For `spark-submit` / Cluster Runs**: When running through an external `spark-submit`, Spark does not bundle Delta Lake by default. You must either:
  - Pass `--packages io.delta:delta-spark_2.13:4.2.0`
  - Or place `delta-spark_2.13-4.2.0.jar` and `delta-storage-4.2.0.jar` in `$SPARK_HOME/jars/`.

### 2. Spark SQL Configuration

Spark must be configured to enable Delta SQL extensions and delegate catalog operations:

- **In `conf/spark-defaults.conf` (System-wide)**:
  ```properties
  spark.sql.extensions               io.delta.sql.DeltaSparkSessionExtension
  spark.sql.catalog.spark_catalog    org.apache.spark.sql.delta.catalog.DeltaCatalog
  ```
- **In Python Code (`SparkSession.builder`)**:
  ```python
  from pyspark.sql import SparkSession

  spark = (SparkSession.builder
      .appName("DeltaApp")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate())
  ```
- **In CLI (`spark-submit`)**:
  ```bash
  spark-submit \
    --packages io.delta:delta-spark_2.13:4.2.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    src/etl/main.py ...
  ```

---

## Hive Metastore with Remote Derby Setup

### Why Use a Remote Derby Network Server?

By default, Spark uses an **embedded Apache Derby instance** for its local Hive metastore. Embedded Derby writes to `./metastore_db/` and places an exclusive lock (`db.lck`).

**The Problem**: Only **one process** can access embedded Derby at a time. If you run a test suite, notebook, or second job simultaneously, it immediately crashes with:
```
ERROR XSDB6: Another instance of Derby may have already booted the database /.../metastore_db
```

**The Solution**: Run Apache Derby as a **locally hosted Network Server** (remoting enabled on port `1527`). Multiple Spark jobs, CLI shells, and unit test runners can connect over TCP concurrently without file lock collisions.

### Required Driver: `derbyclient.jar`

- Embedded Derby uses `derby.jar` (`org.apache.derby.jdbc.EmbeddedDriver`), which ships with standard Spark distributions.
- Remote Derby requires `org.apache.derby.jdbc.ClientDriver`, located in **`derbyclient.jar`**.
- Standard Spark distributions **do not include `derbyclient.jar`**. Download `derbyclient-10.16.1.1.jar` from Maven Central and place it into `$SPARK_HOME/jars/`:
  ```bash
  curl -o "$SPARK_HOME/jars/derbyclient-10.16.1.1.jar" \
    https://repo1.maven.org/maven2/org/apache/derby/derbyclient/10.16.1.1/derbyclient-10.16.1.1.jar
  ```

### Metastore Schema Initialization

Before starting the server, initialize the Hive metastore tables in Derby using Derby's `ij` tool and the included scripts in [`resources/`](resources/):

1. Edit [`resources/derby_ij_script.txt`](resources/derby_ij_script.txt):
   ```sql
   connect 'jdbc:derby:metastore;create=true';
   run 'derby_hive_3.2.0.sql';
   ```
2. Execute with Derby `ij`:
   ```bash
   cd resources
   $DERBY_HOME/bin/ij derby_ij_script.txt
   ```
   This creates the database schema defined in [`resources/derby_hive_3.2.0.sql`](resources/derby_hive_3.2.0.sql).

### Configuration Files

Copy the provided templates to your Spark configuration directory:

#### 1. [`conf/core-site.xml.template`](conf/core-site.xml.template)
Copy to `$SPARK_HOME/conf/core-site.xml`:
```xml
<configuration>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby://127.0.0.1:1527/metastore;create=true</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.apache.derby.jdbc.ClientDriver</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>APP</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>mine</value>
    </property>
</configuration>
```

#### 2. [`conf/spark-defaults.conf.template`](conf/spark-defaults.conf.template)
Copy to `$SPARK_HOME/conf/spark-defaults.conf`:
```properties
spark.sql.catalogImplementation  hive
spark.sql.warehouse.dir          /tmp/hive-warehouse
```

### Starting the Derby Network Server

Start the server before launching Spark:
```bash
# Linux / macOS
$DERBY_HOME/bin/startNetworkServer -h 127.0.0.1 -p 1527

# Or via Java:
java -classpath "/usr/local/derby/lib/*" org.apache.derby.drda.NetworkServerControl start -h 127.0.0.1 -p 1527
```

Verify connection:
```bash
ss -tlpn | grep 1527
```

---

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/zzhu8192/pyspark-example.git
cd pyspark-example
```

### 2. Install Dependencies
```bash
uv sync --group test
```

### 3. Run Unit Tests
```bash
uv run --group test pytest -v
```
Or with `make`:
```bash
make test
```

### 4. Build Wheel Package
```bash
uv build --wheel
```

---

## Running the Application

### Using the Included Helper Scripts

#### Linux / macOS
```bash
./run.sh
```

#### Windows
```cmd
run.bat
```

The script executes the ETL pipeline against [`example/test.csv`](example/test.csv) and writes output to temporary storage.

### Using Python CLI via `uv` Directly

```bash
uv run --group test python -m etl.main pyspark-example \
  --datalake_path "file:///tmp/pyspark-out" \
  --src_path "example/test.csv" \
  --target_path "/tmp/pyspark-out" \
  --hive_db "example_db" \
  --hive_table "example_table"
```

### Using `spark-submit`

```bash
spark-submit \
  --master "local[*]" \
  src/etl/main.py pyspark-example \
  --datalake_path "file:///tmp/pyspark-out" \
  --src_path "example/test.csv" \
  --target_path "/tmp/pyspark-out" \
  --hive_db "example_db" \
  --hive_table "example_table"
```

### Dataproc Serverless Execution Example

For managed execution on Google Cloud Dataproc Serverless:

```bash
gcloud dataproc batches submit pyspark \
  --project="your-gcp-project-id" \
  --region="us-central1" \
  --batch="pyspark-example-$(date +%s)" \
  --py-files="gs://your-bucket/etl-dist.zip" \
  gs://your-bucket/src/etl/main.py \
  -- \
  pyspark-example \
  --datalake_path="gs://your-bucket/datalake" \
  --src_path="gs://your-bucket/input/test.csv" \
  --target_path="gs://your-bucket/output/test.parquet" \
  --hive_db="analytics_db" \
  --hive_table="analytics_table"
```

---

## Running & Debugging in IDEs

### VS Code
1. Open the repository root in VS Code.
2. Select the Python interpreter located in `.venv/bin/python`.
3. In `.vscode/settings.json`, set:
   ```json
   {
     "python.testing.pytestArgs": ["tests"],
     "python.testing.pytestEnabled": true
   }
   ```

### PyCharm / IntelliJ IDEA
1. Open the `pyspark-example` folder.
2. Configure Python Interpreter -> **Existing Environment** -> `.venv/bin/python`.
3. If running tests or `main.py` directly, ensure `JAVA_HOME` is set to JDK 21.

---

## Troubleshooting & Common Errors

### 1. `ModuleNotFoundError: No module named 'pyspark'`
- **Cause**: PySpark was not installed into the active virtual environment.
- **Fix**: Run using `uv run --group test pytest` or execute `uv sync --group test`.

### 2. `IllegalAccessException: module java.base does not open java.lang to unnamed module`
- **Cause**: Strong module encapsulation in Java 17/21 prevents reflective access to internal JDK packages.
- **Fix**: Set `JAVA_OPTS` or `SPARK_SUBMIT_OPTS` to include:
  ```bash
  export SPARK_SUBMIT_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
  ```

### 3. `ERROR XSDB6: Another instance of Derby may have already booted the database`
- **Cause**: Embedded Derby metastore file lock conflict.
- **Fix**:
  - Remove stale local locks: `rm -rf metastore_db/ derby.log spark-warehouse/`
  - Or switch to the [Remote Derby Network Server setup](#hive-metastore-with-remote-derby-setup) using [`conf/core-site.xml.template`](conf/core-site.xml.template).

### 4. Windows `Could not locate executable null\bin\winutils.exe`
- **Cause**: Missing Windows Hadoop native helper binaries.
- **Fix**: Download `winutils.exe` and `hadoop.dll` from [cdarlint/winutils](https://github.com/cdarlint/winutils) and place them in `%HADOOP_HOME%\bin`.

---

## Project Structure

```text
pyspark-example/
├── .editorconfig               # Cross-editor indentation and line-ending rules
├── .gitignore                  # Git ignore rules for Python, Spark, and IDEs
├── conf/
│   ├── core-site.xml.template  # Sample configuration for remote Derby Hive Metastore
│   └── spark-defaults.conf.template # Sample Spark & Delta Lake default properties
├── resources/
│   ├── core-site.xml           # Local configuration for Derby Network Metastore
│   ├── spark-defaults.conf     # Local Spark configuration defaults
│   ├── createdb.txt            # Instructions for Derby database creation via ij
│   ├── derby_ij_script.txt     # Script to execute Hive schema DDL in Derby
│   └── derby_hive_3.2.0.sql    # Complete Hive 3.2.0 DDL schema for Derby
├── example/
│   └── test.csv                # Sample input data for local testing
├── src/
│   └── etl/
│       ├── __init__.py
│       ├── main.py             # CLI entry point using Click
│       └── jobs/
│           ├── __init__.py
│           └── example_job.py  # ETL business logic (CSV, DataFrame transforms, Hive save)
├── tests/
│   └── etl/
│       ├── conftest.py         # Pytest fixtures and shared SparkSession configuration
│       └── test_etl.py         # Unit tests covering API transforms, SQL, and Hive
├── CONTRIBUTING.md             # Contribution guidelines
├── LICENSE                     # Apache License 2.0
├── Makefile                    # Build, test, and clean automation
├── README.md                   # Project documentation
├── pyproject.toml              # PEP 621 project configuration and dependencies
├── run.sh                      # Execution script for Linux / macOS
└── run.bat                     # Execution script for Windows
```

---

## Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/zzhu8192/pyspark-example/issues). Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

This project is licensed under the **Apache License 2.0** - see the [LICENSE](LICENSE) file for details.
