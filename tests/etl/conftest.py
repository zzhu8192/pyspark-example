import logging
import json
from pathlib import Path
import time
from filelock import FileLock
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Generator

import pytest
from pyspark.sql import SparkSession
from rich.logging import RichHandler
from rich.logging import RichHandler


def setup_python_path():
    """Setup Python path for project imports."""
    # Get the project root (assuming conftest.py is in tests/)
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    src_path = project_root / "src"

    # Add paths if not already present
    paths_to_add = [str(project_root), str(src_path)]
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)

    return project_root, src_path


# Setup paths at module level
PROJECT_ROOT, SRC_PATH = setup_python_path()
print(f"Project path: {PROJECT_ROOT}.  Source Path: {SRC_PATH}")


@pytest.fixture(scope="session")
def project_paths():
    """Provide project paths to tests."""
    return {"project_root": PROJECT_ROOT, "src_path": SRC_PATH}


@pytest.fixture
def logger() -> Generator[logging.Logger, None, None]:
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(name)s] %(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )

    logger = logging.getLogger()
    yield logger

CONNECT_URL = os.getenv("SPARK_CONNECT_URL", "sc://localhost:15002")
@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a single Spark session for all tests to improve performance."""
    print("Creating Spark session at date: ", datetime.now())
    # Create a single SparkSession with all necessary configurations
    spark = (
        SparkSession.builder.appName("UnitTest")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", "tmp/spark-warehouse")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.host", "localhost")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.unsafe", "true")
        .config("spark.kryoserializer.buffer", "64k")
        .config("spark.serializer.objectStreamReset", "-1")
        .config("spark.broadcast.compress", "true")
        .config("spark.rdd.compress", "true")
        .config("spark.shuffle.compress", "true")
        # spark sql tweaks
#        .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
#        .config("spark.sql.orc.compression.codec", "zstd")
#        .config("spark.sql.shuffle.partitions", "2")
#        .config("spark.sql.optimizer.maxIterations", "5")
#        .config("spark.sql.parquet.compression.codec", "zstd")
        # enable delta support
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
#        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
#        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # delta operations won't work locally without this setting
#        .config("spark.sql.sources.useV1SourceList", "avro,csv,json,kafka,orc,parquet,text,delta")
        # delta specific optimizations for tests
#        .config("spark.databricks.delta.autoCompact.enabled", "false")
#        .config("spark.databricks.delta.optimizeWrite.enabled", "false")
#        .config("spark.databricks.delta.logRetentionDuration", "0 days")
#        .config("spark.databricks.delta.deletedFileRetention", "0 days")
#        .config("spark.databricks.io.cache.enabled", "false")
 #       .config("spark.databricks.delta.snapshotPartitions", "1")
 #       .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "4")
        .enableHiveSupport()
	# spark connect
#        .remote(CONNECT_URL)
        .getOrCreate()
    )

    # Set log levels
    #spark.sparkContext.setLogLevel("WARN")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("org.apache").setLevel(logging.ERROR)

    return spark



@pytest.fixture(scope="session")
def logger() -> Generator[logging.Logger, None, None]:
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(name)s] %(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )

    logger = logging.getLogger()

    yield logger

@pytest.fixture(autouse=True, scope="session")
def cleanup_sesion():
    yield

    cleanup_all_tmp_data()


def cleanup_all_tmp_data():
    if os.path.exists("./tmp"):
        shutil.rmtree("./tmp")

    if os.path.exists("./metastore_db"):
      shutil.rmtree("./metastore_db")
