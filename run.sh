#!/usr/bin/env bash
set -euo pipefail

# Script to run the PySpark example ETL locally
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_CSV="${PROJECT_DIR}/example/test.csv"
OUTPUT_DIR="${TMPDIR:-/tmp}/pyspark-example-output"
HIVE_DB="test_db"
HIVE_TABLE="test_table"

echo "Cleaning previous output directory at ${OUTPUT_DIR}..."
rm -rf "${OUTPUT_DIR}"

if command -v uv &> /dev/null; then
  echo "Executing ETL job via uv..."
  cd "${PROJECT_DIR}"
  uv run --group test python -m etl.main pyspark-example \
    --datalake_path "file://${OUTPUT_DIR}" \
    --src_path "${INPUT_CSV}" \
    --target_path "${OUTPUT_DIR}" \
    --hive_db "${HIVE_DB}" \
    --hive_table "${HIVE_TABLE}"
elif command -v spark-submit &> /dev/null; then
  echo "Executing ETL job via spark-submit..."
  spark-submit \
    --master "local[*]" \
    "${PROJECT_DIR}/src/etl/main.py" \
    pyspark-example \
    --datalake_path "file://${OUTPUT_DIR}" \
    --src_path "${INPUT_CSV}" \
    --target_path "${OUTPUT_DIR}" \
    --hive_db "${HIVE_DB}" \
    --hive_table "${HIVE_TABLE}"
else
  echo "Error: neither 'uv' nor 'spark-submit' was found on PATH."
  exit 1
fi

echo "ETL execution completed successfully!"
