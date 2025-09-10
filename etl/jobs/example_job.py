from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import length
from pyspark.sql.functions import col

def do_transform_with_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("df")
    return spark.sql("select *,length(name) as len from df")


def do_transform_with_api(spark: SparkSession, df: DataFrame) -> DataFrame:
    return df.withColumn("len", length(col("name")))


def pyspark_example_etl(spark: SparkSession, src_path: str, target_path: str, hive_db: str, hive_table: str):
    print(f"-------------- doETL() reading csv {src_path} {target_path}")
    df = (spark.read.format("csv").option("header", "true")
          .option("inferSchema", "false")
          .option("delimiter", "|")
          .option("treatEmptyValuesAsNulls", True)  # allows empty fields to be null and "" to be empty string.
          .option("nullValue", "__NULL__")
          .option("quote", "\"")
          .option("escape", "\\").load(src_path))
    df.show()
    resultDf=do_transform_with_api(spark, df)
    resultDf.show()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
    resultDf.coalesce(1).write.mode("overwrite").format("parquet").option("compression", "gzip").option('path', target_path).saveAsTable(f"{hive_db}.{hive_table}")


