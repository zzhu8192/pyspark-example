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
    
    # Read CSV with proper configuration
    df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "false")
          .option("delimiter", "|")
          .option("treatEmptyValuesAsNulls", True)
          .option("nullValue", "__NULL__")
          .option("quote", "\"")
          .option("escape", "\\")
          .load(src_path))
    
    df.show()
    
    # Apply transformation
    result_df = do_transform_with_api(spark, df)
    result_df.show()
    
    # Create database and write data
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
    
    # Write to Hive table with proper path handling
    result_df.coalesce(1).write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "gzip") \
        .saveAsTable(f"{hive_db}.{hive_table}")


