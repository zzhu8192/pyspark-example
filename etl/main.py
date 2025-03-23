import click
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import length
from pyspark.sql.functions import col
from etl.jobs.example_job import pyspark_example_etl

@click.group(help="This is the CLI for pyspark ETL project")
def cli():
    pass

def init_spark_session(app_name:str) -> SparkSession:
  spark = (SparkSession.builder
    .appName(app_name)
    .config("spark.ui.enabled", "true")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.parquet.compression.codec", "gzip")
    .enableHiveSupport()
    .getOrCreate())
  return spark

@click.command()
@click.option('--datalake_path', required=True, help='Full path for base of data lake, i.e. gs://test-datalake-poc-tmp/   Locally, e.g. /home/user/datalake')
@click.option('--env', required=False, default='dev', type=click.Choice(['test', 'dev', 'stage', 'prod'], case_sensitive=True), help='Environment')
@click.option('--is_local', is_flag=False, help='Whether running locally or in cloud')
@click.option('--local_hive_path', required=False, help='local path to hive metastore location (only required if is_local=true e.g. /home/user/hivedb ')
@click.option('--src_path', required=True, help='path to input file')
@click.option('--target_path', required=True, help='path to write target output file')
@click.option('--hive_db', required=True, help='hive db name')
@click.option('--hive_table', required=True, help='hive table name')
def pyspark_example(datalake_path: str, env: str, is_local: bool, local_hive_path: str, src_path: str, target_path: str, hive_db: str, hive_table: str):
    print(f"Running pyspark_example datalake_path: {datalake_path} env: {env} is_local: {is_local} local_hive_path: {local_hive_path} src_path: {src_path} target_path: {target_path} hive_db: {hive_db} hive_table: {hive_table}")
    spark = init_spark_session("pyspark_example")
    pyspark_example_etl(spark, src_path, target_path, hive_db, hive_table)



@click.group(help="This is the CLI for data engineering pyspark ETL.  It contains subcommands.")
def cli():
    pass

def main():
  print("starting pyspark main entry point")
  cli.add_command(pyspark_example)
  cli()

if __name__ == '__main__':
  main()
 