from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType,StringType
from etl.jobs.example_job import do_transform_with_api
from etl.jobs.example_job import do_transform_with_sql
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def csvStringToDf(spark: SparkSession, csv: str, delimiter: str = "|") -> DataFrame:
#        return (sparkSession.read
#                .option("header", "true")
#                .option("inferSchema", "false")
#                .option("delimiter", delimiter)
#                .option("treatEmptyValuesAsNulls", True)  # allows empty fields to be null and "" to be empty string.
#                .option("nullValue", "__NULL__")
#                .option("quote", "\"")
#                .option("escape", "\\")
#                .csv(sparkSession.sparkContext.parallelize(csv.split('\n'))))
#                .csv(StringIO(csv)))

  # Split into lines
  lines = csv.strip().splitlines()

  # Extract header and rows
  header = [p.strip() for p in lines[0].split("|")]
  rows = [[p.strip() for p in line.split("|")] for line in lines[1:]]

  # Define schema explicitly (Spark won't infer from Python lists)
  schema = StructType([StructField(p, StringType(), True) for p in header])
  return spark.createDataFrame(rows, schema=schema)



def test_do_transform_with_api(spark: SparkSession):
    # arrange
    csvStr="""id|name
1|zheng
2|zhu"""
    seedData = csvStringToDf(spark, csvStr, "|")
    seedData.show()

    #Act
    resultData = do_transform_with_api(spark, seedData)
    resultData.show()
    resultData.printSchema()

    # Assert
    actualDataArray = resultData.collect()
    assert len(actualDataArray) == 2
    assert len(resultData.schema.fields) == 3

    assert resultData.schema.fields[0].name == "id"
    assert resultData.schema.fields[1].name == "name"
    assert resultData.schema.fields[2].name == "len"

    assert actualDataArray[0]['id'] == "1"
    assert actualDataArray[0]['name'] == "zheng"
    assert actualDataArray[0]['len'] == 5

def test_hive(spark: SparkSession):
    csv = """id|val
    1|zz
    2|ll
    """
    df = csvStringToDf(spark, csv, "|")
    df.show()
    df.createOrReplaceTempView("test_hive")
    spark.sql("DROP DATABASE IF EXISTS unit_test")
    spark.sql("CREATE DATABASE IF NOT EXISTS unit_test")
    spark.sql("DROP TABLE IF  EXISTS unit_test.test_hive")

    spark.sql("CREATE TABLE unit_test.test_hive AS SELECT * FROM test_hive")
    df2=spark.table("unit_test.test_hive")
    assert df2.count() == 2

def test_csv(spark: SparkSession):
    csv = """id|val
    1|zz
    2|ll
    """
    df = csvStringToDf(spark, csv, "|")
    df.show()
    print(f" df count: {df.count()}")
    assert True


def test_csv2(spark: SparkSession):
    csv = """id|val
    1|zz
    2|ll
    3|jj
    """
    df = csvStringToDf(spark, csv, "|")
    df.show()
    print(f" df2 count: {df.count()}")
    assert True

#def test_call_java(spark: SparkSession):
#    jvm = spark.sparkContext._jvm
#    time = jvm.java.util.System.currentTimeMillis()
#    time = jvm.java.util.Random().nextInt(10)
#    print(f"time: {time}")
#    time2 = jvm.java.text.SimpleDateFormat("yyyyMMdd").parse("20211231")
#    print(f"time2: {time2}")

@udf
def wrapit(s):
    return "zz"+s+"zz"

def test_udf(spark: SparkSession):
    spark.udf.register("wrapit", wrapit)
    csv = """id|val
    1|zz
    2|ll
    3|jj
    """
    df = csvStringToDf(spark, csv, "|")
    df.createOrReplaceTempView("df2")
    spark.sql("select id, wrapit(val) from df2").show()
