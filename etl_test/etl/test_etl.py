from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType,StringType
from etl.jobs.example_job import do_transform_with_api
from etl.jobs.example_job import do_transform_with_sql
from pyspark.sql.functions import udf
from etl.test_spark_session import TestSparkSession

def test_do_transform_with_api():
    spark = TestSparkSession().getInstance()

    # arrange
    csvStr="""id|name
1|zheng
2|zhu"""
    seedData = TestSparkSession.csvStringToDf(spark, csvStr, "|")
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

def test_hive():
    spark = TestSparkSession().getInstance()
    print(" spark session: " + spark.sparkContext.version)
    csv = """id|val
    1|zz
    2|ll
    """
    df = TestSparkSession.csvStringToDf(spark, csv, "|")
    df.show()
    df.createOrReplaceTempView("test_hive")
    spark.sql("CREATE DATABASE IF NOT EXISTS unit_test")
    spark.sql("DROP TABLE IF  EXISTS unit_test.test_hive")

    spark.sql("CREATE TABLE unit_test.test_hive AS SELECT * FROM test_hive")
    df2=spark.table("unit_test.test_hive")
    assert df2.count() == 2

def test_csv():
    spark = TestSparkSession().getInstance()
    print(" spark session: " + spark.sparkContext.version)
    csv = """id|val
    1|zz
    2|ll
    """
    df = TestSparkSession.csvStringToDf(spark, csv, "|")
    df.show()
    print(f" df count: {df.count()}")
    assert True


def test_csv2():
    spark = TestSparkSession().getInstance()
    print(" spark session: " + spark.sparkContext.version)
    csv = """id|val
    1|zz
    2|ll
    3|jj
    """
    df = TestSparkSession.csvStringToDf(spark, csv, "|")
    df.show()
    print(f" df2 count: {df.count()}")
    assert True

def test_call_java():
    spark = TestSparkSession().getInstance()
    print(" spark session: " + spark.sparkContext.version)
    jvm = spark.sparkContext._jvm
#    time = jvm.java.util.System.currentTimeMillis()
    time = jvm.java.util.Random().nextInt(10)
    print(f"time: {time}")
    time2 = jvm.java.text.SimpleDateFormat("yyyyMMdd").parse("20211231")
    print(f"time2: {time2}")

## def test_hdfs():
##     spark = TestSparkSession().getInstance()
##     sc=spark.sparkContext
##     URI           = sc._gateway.jvm.java.net.URI
##     Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
##     FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
##     Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
##     fs = FileSystem.get(
##         spark.sparkContext.hadoopConfiguration)
##     status = fs.listStatus(Path('/some_dir/yet_another_one_dir/'))
##     for fileStatus in status:
##        print(fileStatus.getPath())

@udf
def wrapit(s):
    return "zz"+s+"zz"

def test_udf():
    spark = TestSparkSession().getInstance()
    spark.udf.register("wrapit", wrapit)
    csv = """id|val
    1|zz
    2|ll
    3|jj
    """
    df = TestSparkSession.csvStringToDf(spark, csv, "|")
    df.createOrReplaceTempView("df")
    spark.sql("select id, wrapit(val) from df").show()

