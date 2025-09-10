from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType,StringType
from pyspark.sql.functions import udf
import threading


class TestSparkSession:
    __instance = None
    __lock = threading.Lock()

    @staticmethod
    def csvStringToDf(sparkSession: SparkSession, csv: str, delimiter: str = "|") -> DataFrame:
        return (sparkSession.read
                .option("header", "true")
                .option("inferSchema", "false")
                .option("delimiter", delimiter)
                .option("treatEmptyValuesAsNulls", True)  # allows empty fields to be null and "" to be empty string.
                .option("nullValue", "__NULL__")
                .option("quote", "\"")
                .option("escape", "\\")
                .csv(sparkSession.sparkContext.parallelize(csv.split('\n'))))

    @staticmethod
    def getInstance() -> SparkSession:
        if TestSparkSession.__instance is None:
            with TestSparkSession.__lock:
                if TestSparkSession.__instance is None:
                    if TestSparkSession.__instance is not None:
                        raise Exception("This class is a singleton!")
                    else:
                        TestSparkSession.__instance = (SparkSession.builder
                                                       .master("local[1]")
                                                       .appName("test_session")
                                                       .config("spark.sql.catalogImplementation", "hive")
                                                       .config("spark.sql.parquet.compression.codec", "gzip")
                                                       .enableHiveSupport()
                                                       .getOrCreate())
        return TestSparkSession.__instance
