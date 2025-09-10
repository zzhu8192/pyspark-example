pushd target
echo Example usage: spark-submit --py-files etl-dist-1.0.0.zip etl.main.py pyspark-example --datalake_path "file://" --src_path /tmp/test.csv --target_path "/tmp/test.parquet"  --hive_db zz_test_db --hive_table zz_test_table
spark-submit --py-files etl-dist-1.0.0.zip etl.main.py %1 %2 %3 %4 %5 %6 %7 %8 %9
popd
