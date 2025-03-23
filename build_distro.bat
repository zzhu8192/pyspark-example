REM echo off
REM #!/bin/bash
rm -rf target
mkdir target
poetry update
poetry export --without-hashes -f requirements.txt --output target/requirements.txt
pushd target
rm -rf tmp
mkdir tmp
REM #
REM # remove pyspark
REM #
REM sed -i '/pyspark/d' requirements.txt
REM sed -i '/pytest/d' requirements.txt
REM sed -i '/py4j/d' requirements.txt
pip install -r requirements.txt --target tmp
pushd tmp
cp -a ../../etl .
rm -rf *.dist-info
zip -r ../etl-dist-1.0.0.zip .
popd
REM #
REM # entrypoint
REM #
cp ../etl.main.py .
popd
