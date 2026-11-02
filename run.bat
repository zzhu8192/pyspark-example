@echo off
setlocal enabledelayedexpansion

REM Script to run the PySpark example ETL locally on Windows
set "PROJECT_DIR=%~dp0"
set "INPUT_CSV=%PROJECT_DIR%example\test.csv"
set "OUTPUT_DIR=%TEMP%\pyspark-example-output"
set "HIVE_DB=test_db"
set "HIVE_TABLE=test_table"

echo Cleaning previous output directory at %OUTPUT_DIR%...
if exist "%OUTPUT_DIR%" rmdir /s /q "%OUTPUT_DIR%"

where uv >nul 2>nul
if %errorlevel% equ 0 (
    echo Executing ETL job via uv...
    cd /d "%PROJECT_DIR%"
    uv run --group test python -m etl.main pyspark-example --datalake_path "file:///%OUTPUT_DIR:\=/%" --src_path "%INPUT_CSV:\=/%" --target_path "%OUTPUT_DIR:\=/%" --hive_db "%HIVE_DB%" --hive_table "%HIVE_TABLE%"
) else (
    where spark-submit >nul 2>nul
    if %errorlevel% equ 0 (
        echo Executing ETL job via spark-submit...
        spark-submit --master "local[*]" "%PROJECT_DIR%src\etl\main.py" pyspark-example --datalake_path "file:///%OUTPUT_DIR:\=/%" --src_path "%INPUT_CSV:\=/%" --target_path "%OUTPUT_DIR:\=/%" --hive_db "%HIVE_DB%" --hive_table "%HIVE_TABLE%"
    ) else (
        echo Error: neither 'uv' nor 'spark-submit' was found in PATH.
        exit /b 1
    )
)

if %errorlevel% equ 0 (
    echo ETL execution completed successfully!
) else (
    echo ETL execution failed with exit code %errorlevel%.
    exit /b %errorlevel%
)
endlocal
