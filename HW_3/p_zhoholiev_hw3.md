```pyspark
print(spark.version)

```


    VBox()


    Starting Spark application



<table>
    <tr><th>ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>User</th><th>Current session?</th></tr><tr><td>2</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="https://spark-live-ui.emr-serverless.amazonaws.com" class="emr-proxy-link" emr-runtime="emr-serverless" emr-resource="00fu5aseuq7nvh1e" application-id="00ftvojq91dfuq1d">Link</a></td><td></td><td>None</td><td>✔</td></tr></table>



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    SparkSession available as 'spark'.



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    3.4.1-amzn-2


```pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType, TimestampNTZType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pprint import pprint
from pyspark.sql.functions import col, unix_timestamp, lit, hour, dayofweek, udf, when, broadcast, count, avg, sum, when, max, min
from typing import Optional, Any
from pyspark.sql import DataFrame
import boto3
import tempfile
import os
import uuid
import shutil
from enum import Enum
from pathlib import Path
from datetime import datetime, timezone
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


# TASK 2 Імпорт, уніфікація та об’єднання


```pyspark
S3_BUCKET_NAME = "robot-dreams-source-data"
YELLOW_TAXI_PATH = f"s3://{S3_BUCKET_NAME}/home-work-1/nyc_taxi/yellow/"
GREEN_TAXI_PATH = f"s3://{S3_BUCKET_NAME}/home-work-1/nyc_taxi/green/"
ZONE_LOOKUP_PATH = f"s3://{S3_BUCKET_NAME}/Lecture_3/nyc_taxi/taxi_zone_lookup.csv"

UNIFIED_YELLOW_TAXI_PATH = f"s3://{S3_BUCKET_NAME}/home-work-1-unified/nyc_taxi/yellow/"
UNIFIED_GREEN_TAXI_PATH = f"s3://{S3_BUCKET_NAME}/home-work-1-unified/nyc_taxi/green/"

print(f"Using Unified Yellow Taxi data from: {UNIFIED_YELLOW_TAXI_PATH}")
print(f"Using Unified Green Taxi data from: {YELLOW_TAXI_PATH}")
print(f"Using Yellow Taxi data from: {YELLOW_TAXI_PATH}")
print(f"Using Green Taxi data from: {GREEN_TAXI_PATH}")
print(f"Using Zone Lookup data from: {ZONE_LOOKUP_PATH}")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")

    
S3_TEMP_BUCKET = "zhogolev-pv-temp-files"

YELLOW_TEMP_FOLDER = f"s3://{S3_TEMP_BUCKET}/temp/yellow_taxi_combined_da284725/"
GREEN_TEMP_FOLDER = f"s3://{S3_TEMP_BUCKET}/temp/green_taxi_combined_da284725/"

print(f"Yellow taxi source path: {YELLOW_TAXI_PATH}")
print(f"Green taxi source path: {GREEN_TAXI_PATH}")
print(f"S3 temp bucket: {S3_TEMP_BUCKET}")
print(f"Yellow taxi S3 temp folder: {YELLOW_TEMP_FOLDER}")
print(f"Green taxi S3 temp folder: {GREEN_TEMP_FOLDER}")

temp_folders = []
temp_folders.extend([YELLOW_TEMP_FOLDER, GREEN_TEMP_FOLDER])

#CURRENT_DAY
current_date = datetime.now(timezone.utc).date().isoformat()

class TaxiTypeEnum(Enum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    Using Unified Yellow Taxi data from: s3://robot-dreams-source-data/home-work-1-unified/nyc_taxi/yellow/
    Using Unified Green Taxi data from: s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/
    Using Yellow Taxi data from: s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/
    Using Green Taxi data from: s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/
    Using Zone Lookup data from: s3://robot-dreams-source-data/Lecture_3/nyc_taxi/taxi_zone_lookup.csv
    Yellow taxi source path: s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/
    Green taxi source path: s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/
    S3 temp bucket: zhogolev-pv-temp-files
    Yellow taxi S3 temp folder: s3://zhogolev-pv-temp-files/temp/yellow_taxi_combined_da284725/
    Green taxi S3 temp folder: s3://zhogolev-pv-temp-files/temp/green_taxi_combined_da284725/


```pyspark
def show_schema(df):
    for col in df.schema:
        print(col)
        
        
def inspect_parquet_schema(path: Optional[str] = None, df: Any = None, is_read: bool = True):
    """
    Inspect the actual schema of parquet files without trying to enforce a schema
    """
    try:
        if is_read:
            df = spark.read.option("recursiveFileLookup", "true").parquet(path)
        print("Actual schema in the files:")
        schema = df.schema
        for col in schema:
            print(col)
        print("\n")
        
    except Exception as e:
        print(f"Error inspecting schema: {e}")
        return None
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


### BLOCK PROCESSING TIPES DATASETS


```pyspark

def list_files(bucket_name, s3_path):
    s3_client = boto3.client('s3')

    prefix = s3_path.replace(f"s3://{bucket_name}/", "")
    
    paginator = s3_client.get_paginator('list_objects_v2')
    parquet_files = []
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    full_path = f"s3://{bucket_name}/{obj['Key']}"
                    parquet_files.append(full_path)
    
    return sorted(parquet_files)

yellow_files = list_files(S3_BUCKET_NAME, YELLOW_TAXI_PATH)
green_files = list_files(S3_BUCKET_NAME, GREEN_TAXI_PATH)

print("YELLOW TAXI PARQUET FILES:")
print("=" * 100)
for file in yellow_files:
    print(file)

print(f"\nGREEN TAXI PARQUET FILES:")
print("=" * 100)
for file in green_files:
    print(file)

print(f"\nFile counts:")
print(f"Yellow: {len(yellow_files)}")
print(f"Green: {len(green_files)}")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    YELLOW TAXI PARQUET FILES:
    ====================================================================================================
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2015/yellow_tripdata_2015-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2016/yellow_tripdata_2016-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2017/yellow_tripdata_2017-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2018/yellow_tripdata_2018-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2020/yellow_tripdata_2020-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2022/yellow_tripdata_2022-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2023/yellow_tripdata_2023-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2023/yellow_tripdata_2023-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2023/yellow_tripdata_2023-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2023/yellow_tripdata_2023-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2023/yellow_tripdata_2023-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2023/yellow_tripdata_2023-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2023/yellow_tripdata_2023-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2023/yellow_tripdata_2023-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2023/yellow_tripdata_2023-09.parquet
    
    GREEN TAXI PARQUET FILES:
    ====================================================================================================
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2014/green_tripdata_2014-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2015/green_tripdata_2015-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2016/green_tripdata_2016-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2017/green_tripdata_2017-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2018/green_tripdata_2018-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2019/green_tripdata_2019-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2020/green_tripdata_2020-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2021/green_tripdata_2021-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-09.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-10.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-11.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2022/green_tripdata_2022-12.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2023/green_tripdata_2023-01.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2023/green_tripdata_2023-02.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2023/green_tripdata_2023-03.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2023/green_tripdata_2023-04.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2023/green_tripdata_2023-05.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2023/green_tripdata_2023-06.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2023/green_tripdata_2023-07.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2023/green_tripdata_2023-08.parquet
    s3://robot-dreams-source-data/home-work-1/nyc_taxi/green/2023/green_tripdata_2023-09.parquet
    
    File counts:
    Yellow: 92
    Green: 117
    /usr/local/lib/python3.7/site-packages/boto3/compat.py:82: PythonDeprecationWarning: Boto3 will no longer support Python 3.7 starting December 13, 2023. To continue receiving service updates, bug fixes, and security updates please upgrade to Python 3.8 or later. More information can be found here: https://aws.amazon.com/blogs/developer/python-support-policy-updates-for-aws-sdks-and-tools/
      warnings.warn(warning, PythonDeprecationWarning)


```pyspark
#Inspect yellow taxi dataset schema
inspect_parquet_schema(YELLOW_TAXI_PATH)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    Actual schema in the files:
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', LongType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', IntegerType(), True)


```pyspark
#Inspect green taxi dataset schema
inspect_parquet_schema(GREEN_TAXI_PATH)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    Actual schema in the files:
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', LongType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', IntegerType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', DoubleType(), True)
    StructField('congestion_surcharge', IntegerType(), True)


```pyspark
# Schemas for read base source files
yellow_taxi_schema_universal = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", DoubleType(), True),
    StructField("DOLocationID", DoubleType(), True),
    StructField("payment_type", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
])

green_taxi_schema_universal = StructType([
    StructField('VendorID', LongType(), True),
    StructField('lpep_pickup_datetime', TimestampNTZType(), True),
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('RatecodeID', DoubleType(), True),
    StructField('PULocationID', LongType(), True),
    StructField('DOLocationID', LongType(), True),
    StructField('passenger_count', LongType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('ehail_fee', IntegerType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('payment_type', LongType(), True),
    StructField('trip_type', DoubleType(), True),
    StructField('congestion_surcharge', IntegerType(), True)
])

#Schema read if happend some unexpected problems
yellow_taxi_schema_flexible = StructType([
    StructField("VendorID", StringType(), True),
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("passenger_count", StringType(), True),
    StructField("trip_distance", StringType(), True),
    StructField("RatecodeID", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", StringType(), True),
    StructField("DOLocationID", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", StringType(), True),
    StructField("extra", StringType(), True),
    StructField("mta_tax", StringType(), True),
    StructField("tip_amount", StringType(), True),
    StructField("tolls_amount", StringType(), True),
    StructField("improvement_surcharge", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("congestion_surcharge", StringType(), True),
    StructField("airport_fee", StringType(), True)
])

green_taxi_schema_flexible = StructType([
    StructField("VendorID", StringType(), True),
    StructField("lpep_pickup_datetime", TimestampNTZType(), True),
    StructField("lpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", StringType(), True),
    StructField("PULocationID", StringType(), True),
    StructField("DOLocationID", StringType(), True),
    StructField("passenger_count", StringType(), True),
    StructField("trip_distance", StringType(), True),
    StructField("fare_amount", StringType(), True),
    StructField("extra", StringType(), True),
    StructField("mta_tax", StringType(), True),
    StructField("tip_amount", StringType(), True),
    StructField("tolls_amount", StringType(), True),
    StructField("ehail_fee", StringType(), True),
    StructField("improvement_surcharge", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("trip_type", StringType(), True),
    StructField("congestion_surcharge", StringType(), True)
])
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
# Func read singl parquet file
def read_single_parquet_file(file_path: str, taxi_type: TaxiTypeEnum) -> DataFrame:
    """Read a single parquet file with schema enforcement"""
    try:
        df = spark.read \
            .option("mergeSchema", "true") \
            .option("inferSchema", "false") \
            .parquet(file_path)
        
        print(f" Read {os.path.basename(file_path)} without explicit schema")
        return df

    except Exception as e:
        print(f"Failed without schema: {str(e)[:80]}...")

        try:
            if taxi_type == TaxiTypeEnum.YELLOW:
                target_schema = yellow_taxi_schema_universal
            else:
                target_schema = green_taxi_schema_universal

            df = spark.read \
                .option("mergeSchema", "false") \
                .option("inferSchema", "false") \
                .schema(target_schema) \
                .parquet(file_path)

            print(f"Read {os.path.basename(file_path)} with universal schema")
            return df

        except Exception as e2:
            print(f"Failed with universal schema: {str(e2)[:80]}...")

            if taxi_type == TaxiTypeEnum.YELLOW:
                target_schema = yellow_taxi_schema_flexible
            else:
                target_schema = green_taxi_schema_flexible

            df = spark.read \
                .option("mergeSchema", "false") \
                .option("inferSchema", "false") \
                .schema(target_schema) \
                .parquet(file_path)

            print(f"Read {os.path.basename(file_path)} with flexible schema")
            return df

print("Single file reading function defined")

#Func cast types
def apply_type_casting(df: DataFrame) -> DataFrame:
    """Apply type casting to DataFrame"""
    
    casting_rules = {
        "VendorID": "long",
        "passenger_count": "long", 
        "trip_distance": "double",
        "RatecodeID": "double",
        "store_and_fwd_flag": "string",
        "PULocationID": "long",
        "DOLocationID": "long",
        "payment_type": "long",
        "fare_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tip_amount": "double",
        "tolls_amount": "double",
        "improvement_surcharge": "double",
        "total_amount": "double",
        "congestion_surcharge": "double",
        "airport_fee": "double",
        "ehail_fee": "double",
        "trip_type": "long"
    }
    
    cast_columns = []
    for column, cast_type in casting_rules.items():
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(cast_type))
            cast_columns.append(column)
    
    print(f"Applied casting to {len(cast_columns)} columns")
    return df

print("Type casting function defined")

temp_files = []

print("Save to folder function defined")

# Func clear processed files bucket.
def clear_s3_temp_folders():
    """Clean up S3 temporary folders using Spark"""
    import boto3
    
    try:
        s3_client = boto3.client('s3')
        bucket_name = S3_TEMP_BUCKET
        
        for s3_folder_path in temp_folders:
            try:
                s3_key_prefix = s3_folder_path.replace(f"s3://{bucket_name}/", "")
                
                print(f"Cleaning up S3 path: {s3_folder_path}")
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=s3_key_prefix
                )
                
                if 'Contents' in response:
                    objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                    
                    if objects_to_delete:
                        s3_client.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': objects_to_delete}
                        )
                        print(f"✓ Deleted {len(objects_to_delete)} objects from {s3_folder_path}")
                    else:
                        print(f"✓ No objects found in {s3_folder_path}")
                else:
                    print(f"✓ Folder {s3_folder_path} is already empty or doesn't exist")
                    
            except Exception as e:
                print(f"⚠ Error cleaning up {s3_folder_path}: {e}")
                
        temp_folders.clear()
        print("S3 cleanup completed!")
        
    except Exception as e:
        print(f"✗ Error during S3 cleanup: {e}")
        print("You may need to clean up manually using AWS console or CLI")

```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    Single file reading function defined
    Type casting function defined
    Save to folder function defined


```pyspark
#Processing base source file one by one
def process_files_one_by_one_to_s3(file_paths: list, taxi_type: TaxiTypeEnum, target_s3_folder: str) -> dict:
    """Process files one by one and save each to S3 directly in the main folder"""
    
    total_files = len(file_paths)
    successful_files = 0
    failed_files = []
    processed_files_info = []
    
    print(f"Processing {total_files} {taxi_type.value} files one by one to S3...")
    print(f"Target S3 folder: {target_s3_folder}")
    
    for i, file_path in enumerate(file_paths, 1):
        try:
            print(f"[{i}/{total_files}] Processing: {os.path.basename(file_path)}")

            df = read_single_parquet_file(file_path, taxi_type)
 
            df = apply_type_casting(df)

            record_count = df.count()
        
            source_filename = os.path.basename(file_path).replace('.parquet', '').replace('_', '-')
            subfolder_name = f"{taxi_type.value.lower()}-{i:04d}-{source_filename}"
            subfolder_path = f"{target_s3_folder}{subfolder_name}/"
            
            df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(subfolder_path)
            
            show_schema(df)
            
            successful_files += 1
            processed_files_info.append({
                'file_number': i,
                'source_file': os.path.basename(file_path),
                'record_count': record_count
            })
            
            print(f"Processed {record_count:,} records")

            del df
            
        except Exception as e:
            print(f"Failed: {str(e)[:150]}...")
            failed_files.append({
                'file_path': file_path,
                'error': str(e)
            })
            continue
    
    total_records = sum(info['record_count'] for info in processed_files_info)
    
    print(f"\n{taxi_type.value} PROCESSING SUMMARY:")
    print(f" Successfully processed: {successful_files}/{total_files} files")
    print(f" Total records processed: {total_records:,}")
    print(f" S3 folder: {target_s3_folder}")
    print(f" Failed files: {len(failed_files)}")
    
    if failed_files:
        print(f"\n Failed files:")
        for failed in failed_files[:5]:  # Show first 5 failures
            print(f"    - {os.path.basename(failed['file_path'])}: {failed['error'][:100]}...")
    
    return {
        'taxi_type': taxi_type.value,
        'successful_files': successful_files,
        'total_files': total_files,
        'total_records': total_records,
        'failed_files': failed_files,
        'processed_files_info': processed_files_info,
        's3_folder': target_s3_folder
    }

print("File-by-file processing function defined")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    File-by-file processing function defined


```pyspark
# FOR TEST ONLY. One file processing
test_read = []
file_path_test = yellow_files[0]
test_read.append(file_path_test)
print(test_read)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    ['s3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-01.parquet']


```pyspark
#Processing yellow taxi dataset
yellow_results = None

if yellow_files:
    print("=" * 70)
    print("PROCESSING YELLOW TAXI DATA - FILE BY FILE TO S3")
    print("=" * 70)
    
    
    try:
        yellow_results = process_files_one_by_one_to_s3(
            yellow_files, 
            TaxiTypeEnum.YELLOW, 
            YELLOW_TEMP_FOLDER
        )
        
        print(f"YELLOW TAXI COMPLETED:")
        print(f"Files processed: {yellow_results['successful_files']}/{yellow_results['total_files']}")
        print(f"Total records: {yellow_results['total_records']:,}")
        print(f"S3 location: {yellow_results['s3_folder']}")
        
    except Exception as e:
        print(f"✗ Error in yellow taxi processing: {e}")
        yellow_results = {'error': str(e)}
else:
    print("No yellow taxi files to process")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    ======================================================================
    PROCESSING YELLOW TAXI DATA - FILE BY FILE TO S3
    ======================================================================
    🚀 Processing 92 YELLOW files one by one to S3...
    📁 Target S3 folder: s3://zhogolev-pv-temp-files/temp/yellow_taxi_combined_da284725/
    
    [1/92] Processing: yellow_tripdata_2014-01.parquet
        ✓ Read yellow_tripdata_2014-01.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 13,782,517 records
    
    [2/92] Processing: yellow_tripdata_2014-02.parquet
        ✓ Read yellow_tripdata_2014-02.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 13,063,794 records
    
    [3/92] Processing: yellow_tripdata_2014-03.parquet
        ✓ Read yellow_tripdata_2014-03.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 15,428,134 records
    
    [4/92] Processing: yellow_tripdata_2014-04.parquet
        ✓ Read yellow_tripdata_2014-04.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 14,618,759 records
    
    [5/92] Processing: yellow_tripdata_2014-05.parquet
        ✓ Read yellow_tripdata_2014-05.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 14,774,048 records
    
    [6/92] Processing: yellow_tripdata_2014-06.parquet
        ✓ Read yellow_tripdata_2014-06.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 13,813,079 records
    
    [7/92] Processing: yellow_tripdata_2014-07.parquet
        ✓ Read yellow_tripdata_2014-07.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 13,104,273 records
    
    [8/92] Processing: yellow_tripdata_2014-08.parquet
        ✓ Read yellow_tripdata_2014-08.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 12,698,743 records
    
    [9/92] Processing: yellow_tripdata_2014-09.parquet
        ✓ Read yellow_tripdata_2014-09.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 13,424,350 records
    
    [10/92] Processing: yellow_tripdata_2014-10.parquet
        ✓ Read yellow_tripdata_2014-10.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 14,317,774 records
    
    [11/92] Processing: yellow_tripdata_2014-11.parquet
        ✓ Read yellow_tripdata_2014-11.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 13,309,991 records
    
    [12/92] Processing: yellow_tripdata_2014-12.parquet
        ✓ Read yellow_tripdata_2014-12.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 13,112,117 records
    
    [13/92] Processing: yellow_tripdata_2015-01.parquet
        ✓ Read yellow_tripdata_2015-01.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 12,741,035 records
    
    [14/92] Processing: yellow_tripdata_2015-02.parquet
        ✓ Read yellow_tripdata_2015-02.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 12,442,394 records
    
    [15/92] Processing: yellow_tripdata_2015-03.parquet
        ✓ Read yellow_tripdata_2015-03.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 13,342,951 records
    
    [16/92] Processing: yellow_tripdata_2015-04.parquet
        ✓ Read yellow_tripdata_2015-04.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 13,063,758 records
    
    [17/92] Processing: yellow_tripdata_2015-05.parquet
        ✓ Read yellow_tripdata_2015-05.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 13,157,677 records
    
    [18/92] Processing: yellow_tripdata_2015-06.parquet
        ✓ Read yellow_tripdata_2015-06.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 12,324,936 records
    
    [19/92] Processing: yellow_tripdata_2015-07.parquet
        ✓ Read yellow_tripdata_2015-07.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 11,559,666 records
    
    [20/92] Processing: yellow_tripdata_2015-08.parquet
        ✓ Read yellow_tripdata_2015-08.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 11,123,123 records
    
    [21/92] Processing: yellow_tripdata_2015-09.parquet
        ✓ Read yellow_tripdata_2015-09.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 11,218,122 records
    
    [22/92] Processing: yellow_tripdata_2015-10.parquet
        ✓ Read yellow_tripdata_2015-10.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 12,307,333 records
    
    [23/92] Processing: yellow_tripdata_2015-11.parquet
        ✓ Read yellow_tripdata_2015-11.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 11,305,240 records
    
    [24/92] Processing: yellow_tripdata_2015-12.parquet
        ✓ Read yellow_tripdata_2015-12.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 11,452,996 records
    
    [25/92] Processing: yellow_tripdata_2016-01.parquet
        ✓ Read yellow_tripdata_2016-01.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 10,905,067 records
    
    [26/92] Processing: yellow_tripdata_2016-02.parquet
        ✓ Read yellow_tripdata_2016-02.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 11,375,412 records
    
    [27/92] Processing: yellow_tripdata_2016-03.parquet
        ✓ Read yellow_tripdata_2016-03.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 12,203,824 records
    
    [28/92] Processing: yellow_tripdata_2016-04.parquet
        ✓ Read yellow_tripdata_2016-04.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 11,927,996 records
    
    [29/92] Processing: yellow_tripdata_2016-05.parquet
        ✓ Read yellow_tripdata_2016-05.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 11,832,049 records
    
    [30/92] Processing: yellow_tripdata_2016-06.parquet
        ✓ Read yellow_tripdata_2016-06.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 11,131,645 records
    
    [31/92] Processing: yellow_tripdata_2016-07.parquet
        ✓ Read yellow_tripdata_2016-07.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 10,294,080 records
    
    [32/92] Processing: yellow_tripdata_2016-08.parquet
        ✓ Read yellow_tripdata_2016-08.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 9,942,263 records
    
    [33/92] Processing: yellow_tripdata_2016-09.parquet
        ✓ Read yellow_tripdata_2016-09.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 10,116,018 records
    
    [34/92] Processing: yellow_tripdata_2016-10.parquet
        ✓ Read yellow_tripdata_2016-10.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 10,854,626 records
    
    [35/92] Processing: yellow_tripdata_2016-11.parquet
        ✓ Read yellow_tripdata_2016-11.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 10,102,128 records
    
    [36/92] Processing: yellow_tripdata_2016-12.parquet
        ✓ Read yellow_tripdata_2016-12.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 10,446,697 records
    
    [37/92] Processing: yellow_tripdata_2017-01.parquet
        ✓ Read yellow_tripdata_2017-01.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 9,710,820 records
    
    [38/92] Processing: yellow_tripdata_2017-02.parquet
        ✓ Read yellow_tripdata_2017-02.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 9,169,775 records
    
    [39/92] Processing: yellow_tripdata_2017-03.parquet
        ✓ Read yellow_tripdata_2017-03.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 10,295,441 records
    
    [40/92] Processing: yellow_tripdata_2017-04.parquet
        ✓ Read yellow_tripdata_2017-04.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 10,047,135 records
    
    [41/92] Processing: yellow_tripdata_2017-05.parquet
        ✓ Read yellow_tripdata_2017-05.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 10,102,127 records
    
    [42/92] Processing: yellow_tripdata_2017-06.parquet
        ✓ Read yellow_tripdata_2017-06.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 9,656,993 records
    
    [43/92] Processing: yellow_tripdata_2017-07.parquet
        ✓ Read yellow_tripdata_2017-07.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 8,588,486 records
    
    [44/92] Processing: yellow_tripdata_2017-08.parquet
        ✓ Read yellow_tripdata_2017-08.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 8,422,153 records
    
    [45/92] Processing: yellow_tripdata_2017-09.parquet
        ✓ Read yellow_tripdata_2017-09.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 8,945,421 records
    
    [46/92] Processing: yellow_tripdata_2017-10.parquet
        ✓ Read yellow_tripdata_2017-10.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 9,768,672 records
    
    [47/92] Processing: yellow_tripdata_2017-11.parquet
        ✓ Read yellow_tripdata_2017-11.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 9,284,803 records
    
    [48/92] Processing: yellow_tripdata_2017-12.parquet
        ✓ Read yellow_tripdata_2017-12.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 9,508,501 records
    
    [49/92] Processing: yellow_tripdata_2018-01.parquet
        ✓ Read yellow_tripdata_2018-01.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 8,760,687 records
    
    [50/92] Processing: yellow_tripdata_2018-02.parquet
        ✓ Read yellow_tripdata_2018-02.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 8,492,819 records
    
    [51/92] Processing: yellow_tripdata_2018-03.parquet
        ✓ Read yellow_tripdata_2018-03.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 9,431,289 records
    
    [52/92] Processing: yellow_tripdata_2018-04.parquet
        ✓ Read yellow_tripdata_2018-04.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 9,306,216 records
    
    [53/92] Processing: yellow_tripdata_2018-05.parquet
        ✓ Read yellow_tripdata_2018-05.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 9,224,788 records
    
    [54/92] Processing: yellow_tripdata_2018-06.parquet
        ✓ Read yellow_tripdata_2018-06.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 8,714,667 records
    
    [55/92] Processing: yellow_tripdata_2018-07.parquet
        ✓ Read yellow_tripdata_2018-07.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 7,851,143 records
    
    [56/92] Processing: yellow_tripdata_2018-08.parquet
        ✓ Read yellow_tripdata_2018-08.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 7,855,040 records
    
    [57/92] Processing: yellow_tripdata_2018-09.parquet
        ✓ Read yellow_tripdata_2018-09.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 8,049,094 records
    
    [58/92] Processing: yellow_tripdata_2018-10.parquet
        ✓ Read yellow_tripdata_2018-10.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 8,834,520 records
    
    [59/92] Processing: yellow_tripdata_2018-11.parquet
        ✓ Read yellow_tripdata_2018-11.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 8,155,449 records
    
    [60/92] Processing: yellow_tripdata_2018-12.parquet
        ✓ Read yellow_tripdata_2018-12.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 8,195,675 records
    
    [61/92] Processing: yellow_tripdata_2020-01.parquet
        ✓ Read yellow_tripdata_2020-01.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 6,405,008 records
    
    [62/92] Processing: yellow_tripdata_2020-03.parquet
        ✓ Read yellow_tripdata_2020-03.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,007,687 records
    
    [63/92] Processing: yellow_tripdata_2020-04.parquet
        ✓ Read yellow_tripdata_2020-04.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 238,073 records
    
    [64/92] Processing: yellow_tripdata_2020-05.parquet
        ✓ Read yellow_tripdata_2020-05.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 348,415 records
    
    [65/92] Processing: yellow_tripdata_2020-06.parquet
        ✓ Read yellow_tripdata_2020-06.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 549,797 records
    
    [66/92] Processing: yellow_tripdata_2020-07.parquet
        ✓ Read yellow_tripdata_2020-07.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 800,412 records
    
    [67/92] Processing: yellow_tripdata_2020-08.parquet
        ✓ Read yellow_tripdata_2020-08.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 1,007,286 records
    
    [68/92] Processing: yellow_tripdata_2020-09.parquet
        ✓ Read yellow_tripdata_2020-09.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 1,341,017 records
    
    [69/92] Processing: yellow_tripdata_2020-10.parquet
        ✓ Read yellow_tripdata_2020-10.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 1,681,132 records
    
    [70/92] Processing: yellow_tripdata_2020-11.parquet
        ✓ Read yellow_tripdata_2020-11.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 1,509,000 records
    
    [71/92] Processing: yellow_tripdata_2020-12.parquet
        ✓ Read yellow_tripdata_2020-12.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 1,461,898 records
    
    [72/92] Processing: yellow_tripdata_2022-01.parquet
        ✓ Read yellow_tripdata_2022-01.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 2,463,931 records
    
    [73/92] Processing: yellow_tripdata_2022-02.parquet
        ✓ Read yellow_tripdata_2022-02.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 2,979,431 records
    
    [74/92] Processing: yellow_tripdata_2022-03.parquet
        ✓ Read yellow_tripdata_2022-03.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,627,882 records
    
    [75/92] Processing: yellow_tripdata_2022-04.parquet
        ✓ Read yellow_tripdata_2022-04.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,599,920 records
    
    [76/92] Processing: yellow_tripdata_2022-05.parquet
        ✓ Read yellow_tripdata_2022-05.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,588,295 records
    
    [77/92] Processing: yellow_tripdata_2022-06.parquet
        ✓ Read yellow_tripdata_2022-06.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,558,124 records
    
    [78/92] Processing: yellow_tripdata_2022-07.parquet
        ✓ Read yellow_tripdata_2022-07.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,174,394 records
    
    [79/92] Processing: yellow_tripdata_2022-08.parquet
        ✓ Read yellow_tripdata_2022-08.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,152,677 records
    
    [80/92] Processing: yellow_tripdata_2022-09.parquet
        ✓ Read yellow_tripdata_2022-09.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,183,767 records
    
    [81/92] Processing: yellow_tripdata_2022-10.parquet
        ✓ Read yellow_tripdata_2022-10.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,675,411 records
    
    [82/92] Processing: yellow_tripdata_2022-11.parquet
        ✓ Read yellow_tripdata_2022-11.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,252,717 records
    
    [83/92] Processing: yellow_tripdata_2022-12.parquet
        ✓ Read yellow_tripdata_2022-12.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,399,549 records
    
    [84/92] Processing: yellow_tripdata_2023-01.parquet
        ✓ Read yellow_tripdata_2023-01.parquet without explicit schema
        ✓ Applied casting to 16 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
        ✓ Processed 3,066,766 records
    
    [85/92] Processing: yellow_tripdata_2023-02.parquet
        ✓ Read yellow_tripdata_2023-02.parquet without explicit schema
        ✓ Applied casting to 15 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('Airport_fee', DoubleType(), True)
        ✓ Processed 2,913,955 records
    
    [86/92] Processing: yellow_tripdata_2023-03.parquet
        ✓ Read yellow_tripdata_2023-03.parquet without explicit schema
        ✓ Applied casting to 15 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('Airport_fee', DoubleType(), True)
        ✓ Processed 3,403,766 records
    
    [87/92] Processing: yellow_tripdata_2023-04.parquet
        ✓ Read yellow_tripdata_2023-04.parquet without explicit schema
        ✓ Applied casting to 15 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('Airport_fee', DoubleType(), True)
        ✓ Processed 3,288,250 records
    
    [88/92] Processing: yellow_tripdata_2023-05.parquet
        ✓ Read yellow_tripdata_2023-05.parquet without explicit schema
        ✓ Applied casting to 15 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('Airport_fee', DoubleType(), True)
        ✓ Processed 3,513,649 records
    
    [89/92] Processing: yellow_tripdata_2023-06.parquet
        ✓ Read yellow_tripdata_2023-06.parquet without explicit schema
        ✓ Applied casting to 15 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('Airport_fee', DoubleType(), True)
        ✓ Processed 3,307,234 records
    
    [90/92] Processing: yellow_tripdata_2023-07.parquet
        ✓ Read yellow_tripdata_2023-07.parquet without explicit schema
        ✓ Applied casting to 15 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('Airport_fee', DoubleType(), True)
        ✓ Processed 2,907,108 records
    
    [91/92] Processing: yellow_tripdata_2023-08.parquet
        ✓ Read yellow_tripdata_2023-08.parquet without explicit schema
        ✓ Applied casting to 15 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('Airport_fee', DoubleType(), True)
        ✓ Processed 2,824,209 records
    
    [92/92] Processing: yellow_tripdata_2023-09.parquet
        ✓ Read yellow_tripdata_2023-09.parquet without explicit schema
        ✓ Applied casting to 15 columns
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('Airport_fee', DoubleType(), True)
        ✓ Processed 2,846,722 records
    
    📊 YELLOW PROCESSING SUMMARY:
        ✅ Successfully processed: 92/92 files
        📈 Total records processed: 745,067,811
        📁 S3 folder: s3://zhogolev-pv-temp-files/temp/yellow_taxi_combined_da284725/
        ❌ Failed files: 0
    
    🟡 YELLOW TAXI COMPLETED:
       ✅ Files processed: 92/92
       📊 Total records: 745,067,811
       📁 S3 location: s3://zhogolev-pv-temp-files/temp/yellow_taxi_combined_da284725/


```pyspark
#Processing green taxi dataset
green_results = None

if green_files:
    print("=" * 70)
    print("PROCESSING GREEN TAXI DATA - FILE BY FILE TO S3")
    print("=" * 70)
    
    try:
        green_results = process_files_one_by_one_to_s3(
            green_files, 
            TaxiTypeEnum.GREEN, 
            GREEN_TEMP_FOLDER
        )
        
        print(f"GREEN TAXI COMPLETED:")
        print(f"Files processed: {green_results['successful_files']}/{green_results['total_files']}")
        print(f"Total records: {green_results['total_records']:,}")
        print(f"S3 location: {green_results['s3_folder']}")
        
    except Exception as e:
        print(f"✗ Error in green taxi processing: {e}")
        green_results = {'error': str(e)}
else:
    print("No green taxi files to process")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    Exception in thread cell_monitor-75:
    Traceback (most recent call last):
      File "/usr/lib64/python3.7/threading.py", line 926, in _bootstrap_inner
        self.run()
      File "/usr/lib64/python3.7/threading.py", line 870, in run
        self._target(*self._args, **self._kwargs)
      File "/home/hadoop/spark-magic/spark_monitoring_widget/src/awseditorssparkmonitoringwidget/cellmonitor.py", line 180, in cell_monitor
        job_binned_stages[job_id][stage_id] = all_stages[stage_id]
    KeyError: 1296
    


    ======================================================================
    PROCESSING GREEN TAXI DATA - FILE BY FILE TO S3
    ======================================================================
    🚀 Processing 117 GREEN files one by one to S3...
    📁 Target S3 folder: s3://zhogolev-pv-temp-files/temp/green_taxi_combined_da284725/
    
    [1/117] Processing: green_tripdata_2014-01.parquet
        ✓ Read green_tripdata_2014-01.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 803,860 records
    
    [2/117] Processing: green_tripdata_2014-02.parquet
        ✓ Read green_tripdata_2014-02.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,005,242 records
    
    [3/117] Processing: green_tripdata_2014-03.parquet
        ✓ Read green_tripdata_2014-03.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,293,471 records
    
    [4/117] Processing: green_tripdata_2014-04.parquet
        ✓ Read green_tripdata_2014-04.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,309,155 records
    
    [5/117] Processing: green_tripdata_2014-05.parquet
        ✓ Read green_tripdata_2014-05.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,421,503 records
    
    [6/117] Processing: green_tripdata_2014-06.parquet
        ✓ Read green_tripdata_2014-06.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,337,759 records
    
    [7/117] Processing: green_tripdata_2014-07.parquet
        ✓ Read green_tripdata_2014-07.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,273,973 records
    
    [8/117] Processing: green_tripdata_2014-08.parquet
        ✓ Read green_tripdata_2014-08.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,344,941 records
    
    [9/117] Processing: green_tripdata_2014-09.parquet
        ✓ Read green_tripdata_2014-09.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,361,893 records
    
    [10/117] Processing: green_tripdata_2014-10.parquet
        ✓ Read green_tripdata_2014-10.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,491,266 records
    
    [11/117] Processing: green_tripdata_2014-11.parquet
        ✓ Read green_tripdata_2014-11.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,548,159 records
    
    [12/117] Processing: green_tripdata_2014-12.parquet
        ✓ Read green_tripdata_2014-12.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,645,787 records
    
    [13/117] Processing: green_tripdata_2015-01.parquet
        ✓ Read green_tripdata_2015-01.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,508,493 records
    
    [14/117] Processing: green_tripdata_2015-02.parquet
        ✓ Read green_tripdata_2015-02.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,574,830 records
    
    [15/117] Processing: green_tripdata_2015-03.parquet
        ✓ Read green_tripdata_2015-03.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,722,574 records
    
    [16/117] Processing: green_tripdata_2015-04.parquet
        ✓ Read green_tripdata_2015-04.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,664,394 records
    
    [17/117] Processing: green_tripdata_2015-05.parquet
        ✓ Read green_tripdata_2015-05.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,786,848 records
    
    [18/117] Processing: green_tripdata_2015-06.parquet
        ✓ Read green_tripdata_2015-06.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,638,868 records
    
    [19/117] Processing: green_tripdata_2015-07.parquet
        ✓ Read green_tripdata_2015-07.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,541,671 records
    
    [20/117] Processing: green_tripdata_2015-08.parquet
        ✓ Read green_tripdata_2015-08.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,532,343 records
    
    [21/117] Processing: green_tripdata_2015-09.parquet
        ✓ Read green_tripdata_2015-09.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,494,927 records
    
    [22/117] Processing: green_tripdata_2015-10.parquet
        ✓ Read green_tripdata_2015-10.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,630,536 records
    
    [23/117] Processing: green_tripdata_2015-11.parquet
        ✓ Read green_tripdata_2015-11.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,529,984 records
    
    [24/117] Processing: green_tripdata_2015-12.parquet
        ✓ Read green_tripdata_2015-12.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,608,297 records
    
    [25/117] Processing: green_tripdata_2016-01.parquet
        ✓ Read green_tripdata_2016-01.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,445,292 records
    
    [26/117] Processing: green_tripdata_2016-02.parquet
        ✓ Read green_tripdata_2016-02.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,510,722 records
    
    [27/117] Processing: green_tripdata_2016-03.parquet
        ✓ Read green_tripdata_2016-03.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,576,393 records
    
    [28/117] Processing: green_tripdata_2016-04.parquet
        ✓ Read green_tripdata_2016-04.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,543,926 records
    
    [29/117] Processing: green_tripdata_2016-05.parquet
        ✓ Read green_tripdata_2016-05.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,536,979 records
    
    [30/117] Processing: green_tripdata_2016-06.parquet
        ✓ Read green_tripdata_2016-06.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,404,727 records
    
    [31/117] Processing: green_tripdata_2016-07.parquet
        ✓ Read green_tripdata_2016-07.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,332,510 records
    
    [32/117] Processing: green_tripdata_2016-08.parquet
        ✓ Read green_tripdata_2016-08.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,247,675 records
    
    [33/117] Processing: green_tripdata_2016-09.parquet
        ✓ Read green_tripdata_2016-09.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,162,373 records
    
    [34/117] Processing: green_tripdata_2016-10.parquet
        ✓ Read green_tripdata_2016-10.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,252,572 records
    
    [35/117] Processing: green_tripdata_2016-11.parquet
        ✓ Read green_tripdata_2016-11.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,148,214 records
    
    [36/117] Processing: green_tripdata_2016-12.parquet
        ✓ Read green_tripdata_2016-12.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,224,158 records
    
    [37/117] Processing: green_tripdata_2017-01.parquet
        ✓ Read green_tripdata_2017-01.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,069,565 records
    
    [38/117] Processing: green_tripdata_2017-02.parquet
        ✓ Read green_tripdata_2017-02.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,022,313 records
    
    [39/117] Processing: green_tripdata_2017-03.parquet
        ✓ Read green_tripdata_2017-03.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,157,827 records
    
    [40/117] Processing: green_tripdata_2017-04.parquet
        ✓ Read green_tripdata_2017-04.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,080,844 records
    
    [41/117] Processing: green_tripdata_2017-05.parquet
        ✓ Read green_tripdata_2017-05.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 1,059,463 records
    
    [42/117] Processing: green_tripdata_2017-06.parquet
        ✓ Read green_tripdata_2017-06.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 976,467 records
    
    [43/117] Processing: green_tripdata_2017-07.parquet
        ✓ Read green_tripdata_2017-07.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 914,783 records
    
    [44/117] Processing: green_tripdata_2017-08.parquet
        ✓ Read green_tripdata_2017-08.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 867,407 records
    
    [45/117] Processing: green_tripdata_2017-09.parquet
        ✓ Read green_tripdata_2017-09.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 882,464 records
    
    [46/117] Processing: green_tripdata_2017-10.parquet
        ✓ Read green_tripdata_2017-10.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 925,737 records
    
    [47/117] Processing: green_tripdata_2017-11.parquet
        ✓ Read green_tripdata_2017-11.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 874,173 records
    
    [48/117] Processing: green_tripdata_2017-12.parquet
        ✓ Read green_tripdata_2017-12.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 906,016 records
    
    [49/117] Processing: green_tripdata_2018-01.parquet
        ✓ Read green_tripdata_2018-01.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 792,744 records
    
    [50/117] Processing: green_tripdata_2018-02.parquet
        ✓ Read green_tripdata_2018-02.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 769,197 records
    
    [51/117] Processing: green_tripdata_2018-03.parquet
        ✓ Read green_tripdata_2018-03.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 836,246 records
    
    [52/117] Processing: green_tripdata_2018-04.parquet
        ✓ Read green_tripdata_2018-04.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 799,383 records
    
    [53/117] Processing: green_tripdata_2018-05.parquet
        ✓ Read green_tripdata_2018-05.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 796,552 records
    
    [54/117] Processing: green_tripdata_2018-06.parquet
        ✓ Read green_tripdata_2018-06.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 738,546 records
    
    [55/117] Processing: green_tripdata_2018-07.parquet
        ✓ Read green_tripdata_2018-07.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 684,374 records
    
    [56/117] Processing: green_tripdata_2018-08.parquet
        ✓ Read green_tripdata_2018-08.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 675,815 records
    
    [57/117] Processing: green_tripdata_2018-09.parquet
        ✓ Read green_tripdata_2018-09.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 682,032 records
    
    [58/117] Processing: green_tripdata_2018-10.parquet
        ✓ Read green_tripdata_2018-10.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 731,888 records
    
    [59/117] Processing: green_tripdata_2018-11.parquet
        ✓ Read green_tripdata_2018-11.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 673,287 records
    
    [60/117] Processing: green_tripdata_2018-12.parquet
        ✓ Read green_tripdata_2018-12.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 719,654 records
    
    [61/117] Processing: green_tripdata_2019-01.parquet
        ✓ Read green_tripdata_2019-01.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 672,105 records
    
    [62/117] Processing: green_tripdata_2019-02.parquet
        ✓ Read green_tripdata_2019-02.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 615,594 records
    
    [63/117] Processing: green_tripdata_2019-03.parquet
        ✓ Read green_tripdata_2019-03.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 643,063 records
    
    [64/117] Processing: green_tripdata_2019-04.parquet
        ✓ Read green_tripdata_2019-04.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 567,852 records
    
    [65/117] Processing: green_tripdata_2019-05.parquet
        ✓ Read green_tripdata_2019-05.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 545,452 records
    
    [66/117] Processing: green_tripdata_2019-06.parquet
        ✓ Read green_tripdata_2019-06.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 506,238 records
    
    [67/117] Processing: green_tripdata_2019-07.parquet
        ✓ Read green_tripdata_2019-07.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 470,743 records
    
    [68/117] Processing: green_tripdata_2019-08.parquet
        ✓ Read green_tripdata_2019-08.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 449,695 records
    
    [69/117] Processing: green_tripdata_2019-09.parquet
        ✓ Read green_tripdata_2019-09.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 449,063 records
    
    [70/117] Processing: green_tripdata_2019-10.parquet
        ✓ Read green_tripdata_2019-10.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 476,386 records
    
    [71/117] Processing: green_tripdata_2019-11.parquet
        ✓ Read green_tripdata_2019-11.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 449,500 records
    
    [72/117] Processing: green_tripdata_2019-12.parquet
        ✓ Read green_tripdata_2019-12.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 455,294 records
    
    [73/117] Processing: green_tripdata_2020-01.parquet
        ✓ Read green_tripdata_2020-01.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 447,770 records
    
    [74/117] Processing: green_tripdata_2020-02.parquet
        ✓ Read green_tripdata_2020-02.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 398,632 records
    
    [75/117] Processing: green_tripdata_2020-03.parquet
        ✓ Read green_tripdata_2020-03.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 223,496 records
    
    [76/117] Processing: green_tripdata_2020-04.parquet
        ✓ Read green_tripdata_2020-04.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 35,644 records
    
    [77/117] Processing: green_tripdata_2020-05.parquet
        ✓ Read green_tripdata_2020-05.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 57,361 records
    
    [78/117] Processing: green_tripdata_2020-06.parquet
        ✓ Read green_tripdata_2020-06.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 63,110 records
    
    [79/117] Processing: green_tripdata_2020-07.parquet
        ✓ Read green_tripdata_2020-07.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 72,258 records
    
    [80/117] Processing: green_tripdata_2020-08.parquet
        ✓ Read green_tripdata_2020-08.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 81,063 records
    
    [81/117] Processing: green_tripdata_2020-09.parquet
        ✓ Read green_tripdata_2020-09.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 87,987 records
    
    [82/117] Processing: green_tripdata_2020-10.parquet
        ✓ Read green_tripdata_2020-10.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 95,120 records
    
    [83/117] Processing: green_tripdata_2020-11.parquet
        ✓ Read green_tripdata_2020-11.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 88,605 records
    
    [84/117] Processing: green_tripdata_2020-12.parquet
        ✓ Read green_tripdata_2020-12.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 83,130 records
    
    [85/117] Processing: green_tripdata_2021-01.parquet
        ✓ Read green_tripdata_2021-01.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 76,518 records
    
    [86/117] Processing: green_tripdata_2021-02.parquet
        ✓ Read green_tripdata_2021-02.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 64,572 records
    
    [87/117] Processing: green_tripdata_2021-03.parquet
        ✓ Read green_tripdata_2021-03.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 83,827 records
    
    [88/117] Processing: green_tripdata_2021-04.parquet
        ✓ Read green_tripdata_2021-04.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 86,941 records
    
    [89/117] Processing: green_tripdata_2021-05.parquet
        ✓ Read green_tripdata_2021-05.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 88,180 records
    
    [90/117] Processing: green_tripdata_2021-06.parquet
        ✓ Read green_tripdata_2021-06.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 86,737 records
    
    [91/117] Processing: green_tripdata_2021-07.parquet
        ✓ Read green_tripdata_2021-07.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 83,691 records
    
    [92/117] Processing: green_tripdata_2021-08.parquet
        ✓ Read green_tripdata_2021-08.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 83,499 records
    
    [93/117] Processing: green_tripdata_2021-09.parquet
        ✓ Read green_tripdata_2021-09.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 95,709 records
    
    [94/117] Processing: green_tripdata_2021-10.parquet
        ✓ Read green_tripdata_2021-10.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 110,891 records
    
    [95/117] Processing: green_tripdata_2021-11.parquet
        ✓ Read green_tripdata_2021-11.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 108,229 records
    
    [96/117] Processing: green_tripdata_2021-12.parquet
        ✓ Read green_tripdata_2021-12.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 99,961 records
    
    [97/117] Processing: green_tripdata_2022-01.parquet
        ✓ Read green_tripdata_2022-01.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 62,495 records
    
    [98/117] Processing: green_tripdata_2022-02.parquet
        ✓ Read green_tripdata_2022-02.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 69,399 records
    
    [99/117] Processing: green_tripdata_2022-03.parquet
        ✓ Read green_tripdata_2022-03.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 78,537 records
    
    [100/117] Processing: green_tripdata_2022-04.parquet
        ✓ Read green_tripdata_2022-04.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 76,136 records
    
    [101/117] Processing: green_tripdata_2022-05.parquet
        ✓ Read green_tripdata_2022-05.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 76,891 records
    
    [102/117] Processing: green_tripdata_2022-06.parquet
        ✓ Read green_tripdata_2022-06.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 73,718 records
    
    [103/117] Processing: green_tripdata_2022-07.parquet
        ✓ Read green_tripdata_2022-07.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 64,192 records
    
    [104/117] Processing: green_tripdata_2022-08.parquet
        ✓ Read green_tripdata_2022-08.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 65,929 records
    
    [105/117] Processing: green_tripdata_2022-09.parquet
        ✓ Read green_tripdata_2022-09.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 69,031 records
    
    [106/117] Processing: green_tripdata_2022-10.parquet
        ✓ Read green_tripdata_2022-10.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 69,322 records
    
    [107/117] Processing: green_tripdata_2022-11.parquet
        ✓ Read green_tripdata_2022-11.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 62,313 records
    
    [108/117] Processing: green_tripdata_2022-12.parquet
        ✓ Read green_tripdata_2022-12.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 72,439 records
    
    [109/117] Processing: green_tripdata_2023-01.parquet
        ✓ Read green_tripdata_2023-01.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 68,211 records
    
    [110/117] Processing: green_tripdata_2023-02.parquet
        ✓ Read green_tripdata_2023-02.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 64,809 records
    
    [111/117] Processing: green_tripdata_2023-03.parquet
        ✓ Read green_tripdata_2023-03.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 72,044 records
    
    [112/117] Processing: green_tripdata_2023-04.parquet
        ✓ Read green_tripdata_2023-04.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 65,392 records
    
    [113/117] Processing: green_tripdata_2023-05.parquet
        ✓ Read green_tripdata_2023-05.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 69,174 records
    
    [114/117] Processing: green_tripdata_2023-06.parquet
        ✓ Read green_tripdata_2023-06.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 65,550 records
    
    [115/117] Processing: green_tripdata_2023-07.parquet
        ✓ Read green_tripdata_2023-07.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 61,343 records
    
    [116/117] Processing: green_tripdata_2023-08.parquet
        ✓ Read green_tripdata_2023-08.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 60,649 records
    
    [117/117] Processing: green_tripdata_2023-09.parquet
        ✓ Read green_tripdata_2023-09.parquet without explicit schema
        ✓ Applied casting to 18 columns
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
        ✓ Processed 65,471 records
    
    📊 GREEN PROCESSING SUMMARY:
        ✅ Successfully processed: 117/117 files
        📈 Total records processed: 82,630,053
        📁 S3 folder: s3://zhogolev-pv-temp-files/temp/green_taxi_combined_da284725/
        ❌ Failed files: 0
    
    🟢 GREEN TAXI COMPLETED:
       ✅ Files processed: 117/117
       📊 Total records: 82,630,053
       📁 S3 location: s3://zhogolev-pv-temp-files/temp/green_taxi_combined_da284725/

### BLOCK WORK WITH TYPES PROCESSED DATA


```pyspark
print("1. Set up schema for read processed data...")
green_processed_schema = StructType([
    StructField('VendorID', LongType(), True),
    StructField('lpep_pickup_datetime', TimestampNTZType(), True),
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('RatecodeID', DoubleType(), True),
    StructField('PULocationID', LongType(), True),
    StructField('DOLocationID', LongType(), True),
    StructField('passenger_count', LongType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('ehail_fee', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('payment_type', LongType(), True),
    StructField('trip_type', LongType(), True),
    StructField('congestion_surcharge', DoubleType(), True)
])

yellow_processed_schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
])
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    1. Set up schema for read processed data...


```pyspark
print("2. Read processed_data...")
def verify_processed_data_safely(s3_folder: str, taxi_type: TaxiTypeEnum, schema) -> dict:
    """Safely verify processed data using sampling"""
    try:
        print(f"Verifying {taxi_type} processed data...")
        print(f"S3 folder: {s3_folder}")

        df = spark.read.schema(schema).option("recursiveFileLookup", "true").parquet(s3_folder)

        schema_info = {
            "columns": len(df.columns),
            "column_names": df.columns
        }

        sample_fraction = 0.005  
        sample_df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
        sample_count = sample_df.count()
        estimated_total = int(sample_count / sample_fraction) if sample_count > 0 else 0

        print(f"Columns: {schema_info['columns']}")
        print(f"Estimated total records: ~{estimated_total:,}")
        print(f"Sample data (from {sample_fraction*100}% sample):")
        sample_df.limit(3).show(truncate=False)
        
        return df
        
    except Exception as e:
        print(f"    ✗ Verification failed: {e}")
        return {"error": str(e)}

# Read Yellow Taxi Data
yellow_df = verify_processed_data_safely(YELLOW_TEMP_FOLDER, TaxiTypeEnum.YELLOW, yellow_processed_schema)

# Read Green Taxi Data  
green_df = verify_processed_data_safely(GREEN_TEMP_FOLDER, TaxiTypeEnum.GREEN, green_processed_schema)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    2. Read processed_data...
    Verifying TaxiTypeEnum.YELLOW processed data...
    S3 folder: s3://zhogolev-pv-temp-files/temp/yellow_taxi_combined_da284725/
    Columns: 19
    Estimated total records: ~744,854,800
    Sample data (from 0.5% sample):
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+
    |VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+
    |2       |2014-01-01 00:50:00 |2014-01-01 00:54:00  |5              |0.88         |1.0       |null              |224         |107         |2           |5.0        |0.5  |0.5    |0.0       |0.0         |0.0                  |6.0         |null                |null       |
    |2       |2014-01-01 00:06:00 |2014-01-01 00:35:00  |3              |3.23         |1.0       |null              |48          |144         |2           |19.0       |0.5  |0.5    |0.0       |0.0         |0.0                  |20.0        |null                |null       |
    |2       |2014-01-01 00:43:00 |2014-01-01 00:50:00  |1              |2.16         |1.0       |null              |262         |238         |2           |9.0        |0.5  |0.5    |0.0       |0.0         |0.0                  |10.0        |null                |null       |
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+
    
    Verifying TaxiTypeEnum.GREEN processed data...
    S3 folder: s3://zhogolev-pv-temp-files/temp/green_taxi_combined_da284725/
    Columns: 20
    Estimated total records: ~82,727,400
    Sample data (from 0.5% sample):
    +--------+--------------------+---------------------+------------------+----------+------------+------------+---------------+-------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+------------+---------+--------------------+
    |VendorID|lpep_pickup_datetime|lpep_dropoff_datetime|store_and_fwd_flag|RatecodeID|PULocationID|DOLocationID|passenger_count|trip_distance|fare_amount|extra|mta_tax|tip_amount|tolls_amount|ehail_fee|improvement_surcharge|total_amount|payment_type|trip_type|congestion_surcharge|
    +--------+--------------------+---------------------+------------------+----------+------------+------------+---------------+-------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+------------+---------+--------------------+
    |1       |2014-02-01 00:07:16 |2014-02-01 00:13:42  |N                 |1.0       |166         |41          |1              |1.1          |6.5        |0.5  |0.5    |0.0       |0.0         |null     |null                 |7.5         |2           |null     |null                |
    |2       |2014-02-01 00:09:42 |2014-02-01 00:18:43  |N                 |1.0       |25          |49          |1              |2.12         |9.5        |0.5  |0.5    |0.0       |0.0         |null     |null                 |10.5        |1           |null     |null                |
    |2       |2014-02-01 00:44:56 |2014-02-01 00:52:39  |N                 |1.0       |256         |255         |4              |1.52         |7.5        |0.5  |0.5    |1.0       |0.0         |null     |null                 |9.5         |1           |null     |null                |
    +--------+--------------------+---------------------+------------------+----------+------------+------------+---------------+-------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+------------+---------+--------------------+


```pyspark
print("3. Analyzing schemas...")

print("Yellow taxi columns:")
yellow_schema = yellow_df.schema
show_schema(yellow_df)
set_yellow_schema = set(yellow_schema)

print("-"*100)

print("Green taxi columns:")
green_schema = green_df.schema
show_schema(green_df)
set_green_schema = set(green_schema)

print("-"*100)

# get diff between columes
diff_green_yellow = list(set_green_schema - set_yellow_schema)
print(f"Diff green_yellow {diff_green_yellow}")

diff_yellow_green = list(set_yellow_schema - set_green_schema)
print(f"Diff yellow_green {diff_yellow_green}")

```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    3. Analyzing schemas...
    Yellow taxi columns:
    StructField('VendorID', LongType(), True)
    StructField('tpep_pickup_datetime', TimestampNTZType(), True)
    StructField('tpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
    ----------------------------------------------------------------------------------------------------
    Green taxi columns:
    StructField('VendorID', LongType(), True)
    StructField('lpep_pickup_datetime', TimestampNTZType(), True)
    StructField('lpep_dropoff_datetime', TimestampNTZType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('payment_type', LongType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    ----------------------------------------------------------------------------------------------------
    Diff green_yellow [StructField('trip_type', LongType(), True), StructField('ehail_fee', DoubleType(), True), StructField('lpep_dropoff_datetime', TimestampNTZType(), True), StructField('lpep_pickup_datetime', TimestampNTZType(), True)]
    Diff yellow_green [StructField('tpep_dropoff_datetime', TimestampNTZType(), True), StructField('tpep_pickup_datetime', TimestampNTZType(), True), StructField('airport_fee', DoubleType(), True)]


```pyspark
print("4. Performing union...")

yellow_df_standardized = yellow_df.select(
    col("VendorID"),
    col("tpep_pickup_datetime").alias("pickup_datetime"),
    col("tpep_dropoff_datetime").alias("dropoff_datetime"),
    col("passenger_count"),
    col("trip_distance"),
    col("RatecodeID"),
    col("store_and_fwd_flag"),
    col("PULocationID"),
    col("DOLocationID"),
    col("payment_type"),
    col("fare_amount"),
    col("extra"),
    col("mta_tax"),
    col("tip_amount"),
    col("tolls_amount"),
    lit(None).cast("double").alias("ehail_fee"), 
    col("improvement_surcharge"),
    col("total_amount"),
    lit(None).cast("long").alias("trip_type"),
    col("congestion_surcharge"),
    col("airport_fee"),
    lit("yellow").alias("taxi_type")
)

green_df_standardized = green_df.select(
    col("VendorID"),
    col("lpep_pickup_datetime").alias("pickup_datetime"), 
    col("lpep_dropoff_datetime").alias("dropoff_datetime"), 
    col("passenger_count"),
    col("trip_distance"),
    col("RatecodeID"),
    col("store_and_fwd_flag"),
    col("PULocationID"),
    col("DOLocationID"),
    col("payment_type"),
    col("fare_amount"),
    col("extra"),
    col("mta_tax"),
    col("tip_amount"),
    col("tolls_amount"),
    col("ehail_fee"),
    col("improvement_surcharge"),
    col("total_amount"),
    col("trip_type"),
    col("congestion_surcharge"),
    lit(None).cast("double").alias("airport_fee"),
    lit("green").alias("taxi_type") 
)

print("Yellow taxi standardized columns:")
print(yellow_df_standardized.columns)
print(f"Count: {len(yellow_df_standardized.columns)}")

print("\nGreen taxi standardized columns:")
print(green_df_standardized.columns)
print(f"Count: {len(green_df_standardized.columns)}")

print(f"\nSchemas match: {yellow_df_standardized.columns == green_df_standardized.columns}")

print("4. Performing union...")
raw_trips_df = yellow_df_standardized.union(green_df_standardized)

print(f"\nCombined dataframe:")
print(f"- Total rows: {raw_trips_df.count():,}")
print(f"- Total columns: {len(raw_trips_df.columns)}")
print(f"- Yellow trips: {raw_trips_df.filter(col('taxi_type') == 'yellow').count():,}")
print(f"- Green trips: {raw_trips_df.filter(col('taxi_type') == 'green').count():,}")

print("\nSample of combined data:")
raw_trips_df.show(5, truncate=False)

print("\nFinal schema:")
raw_trips_df.printSchema()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    4. Performing union...
    Yellow taxi standardized columns:
    ['VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'trip_type', 'congestion_surcharge', 'airport_fee', 'taxi_type']
    Count: 22
    
    Green taxi standardized columns:
    ['VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'trip_type', 'congestion_surcharge', 'airport_fee', 'taxi_type']
    Count: 22
    
    Schemas match: True
    4. Performing union...
    
    Combined dataframe:
    - Total rows: 827,697,864
    - Total columns: 22
    - Yellow trips: 745,067,811
    - Green trips: 82,630,053
    
    Sample of combined data:
    +--------+-------------------+-------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+---------+--------------------+-----------+---------+
    |VendorID|pickup_datetime    |dropoff_datetime   |passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|ehail_fee|improvement_surcharge|total_amount|trip_type|congestion_surcharge|airport_fee|taxi_type|
    +--------+-------------------+-------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+---------+--------------------+-----------+---------+
    |2       |2014-01-01 00:02:00|2014-01-01 00:04:00|6              |0.0          |1.0       |null              |146         |146         |1           |3.5        |0.5  |0.5    |0.02      |0.0         |null     |0.0                  |4.52        |null     |null                |null       |yellow   |
    |2       |2014-01-01 00:06:00|2014-01-01 00:09:00|5              |0.0          |1.0       |null              |146         |146         |1           |3.5        |0.5  |0.5    |0.05      |0.0         |null     |0.0                  |4.55        |null     |null                |null       |yellow   |
    |2       |2014-01-01 00:10:00|2014-01-01 00:13:00|5              |0.0          |1.0       |null              |146         |146         |1           |3.5        |0.5  |0.5    |0.08      |0.0         |null     |0.0                  |4.58        |null     |null                |null       |yellow   |
    |2       |2014-01-01 00:54:00|2014-01-01 00:55:00|5              |0.0          |1.0       |null              |264         |264         |2           |2.5        |0.5  |0.5    |0.0       |0.0         |null     |0.0                  |3.5         |null     |null                |null       |yellow   |
    |1       |2014-01-01 00:29:18|2014-01-01 00:35:13|2              |1.8          |1.0       |N                 |229         |262         |2           |7.5        |0.5  |0.5    |0.0       |0.0         |null     |0.0                  |8.5         |null     |null                |null       |yellow   |
    +--------+-------------------+-------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+---------+--------------------+-----------+---------+
    only showing top 5 rows
    
    
    Final schema:
    root
     |-- VendorID: long (nullable = true)
     |-- pickup_datetime: timestamp_ntz (nullable = true)
     |-- dropoff_datetime: timestamp_ntz (nullable = true)
     |-- passenger_count: long (nullable = true)
     |-- trip_distance: double (nullable = true)
     |-- RatecodeID: double (nullable = true)
     |-- store_and_fwd_flag: string (nullable = true)
     |-- PULocationID: long (nullable = true)
     |-- DOLocationID: long (nullable = true)
     |-- payment_type: long (nullable = true)
     |-- fare_amount: double (nullable = true)
     |-- extra: double (nullable = true)
     |-- mta_tax: double (nullable = true)
     |-- tip_amount: double (nullable = true)
     |-- tolls_amount: double (nullable = true)
     |-- ehail_fee: double (nullable = true)
     |-- improvement_surcharge: double (nullable = true)
     |-- total_amount: double (nullable = true)
     |-- trip_type: long (nullable = true)
     |-- congestion_surcharge: double (nullable = true)
     |-- airport_fee: double (nullable = true)
     |-- taxi_type: string (nullable = false)


```pyspark
print("5. Add calculate colume trip_duration_minutes")
raw_trips_with_duration = raw_trips_df.withColumn(
    "trip_duration_minutes", 
    (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
)
raw_trips_with_duration.show(5, truncate=False)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    5. Add calculate colume trip_duration_minutes
    +--------+-------------------+-------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+---------+--------------------+-----------+---------+---------------------+
    |VendorID|pickup_datetime    |dropoff_datetime   |passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|ehail_fee|improvement_surcharge|total_amount|trip_type|congestion_surcharge|airport_fee|taxi_type|trip_duration_minutes|
    +--------+-------------------+-------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+---------+--------------------+-----------+---------+---------------------+
    |2       |2014-01-01 00:02:00|2014-01-01 00:04:00|6              |0.0          |1.0       |null              |146         |146         |1           |3.5        |0.5  |0.5    |0.02      |0.0         |null     |0.0                  |4.52        |null     |null                |null       |yellow   |2.0                  |
    |2       |2014-01-01 00:06:00|2014-01-01 00:09:00|5              |0.0          |1.0       |null              |146         |146         |1           |3.5        |0.5  |0.5    |0.05      |0.0         |null     |0.0                  |4.55        |null     |null                |null       |yellow   |3.0                  |
    |2       |2014-01-01 00:10:00|2014-01-01 00:13:00|5              |0.0          |1.0       |null              |146         |146         |1           |3.5        |0.5  |0.5    |0.08      |0.0         |null     |0.0                  |4.58        |null     |null                |null       |yellow   |3.0                  |
    |2       |2014-01-01 00:54:00|2014-01-01 00:55:00|5              |0.0          |1.0       |null              |264         |264         |2           |2.5        |0.5  |0.5    |0.0       |0.0         |null     |0.0                  |3.5         |null     |null                |null       |yellow   |1.0                  |
    |1       |2014-01-01 00:29:18|2014-01-01 00:35:13|2              |1.8          |1.0       |N                 |229         |262         |2           |7.5        |0.5  |0.5    |0.0       |0.0         |null     |0.0                  |8.5         |null     |null                |null       |yellow   |5.916666666666667    |
    +--------+-------------------+-------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+---------+--------------------+-----------+---------+---------------------+
    only showing top 5 rows


```pyspark
print("6. Filtering")

filtered_trips_df = raw_trips_with_duration.filter(
    (col("trip_distance") >= 0.1) &           
    (col("fare_amount") >= 2.0) &            
    (col("trip_duration_minutes") >= 1.0)  
)

print("After filtering:")

initial_count = raw_trips_with_duration.count()
final_count = filtered_trips_df.count()
removed_count = initial_count - final_count

print(f"Total trips: {final_count:,}")
print(f"Removed trips: {removed_count:,}")
print(f"Removal rate: {(removed_count/initial_count*100):.2f}%")

print("Filtered data statistics:")
print(f"Yellow trips: {filtered_trips_df.filter(col('taxi_type') == 'yellow').count():,}")
print(f"Green trips: {filtered_trips_df.filter(col('taxi_type') == 'green').count():,}")

print("Sample of filtered data:")
filtered_trips_df.select(
    "taxi_type", "pickup_datetime", "dropoff_datetime", 
    "trip_distance", "fare_amount", "trip_duration_minutes"
).show(5, truncate=False)
print(f"\nUpdated raw_trips_df with {raw_trips_df.count():,} valid trips")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    6. Filtering
    After filtering:
    Total trips: 816,137,348
    Removed trips: 11,560,516
    Removal rate: 1.40%
    Filtered data statistics:
    Yellow trips: 735,821,800
    Green trips: 80,315,548
    Sample of filtered data:
    +---------+-------------------+-------------------+-------------+-----------+---------------------+
    |taxi_type|pickup_datetime    |dropoff_datetime   |trip_distance|fare_amount|trip_duration_minutes|
    +---------+-------------------+-------------------+-------------+-----------+---------------------+
    |yellow   |2014-01-01 00:29:18|2014-01-01 00:35:13|1.8          |7.5        |5.916666666666667    |
    |yellow   |2014-01-01 00:36:33|2014-01-01 00:55:19|4.8          |17.5       |18.766666666666666   |
    |yellow   |2014-01-01 00:20:43|2014-01-01 00:31:07|2.0          |9.5        |10.4                 |
    |yellow   |2014-01-01 00:48:52|2014-01-01 00:56:14|1.1          |6.5        |7.366666666666666    |
    |yellow   |2014-01-01 00:45:29|2014-01-01 00:55:33|1.6          |8.5        |10.066666666666666   |
    +---------+-------------------+-------------------+-------------+-----------+---------------------+
    only showing top 5 rows
    
    
    Updated raw_trips_df with 827,697,864 valid trips


```pyspark
print("7. Adding time-based features...")

raw_trips_enhanced = filtered_trips_df.withColumn(
    "pickup_hour", hour(col("pickup_datetime"))
        ).withColumn(
    "pickup_day_of_week", dayofweek(col("pickup_datetime"))
        ).withColumn(
    "duration_min", 
    (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
        )

print("Time features added successfully!")
print("Sample with new time features:")
raw_trips_enhanced.select(
    "pickup_datetime", "pickup_hour", "pickup_day_of_week", "duration_min"
).show(5)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    7. Adding time-based features...
    Time features added successfully!
    Sample with new time features:
    +-------------------+-----------+------------------+------------------+
    |    pickup_datetime|pickup_hour|pickup_day_of_week|      duration_min|
    +-------------------+-----------+------------------+------------------+
    |2014-01-01 00:29:18|          0|                 4| 5.916666666666667|
    |2014-01-01 00:36:33|          0|                 4|18.766666666666666|
    |2014-01-01 00:20:43|          0|                 4|              10.4|
    |2014-01-01 00:48:52|          0|                 4| 7.366666666666666|
    |2014-01-01 00:45:29|          0|                 4|10.066666666666666|
    +-------------------+-----------+------------------+------------------+
    only showing top 5 rows


```pyspark
print("8. Read taxi_zone_lookup.csv")
zone_lookup_schema = StructType([
    StructField("LocationID", LongType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])


zone_lookup_df = spark.read.csv(
    ZONE_LOOKUP_PATH, 
    header=True, 
    schema=zone_lookup_schema
)
print(f"Zone lookup data loaded: {zone_lookup_df.count()} zones")
print("Zone lookup schema:")
zone_lookup_df.printSchema()
print("\nSample zone data:")
zone_lookup_df.show(5)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    8. Read taxi_zone_lookup.csv
    Zone lookup data loaded: 265 zones
    Zone lookup schema:
    root
     |-- LocationID: long (nullable = true)
     |-- Borough: string (nullable = true)
     |-- Zone: string (nullable = true)
     |-- service_zone: string (nullable = true)
    
    
    Sample zone data:
    +----------+-------------+--------------------+------------+
    |LocationID|      Borough|                Zone|service_zone|
    +----------+-------------+--------------------+------------+
    |         1|          EWR|      Newark Airport|         EWR|
    |         2|       Queens|         Jamaica Bay|   Boro Zone|
    |         3|        Bronx|Allerton/Pelham G...|   Boro Zone|
    |         4|    Manhattan|       Alphabet City| Yellow Zone|
    |         5|Staten Island|       Arden Heights|   Boro Zone|
    +----------+-------------+--------------------+------------+
    only showing top 5 rows


```pyspark
raw_trips_enhanced.printSchema()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    root
     |-- VendorID: long (nullable = true)
     |-- pickup_datetime: timestamp_ntz (nullable = true)
     |-- dropoff_datetime: timestamp_ntz (nullable = true)
     |-- passenger_count: long (nullable = true)
     |-- trip_distance: double (nullable = true)
     |-- RatecodeID: double (nullable = true)
     |-- store_and_fwd_flag: string (nullable = true)
     |-- PULocationID: long (nullable = true)
     |-- DOLocationID: long (nullable = true)
     |-- payment_type: long (nullable = true)
     |-- fare_amount: double (nullable = true)
     |-- extra: double (nullable = true)
     |-- mta_tax: double (nullable = true)
     |-- tip_amount: double (nullable = true)
     |-- tolls_amount: double (nullable = true)
     |-- ehail_fee: double (nullable = true)
     |-- improvement_surcharge: double (nullable = true)
     |-- total_amount: double (nullable = true)
     |-- trip_type: long (nullable = true)
     |-- congestion_surcharge: double (nullable = true)
     |-- airport_fee: double (nullable = true)
     |-- taxi_type: string (nullable = false)
     |-- trip_duration_minutes: double (nullable = true)
     |-- pickup_hour: integer (nullable = true)
     |-- pickup_day_of_week: integer (nullable = true)
     |-- duration_min: double (nullable = true)


```pyspark
print("9. Join pickup_zones")
pickup_zones = zone_lookup_df.select(
    col("LocationID").alias("pickup_location_id"),
    col("Zone").alias("pickup_zone"),
    col("Borough").alias("pickup_borough"),
    col("service_zone").alias("pickup_service_zone")
)

trips_with_pickup_zones = raw_trips_enhanced.join(
    broadcast(pickup_zones),
    raw_trips_df.PULocationID == pickup_zones.pickup_location_id,
    "left"
).drop("pickup_location_id")

print("Show joined pickup_zones")
trips_with_pickup_zones.select("pickup_zone", "pickup_borough", "pickup_service_zone").show(5)
trips_with_pickup_zones.cache()
show_schema(trips_with_pickup_zones)

print("Pickup zones joined successfully!")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    9. Join pickup_zones
    Show joined pickup_zones
    +--------------------+--------------+-------------------+
    |         pickup_zone|pickup_borough|pickup_service_zone|
    +--------------------+--------------+-------------------+
    |Sutton Place/Turt...|     Manhattan|        Yellow Zone|
    |     Lenox Hill East|     Manhattan|        Yellow Zone|
    |            Elmhurst|        Queens|          Boro Zone|
    |        Midtown East|     Manhattan|        Yellow Zone|
    |            Flatiron|     Manhattan|        Yellow Zone|
    +--------------------+--------------+-------------------+
    only showing top 5 rows
    
    StructField('VendorID', LongType(), True)
    StructField('pickup_datetime', TimestampNTZType(), True)
    StructField('dropoff_datetime', TimestampNTZType(), True)
    StructField('passenger_count', LongType(), True)
    StructField('trip_distance', DoubleType(), True)
    StructField('RatecodeID', DoubleType(), True)
    StructField('store_and_fwd_flag', StringType(), True)
    StructField('PULocationID', LongType(), True)
    StructField('DOLocationID', LongType(), True)
    StructField('payment_type', LongType(), True)
    StructField('fare_amount', DoubleType(), True)
    StructField('extra', DoubleType(), True)
    StructField('mta_tax', DoubleType(), True)
    StructField('tip_amount', DoubleType(), True)
    StructField('tolls_amount', DoubleType(), True)
    StructField('ehail_fee', DoubleType(), True)
    StructField('improvement_surcharge', DoubleType(), True)
    StructField('total_amount', DoubleType(), True)
    StructField('trip_type', LongType(), True)
    StructField('congestion_surcharge', DoubleType(), True)
    StructField('airport_fee', DoubleType(), True)
    StructField('taxi_type', StringType(), False)
    StructField('trip_duration_minutes', DoubleType(), True)
    StructField('pickup_hour', IntegerType(), True)
    StructField('pickup_day_of_week', IntegerType(), True)
    StructField('duration_min', DoubleType(), True)
    StructField('pickup_zone', StringType(), True)
    StructField('pickup_borough', StringType(), True)
    StructField('pickup_service_zone', StringType(), True)
    Pickup zones joined successfully!


```pyspark
print("10. Join dropoff_zones")
dropoff_zones = zone_lookup_df.select(
    col("LocationID").alias("dropoff_location_id"),
    col("Zone").alias("dropoff_zone"),
    col("Borough").alias("dropoff_borough"),
    col("service_zone").alias("dropoff_service_zone")
)

trips_with_all_zones = trips_with_pickup_zones.join(
    dropoff_zones,
    trips_with_pickup_zones.DOLocationID == dropoff_zones.dropoff_location_id,
    "left"
).drop("dropoff_location_id")

print("Show joined dropoff_zones")

trips_with_all_zones.printSchema()

print("Dropoff zones joined successfully!")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    10. Join dropoff_zones
    Show joined dropoff_zones
    root
     |-- VendorID: long (nullable = true)
     |-- pickup_datetime: timestamp_ntz (nullable = true)
     |-- dropoff_datetime: timestamp_ntz (nullable = true)
     |-- passenger_count: long (nullable = true)
     |-- trip_distance: double (nullable = true)
     |-- RatecodeID: double (nullable = true)
     |-- store_and_fwd_flag: string (nullable = true)
     |-- PULocationID: long (nullable = true)
     |-- DOLocationID: long (nullable = true)
     |-- payment_type: long (nullable = true)
     |-- fare_amount: double (nullable = true)
     |-- extra: double (nullable = true)
     |-- mta_tax: double (nullable = true)
     |-- tip_amount: double (nullable = true)
     |-- tolls_amount: double (nullable = true)
     |-- ehail_fee: double (nullable = true)
     |-- improvement_surcharge: double (nullable = true)
     |-- total_amount: double (nullable = true)
     |-- trip_type: long (nullable = true)
     |-- congestion_surcharge: double (nullable = true)
     |-- airport_fee: double (nullable = true)
     |-- taxi_type: string (nullable = false)
     |-- trip_duration_minutes: double (nullable = true)
     |-- pickup_hour: integer (nullable = true)
     |-- pickup_day_of_week: integer (nullable = true)
     |-- duration_min: double (nullable = true)
     |-- pickup_zone: string (nullable = true)
     |-- pickup_borough: string (nullable = true)
     |-- pickup_service_zone: string (nullable = true)
     |-- dropoff_zone: string (nullable = true)
     |-- dropoff_borough: string (nullable = true)
     |-- dropoff_service_zone: string (nullable = true)
    
    Dropoff zones joined successfully!

# TASK 3 Створення та агрегація фінального датафрейму zone_summary


```pyspark
print("Part 1: Creating zone_summary dataframe with basic aggregations...")

try:
    trips_with_all_zones.unpersist()
except:
    pass

trips_with_valid_zones = trips_with_all_zones.filter(col("pickup_zone").isNotNull())

print("Calculating basic metrics...")
zone_basic_metrics = trips_with_valid_zones.groupBy("pickup_zone").agg(
    count("*").alias("total_trips"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("total_amount").alias("avg_total_amount"),
    avg("tip_amount").alias("avg_tip_amount"),
    max("trip_distance").alias("max_trip_distance"),
    min("tip_amount").alias("min_tip_amount")
)
zone_basic_metrics.cache()
print(f"Basic metrics calculated for {zone_basic_metrics.count()} zones")

print("\nCalculating taxi type shares...")
taxi_type_counts = trips_with_valid_zones.groupBy("pickup_zone", "taxi_type") \
    .agg(count("*").alias("type_count"))

taxi_shares = taxi_type_counts.groupBy("pickup_zone").pivot("taxi_type", ["yellow", "green"]) \
    .sum("type_count") \
    .fillna(0)

taxi_shares_with_total = taxi_shares.join(
    zone_basic_metrics.select("pickup_zone", "total_trips"),
    "pickup_zone"
)

taxi_shares_final = taxi_shares_with_total.select(
    "pickup_zone",
    (col("yellow") / col("total_trips")).alias("yellow_share"),
    (col("green") / col("total_trips")).alias("green_share")
)

taxi_shares.unpersist()

print("Taxi shares calculated successfully")


```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    Part 1: Creating zone_summary dataframe with basic aggregations...
    Calculating basic metrics...
    Basic metrics calculated for 262 zones
    
    Calculating taxi type shares...
    Taxi shares calculated successfully


```pyspark
print("Part 2: Joining all metrics...")

zone_summary = zone_basic_metrics.join(taxi_shares_final, "pickup_zone") \
    .select(
        "pickup_zone",
        "total_trips",
        "avg_trip_distance",
        "avg_total_amount",
        "avg_tip_amount",
        "yellow_share",
        "green_share",
        "max_trip_distance",
        "min_tip_amount"
    ) \
    .orderBy(col("total_trips").desc())

zone_basic_metrics.unpersist()

print("Zone summary schema:")
zone_summary.printSchema()

print("\nTop 10 zones by total trips:")
zone_summary.show(10, truncate=False)

print(f"\nTotal number of zones: {zone_summary.count()}")

print("\nVerifying yellow_share + green_share = 1:")
zone_summary.select(
    "pickup_zone",
    "yellow_share",
    "green_share",
    (col("yellow_share") + col("green_share")).alias("total_share")
).show(5)


```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    Part 2: Joining all metrics...
    Zone summary schema:
    root
     |-- pickup_zone: string (nullable = true)
     |-- total_trips: long (nullable = false)
     |-- avg_trip_distance: double (nullable = true)
     |-- avg_total_amount: double (nullable = true)
     |-- avg_tip_amount: double (nullable = true)
     |-- yellow_share: double (nullable = true)
     |-- green_share: double (nullable = true)
     |-- max_trip_distance: double (nullable = true)
     |-- min_tip_amount: double (nullable = true)
    
    
    Top 10 zones by total trips:
    +----------------------------+-----------+------------------+------------------+------------------+------------------+---------------------+-----------------+--------------+
    |pickup_zone                 |total_trips|avg_trip_distance |avg_total_amount  |avg_tip_amount    |yellow_share      |green_share          |max_trip_distance|min_tip_amount|
    +----------------------------+-----------+------------------+------------------+------------------+------------------+---------------------+-----------------+--------------+
    |Upper East Side South       |28757902   |4.9582161563802565|12.717631308413925|1.3744157724023394|0.9999496486217945|5.0351378205545034E-5|1.2E7            |-0.84         |
    |Midtown Center              |26879469   |6.282413865020876 |15.451334278195718|1.7337466885972217|0.999961234353253 |3.8765646746965125E-5|1.18000006E7     |0.0           |
    |Upper East Side North       |26142940   |3.579347698843354 |13.292185396884864|1.4665199568220209|0.9923375488755282|0.007662451124471846 |1.18000052E7     |-4.42         |
    |Midtown East                |24759596   |6.300220616685335 |15.321438132899038|1.7786730050038544|0.9999599751142951|4.002488570492023E-5 |1.20000032E7     |-0.08         |
    |Penn Station/Madison Sq West|24359583   |7.304832184114192 |15.155543334610748|1.6178981536753523|0.9999659682187498|3.4031781250114175E-5|1.5420061E7      |0.0           |
    |Times Sq/Theatre District   |23890071   |6.695229916227531 |16.823519044354143|1.7349072939130767|0.999968731779826 |3.126822017397939E-5 |1.57E7           |0.0           |
    |Murray Hill                 |23669051   |7.857148930474677 |14.749365483207507|1.6906875666456014|0.9998815753111521|1.1842468884789678E-4|1.43318E7        |0.0           |
    |Union Sq                    |23389195   |4.25213690637918  |14.212888703939512|1.6731893051471138|0.9999620765058396|3.7923494160444595E-5|1.18000021E7     |0.0           |
    |Clinton East                |23099704   |6.7236672136577695|14.89284430050174 |1.5446955982639707|0.9999345446158098|6.545538419020434E-5 |1.18000004E7     |0.0           |
    |East Village                |22551070   |5.820349961664764 |14.810184079946513|1.7079835630859026|0.9999631503072803|3.684969271968026E-5 |1.18000011E7     |0.0           |
    +----------------------------+-----------+------------------+------------------+------------------+------------------+---------------------+-----------------+--------------+
    only showing top 10 rows
    
    
    Total number of zones: 262
    
    Verifying yellow_share + green_share = 1:
    +--------------------+------------------+--------------------+-----------+
    |         pickup_zone|      yellow_share|         green_share|total_share|
    +--------------------+------------------+--------------------+-----------+
    |Upper East Side S...|0.9999496486217945|5.035137820554503...|        1.0|
    |      Midtown Center| 0.999961234353253|3.876564674696512...|        1.0|
    |Upper East Side N...|0.9923375488755282|0.007662451124471846|        1.0|
    |        Midtown East|0.9999599751142951|4.002488570492023E-5|        1.0|
    |Penn Station/Madi...|0.9999659682187498|3.403178125011417...|        1.0|
    +--------------------+------------------+--------------------+-----------+
    only showing top 5 rows


```pyspark
print("Part 3: Save results..)

output_path = f"s3://pzhoholiev-emr-bucket-hw/results/zone_statistic/{current_date}/zone_statistic.parquet"

print(f"\nSaving zone_summary to: {output_path}")
      
zone_summary.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_path)

print("Zone summary saved successfully!")
print("\nVerifying saved data...")
saved_zone_summary = spark.read.parquet(output_path)
print(f"Saved records count: {saved_zone_summary.count()}")
saved_zone_summary.show(5)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    
    Saving zone_summary to: s3://pzhoholiev-emr-bucket-hw/results/zone_statistic/2025-07-19/zone_statistic.parquet
    Zone summary saved successfully!
    
    Verifying saved data...
    Saved records count: 262
    +--------------------+-----------+------------------+------------------+------------------+------------------+--------------------+-----------------+--------------+
    |         pickup_zone|total_trips| avg_trip_distance|  avg_total_amount|    avg_tip_amount|      yellow_share|         green_share|max_trip_distance|min_tip_amount|
    +--------------------+-----------+------------------+------------------+------------------+------------------+--------------------+-----------------+--------------+
    |Upper East Side S...|   28757902| 4.958216156380258|12.717631308413935|1.3744157724023394|0.9999496486217945|5.035137820554503...|            1.2E7|         -0.84|
    |      Midtown Center|   26879469| 6.282413865020874|15.451334278195722|1.7337466885972215| 0.999961234353253|3.876564674696512...|     1.18000006E7|           0.0|
    |Upper East Side N...|   26142940|3.5793476988433537|13.292185396884854|1.4665199568220204|0.9923375488755282|0.007662451124471846|     1.18000052E7|         -4.42|
    |        Midtown East|   24759596|  6.30022061668534| 15.32143813289904| 1.778673005003854|0.9999599751142951|4.002488570492023E-5|     1.20000032E7|         -0.08|
    |Penn Station/Madi...|   24359583| 7.304832184114191|15.155543334610748|1.6178981536753503|0.9999659682187498|3.403178125011417...|      1.5420061E7|           0.0|
    +--------------------+-----------+------------------+------------------+------------------+------------------+--------------------+-----------------+--------------+
    only showing top 5 rows

# TASK 4 Агрегація по днях тижня та зонах


```pyspark
day_names = {
    1: "Monday",
    2: "Tuesday", 
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday"
}
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
def get_trips_with_fares_over_30(week_day: Optional[int] = None):
    days_stats = []
    list_days_int_values = list(day_names.keys())
    if week_day is None:
        processed_days_list = list_days_int_values
    else:
        if week_day not in list_days_int_values:
            raise ValueError("Week_day arg can be between 1-7")
        processed_days_list = [week_day]
        
    for day in processed_days_list:
        trips_selected_day = trips_with_all_zones.filter(
            (col("pickup_day_of_week") == day) & 
            (col("pickup_zone").isNotNull())
        )
        
        zone_day_stats = trips_selected_day.groupBy("pickup_zone").agg(
            count("*").alias("trips_count"),

            sum(when(col("total_amount") > 30, 1).otherwise(0)).alias("high_fare_count"),

            avg("total_amount").alias("avg_total_amount"),
            avg("trip_distance").alias("avg_trip_distance"),

            max("total_amount").alias("max_fare"),
            min("total_amount").alias("min_fare")
        )
        
        zone_day_stats_final = zone_day_stats.withColumn(
            "high_fare_share",
            col("high_fare_count") / col("trips_count")
        ).withColumn(
            "day_of_week", 
            lit(day)
        ).withColumn(
            "day_name",
            lit(day_names[day])
        )
        
        zone_day_stats_final = zone_day_stats_final.select(
            "pickup_zone",
            "day_of_week",
            "day_name",
            "trips_count",
            "high_fare_share",
            "high_fare_count",
            "avg_total_amount",
            "avg_trip_distance",
            "max_fare",
            "min_fare"
        ).orderBy(col("trips_count").desc())
        days_stats.append(zone_day_stats_final)
    result = days_stats[0]
    if len(days_stats) > 1:
        for df in days_stats[1:]:
            result = result.union(df)
    return result
    
        
    
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
print("Stats on Monday")

monday_stats = get_trips_with_fares_over_30(1)
monday_stats.show(10)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    Stats on Monday
    +--------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    |         pickup_zone|day_of_week|day_name|trips_count|     high_fare_share|high_fare_count|  avg_total_amount| avg_trip_distance| max_fare|min_fare|
    +--------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    |        East Village|          1|  Monday|    4191193| 0.05484691351603231|         229874|14.746358184888384|7.4885988881924686|  2759.37|     3.0|
    |Penn Station/Madi...|          1|  Monday|    3400808| 0.06788210331191881|         230854|15.107021913613405|   7.4215919569702|  1026.66|     2.8|
    |        Clinton East|          1|  Monday|    3341669| 0.07649889920276365|         255634|15.084945588563905|6.5619582998794845| 99619.02|     2.0|
    |Times Sq/Theatre ...|          1|  Monday|    3130290| 0.11675371930396225|         365473| 16.84883943340489|16.974902175197858|548463.35|     2.8|
    |            Union Sq|          1|  Monday|    2700344|0.048643802419247324|         131355| 13.39148741789698| 4.913931899046934|   8007.3|     3.0|
    |         JFK Airport|          1|  Monday|    2672880|  0.9004837478674688|        2406885| 58.78656007752754|29.677572487354414|   1722.3|     2.3|
    |        East Chelsea|          1|  Monday|    2671179| 0.06504431189373681|         173745|14.660237412016677| 7.372894572022323|   6979.8|     2.8|
    |Upper East Side S...|          1|  Monday|    2665438| 0.03955522506995098|         105432|12.316404909809908| 2.338687544035915|    490.3|     2.5|
    |   LaGuardia Airport|          1|  Monday|    2659554|  0.8362481077654373|        2224047|41.138734415623524|13.951282809072488|   6993.1|     2.0|
    |         Murray Hill|          1|  Monday|    2645349| 0.06616064647802615|         175018|14.241759586350943|14.768930882087782|    999.8|     2.8|
    +--------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    only showing top 10 rows


```pyspark
print("Stats all week days")

all_day_stats = get_trips_with_fares_over_30()
for day_num in day_names.keys():
    all_day_stats.filter(col("day_of_week") == day_num).show(10, truncate=False)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    Stats all week days
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    |pickup_zone                 |day_of_week|day_name|trips_count|high_fare_share     |high_fare_count|avg_total_amount  |avg_trip_distance |max_fare |min_fare|
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    |East Village                |1          |Monday  |4191193    |0.05484691351603231 |229874         |14.746358184888383|7.488598888192464 |2759.37  |3.0     |
    |Penn Station/Madison Sq West|1          |Monday  |3400808    |0.06788210331191881 |230854         |15.10702191361341 |7.421591956970198 |1026.66  |2.8     |
    |Clinton East                |1          |Monday  |3341669    |0.07649889920276365 |255634         |15.084945588563903|6.561958299879486 |99619.02 |2.0     |
    |Times Sq/Theatre District   |1          |Monday  |3130290    |0.11675371930396225 |365473         |16.848839433404887|16.97490217519785 |548463.35|2.8     |
    |Union Sq                    |1          |Monday  |2700344    |0.048643802419247324|131355         |13.39148741789698 |4.913931899046933 |8007.3   |3.0     |
    |JFK Airport                 |1          |Monday  |2672880    |0.9004837478674688  |2406885        |58.78656007752756 |29.67757248735441 |1722.3   |2.3     |
    |East Chelsea                |1          |Monday  |2671179    |0.06504431189373681 |173745         |14.660237412016686|7.372894572022322 |6979.8   |2.8     |
    |Upper East Side South       |1          |Monday  |2665438    |0.03955522506995098 |105432         |12.31640490980991 |2.338687544035914 |490.3    |2.5     |
    |LaGuardia Airport           |1          |Monday  |2659554    |0.8362481077654373  |2224047        |41.13873441562353 |13.951282809072488|6993.1   |2.0     |
    |Murray Hill                 |1          |Monday  |2645349    |0.06616064647802615 |175018         |14.241759586350943|14.768930882087778|999.8    |2.8     |
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    only showing top 10 rows
    
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    |pickup_zone                 |day_of_week|day_name|trips_count|high_fare_share     |high_fare_count|avg_total_amount  |avg_trip_distance |max_fare |min_fare|
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    |Upper East Side South       |2          |Tuesday |4018834    |0.031807733287814326|127830         |12.31111910568825 |8.80300298544304  |1102.9   |2.85    |
    |Midtown Center              |2          |Tuesday |3999124    |0.06393700220348256 |255692         |14.764790814187128|7.837510869880505 |963.06   |3.0     |
    |Upper East Side North       |2          |Tuesday |3673761    |0.03923581310814721 |144143         |13.018473008992768|5.890501298260805 |6980.8   |3.0     |
    |Midtown East                |2          |Tuesday |3600839    |0.061235450960179   |220499         |14.669606225102672|6.551085202643078 |37360.32 |3.0     |
    |Penn Station/Madison Sq West|2          |Tuesday |3505338    |0.056005440844791574|196318         |14.698241173887116|14.29692995368774 |952.81   |2.3     |
    |Times Sq/Theatre District   |2          |Tuesday |3180767    |0.09775629588712408 |310940         |16.49534335586113 |5.509353725060661 |669681.49|2.8     |
    |Murray Hill                 |2          |Tuesday |3137353    |0.06046912795595523 |189713         |14.356099794951993|2.345305437418105 |6987.3   |2.75    |
    |LaGuardia Airport           |2          |Tuesday |3132830    |0.8569510634155061  |2684682        |43.55108605638277 |14.843435152880936|6997.6   |2.8     |
    |Union Sq                    |2          |Tuesday |3023764    |0.04177144777171764 |126307         |13.752905597127679|6.520849897677201 |361772.02|2.8     |
    |JFK Airport                 |2          |Tuesday |2874316    |0.8999842049378008  |2586839        |59.32464513993104 |31.212563632530333|8452.8   |2.6     |
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    only showing top 10 rows
    
    +----------------------------+-----------+---------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    |pickup_zone                 |day_of_week|day_name |trips_count|high_fare_share     |high_fare_count|avg_total_amount  |avg_trip_distance |max_fare |min_fare|
    +----------------------------+-----------+---------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    |Upper East Side South       |3          |Wednesday|4664217    |0.03380803251649741 |157688         |12.764076285471392|1.8037879498316636|587.8    |2.5     |
    |Midtown Center              |3          |Wednesday|4436806    |0.06975738853580707 |309500         |15.374044573957274|2.457753221574259 |865.8    |2.8     |
    |Upper East Side North       |3          |Wednesday|4164977    |0.04106553289489954 |171037         |13.427330734357254|2.0120434686674153|6978.85  |3.0     |
    |Midtown East                |3          |Wednesday|4030203    |0.06599617934878219 |265978         |15.239454099457433|5.59742725118309  |1228.95  |0.5     |
    |Times Sq/Theatre District   |3          |Wednesday|3559970    |0.09367269948904064 |333472         |16.50911627345026 |5.030409818060239 |921.2    |2.8     |
    |Murray Hill                 |3          |Wednesday|3525656    |0.06122321633193936 |215852         |14.87982808021887 |4.9006081790169   |404114.66|2.8     |
    |Penn Station/Madison Sq West|3          |Wednesday|3478931    |0.058389775479881605|203134         |15.052549283096164|3.882119182587974 |4268.3   |2.3     |
    |Union Sq                    |3          |Wednesday|3468159    |0.0456740881833849  |158405         |14.231039652447189|3.659065907877902 |727.88   |2.8     |
    |Clinton East                |3          |Wednesday|3071268    |0.059690981054079294|183327         |14.527399269614826|6.053661588633766 |86330.55 |2.8     |
    |Lincoln Square East         |3          |Wednesday|3050225    |0.043144686047750576|131601         |14.151925720231205|2.481140286372319 |171863.58|3.0     |
    +----------------------------+-----------+---------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    only showing top 10 rows
    
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    |pickup_zone                 |day_of_week|day_name|trips_count|high_fare_share     |high_fare_count|avg_total_amount  |avg_trip_distance |max_fare |min_fare|
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    |Upper East Side South       |4          |Thursday|4826830    |0.03692278369033092 |178220         |13.016533329327107|4.470642038356435 |6981.3   |3.0     |
    |Midtown Center              |4          |Thursday|4478641    |0.07933076127334163 |355294         |16.09455889186117 |4.21745239236634  |861611.05|-36.6   |
    |Upper East Side North       |4          |Thursday|4251593    |0.04326895824694414 |183962         |13.681820938644151|3.1927679695587026|335545.21|3.0     |
    |Midtown East                |4          |Thursday|4148804    |0.07431635719595334 |308324         |15.72367194979539 |3.0092627995923658|8010.8   |2.7     |
    |Murray Hill                 |4          |Thursday|3693470    |0.06596777556065164 |243650         |15.080433145522608|4.254830498149438 |834.84   |3.0     |
    |Times Sq/Theatre District   |4          |Thursday|3655359    |0.10450519360752254 |382004         |17.188075351832147|3.2546033098253813|889.3    |2.3     |
    |Union Sq                    |4          |Thursday|3641688    |0.05087064020860656 |185255         |14.809501006124947|5.97403761387578  |588123.55|3.0     |
    |Penn Station/Madison Sq West|4          |Thursday|3528788    |0.06337161654369716 |223625         |15.390747027022458|2.7129173529268376|12891.25 |2.3     |
    |Clinton East                |4          |Thursday|3241541    |0.06453998268107669 |209209         |14.91706860101132 |5.4988122007403275|1104.79  |2.8     |
    |Lincoln Square East         |4          |Thursday|3208196    |0.046125610779391285|147980         |14.431221209674053|12.071265493130662|200006.3 |2.8     |
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+---------+--------+
    only showing top 10 rows
    
    +----------------------------+-----------+--------+-----------+-------------------+---------------+------------------+------------------+----------+--------+
    |pickup_zone                 |day_of_week|day_name|trips_count|high_fare_share    |high_fare_count|avg_total_amount  |avg_trip_distance |max_fare  |min_fare|
    +----------------------------+-----------+--------+-----------+-------------------+---------------+------------------+------------------+----------+--------+
    |Upper East Side South       |5          |Friday  |4794243    |0.03976456762829919|190641         |13.17616926801506 |2.7680115984942844|4010.3    |2.5     |
    |Midtown Center              |5          |Friday  |4351344    |0.08703862530749121|378735         |16.30214271039096 |7.986928999407985 |7660.32   |2.8     |
    |Upper East Side North       |5          |Friday  |4265205    |0.04665449843559688|198991         |13.69597500237227 |2.1458066376645433|8025.1    |2.8     |
    |Midtown East                |5          |Friday  |4090151    |0.08364434467089357|342118         |16.166887437651997|4.857828214655154 |20974.76  |2.0     |
    |Murray Hill                 |5          |Friday  |3801013    |0.0723391369616468 |274962         |15.40182798111837 |5.650059478881032 |4705.75   |2.5     |
    |Union Sq                    |5          |Friday  |3606871    |0.0563837741909816 |203369         |14.891524088328351|2.642143339753487 |5219.01   |3.0     |
    |Times Sq/Theatre District   |5          |Friday  |3579578    |0.11089742980876517|396966         |17.538418182254414|7.7338147150306344|818.56    |2.3     |
    |Penn Station/Madison Sq West|5          |Friday  |3542472    |0.06714576713662099|237862         |15.601824045466797|2.339049245837367 |7010.85   |2.3     |
    |Clinton East                |5          |Friday  |3386751    |0.06951204856808192|235420         |15.26089200239148 |2.78994749244925  |186132.54 |2.8     |
    |Lincoln Square East         |5          |Friday  |3233641    |0.04951384522895399|160110         |15.41976639026762 |6.734719098378561 |2554370.48|3.0     |
    +----------------------------+-----------+--------+-----------+-------------------+---------------+------------------+------------------+----------+--------+
    only showing top 10 rows
    
    +----------------------------+-----------+--------+-----------+-------------------+---------------+------------------+------------------+--------+--------+
    |pickup_zone                 |day_of_week|day_name|trips_count|high_fare_share    |high_fare_count|avg_total_amount  |avg_trip_distance |max_fare|min_fare|
    +----------------------------+-----------+--------+-----------+-------------------+---------------+------------------+------------------+--------+--------+
    |Upper East Side South       |6          |Saturday|4336971    |0.03733896306892529|161938         |12.893960259358957|9.558571069532201 |7003.85 |2.0     |
    |Upper East Side North       |6          |Saturday|4045857    |0.04528039424033029|183198         |13.463534657797396|3.409721196275609 |1410.66 |3.0     |
    |Midtown Center              |6          |Saturday|3974784    |0.08143587173542009|323690         |15.878286915716373|4.4261221465116   |648.56  |-69.7   |
    |Murray Hill                 |6          |Saturday|3688852    |0.0678652870866058 |250345         |15.07237378186859 |15.622355659701178|6007.3  |2.3     |
    |Midtown East                |6          |Saturday|3647685    |0.08070324054845744|294380         |15.852462501557827|12.745998886965236|795.41  |3.0     |
    |Penn Station/Madison Sq West|6          |Saturday|3596137    |0.06450532891266378|231970         |15.379218628209413|8.123382312742821 |1242.3  |2.5     |
    |Union Sq                    |6          |Saturday|3561446    |0.05429339655858884|193363         |14.52797388195483 |3.117409041159124 |8011.3  |2.8     |
    |Clinton East                |6          |Saturday|3514104    |0.06991938770167302|245704         |15.196180761864653|10.222809236721515|1050.85 |2.3     |
    |East Village                |6          |Saturday|3474372    |0.05668333730527416|196939         |15.140302880633618|5.247255023353895 |995.9   |2.8     |
    |Times Sq/Theatre District   |6          |Saturday|3467129    |0.11052141411525213|383192         |17.37849135408483 |3.0352340596499285|1532.58 |3.0     |
    +----------------------------+-----------+--------+-----------+-------------------+---------------+------------------+------------------+--------+--------+
    only showing top 10 rows
    
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+--------+--------+
    |pickup_zone                 |day_of_week|day_name|trips_count|high_fare_share     |high_fare_count|avg_total_amount  |avg_trip_distance |max_fare|min_fare|
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+--------+--------+
    |East Village                |7          |Sunday  |5002608    |0.051153118533372996|255899         |14.786271304888556|5.507951006754898 |899.68  |2.8     |
    |Clinton East                |7          |Sunday  |3736013    |0.0658969869751524  |246192         |14.792289389782761|5.2588919899368705|94017.93|2.3     |
    |Upper East Side South       |7          |Sunday  |3451369    |0.031959202275966436|110303         |12.161531542987776|4.71071238108704  |22544.11|3.0     |
    |Union Sq                    |7          |Sunday  |3386923    |0.04324515201556103 |146468         |13.564338684401948|3.3626639814368398|6983.8  |3.0     |
    |Times Sq/Theatre District   |7          |Sunday  |3316978    |0.08656795432468953 |287144         |15.69842043872167 |6.414570259434942 |844.8   |2.3     |
    |Penn Station/Madison Sq West|7          |Sunday  |3307109    |0.06095777308821693 |201594         |14.826262379011203|12.702918957312878|848.3   |2.3     |
    |Lincoln Square East         |7          |Sunday  |3209215    |0.04186942912830708 |134368         |13.476296608357815|8.4350319034406   |724.24  |3.0     |
    |Upper East Side North       |7          |Sunday  |3179370    |0.03933829658076914 |125071         |12.688551659602917|5.505057712691486 |7002.3  |2.8     |
    |Murray Hill                 |7          |Sunday  |3177358    |0.050432151491899876|160241         |13.875150200258073|8.638238523956112 |1014.34 |2.8     |
    |Midtown Center              |7          |Sunday  |3102705    |0.05657643894601646 |175540         |14.138970176020749|11.052112469603165|630.08  |2.8     |
    +----------------------------+-----------+--------+-----------+--------------------+---------------+------------------+------------------+--------+--------+
    only showing top 10 rows


```pyspark
print("Save result task 4")
output_path = f"s3://pzhoholiev-emr-bucket-hw/results/zone_days_statistic/{current_date}/zone_days_statsti.parquet"

print(f"Saving zone day statistics to: {output_path}")
zone_day_stats_final.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_path)

print("Zone day statistics saved successfully!")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    Save result task 4
    Saving zone day statistics to: s3://pzhoholiev-emr-bucket-hw/results/zone_days_statistic/2025-07-19/zone_days_statsti.parquet
    Zone day statistics saved successfully!


```pyspark

```
