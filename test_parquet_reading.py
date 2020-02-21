import os
import time
#import configparser
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id

def create_spark_session():
    # don't seem to need the config line:
    # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


spark = create_spark_session()
# should speed up s3 writes
# https://stackoverflow.com/a/42834182/4549682
sc = spark.sparkContext
# turn off info logging
# https://stackoverflow.com/a/40504350/4549682
sc.setLogLevel('WARN')
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

input_data = "s3a://udacity-dend/"
output_data = "s3a://udacity-dend-spark-dwh2/"

# read in song data to use for songplays table
print('reading songs parquet tables...')
start = time.time()
songs_df = spark.read.parquet(output_data + 'songs/*/*/*')
end = time.time()
print('took', int(end-start), 's to read')

# read in artist data
print('reading artists parquet tables...')
start = time.time()
artists_df = spark.read.parquet(output_data + 'artists/*')
end = time.time()
print('took', int(end-start), 's to read')
