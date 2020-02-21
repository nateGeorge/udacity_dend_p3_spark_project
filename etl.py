import os
import time
#import configparser
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id

# config = configparser.ConfigParser()
# config.read('dl.cfg')
#
# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    # don't seem to need the config line:
    # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads song JSON files and creates song and artists tables
    for star schema DB as parquet files in HDFS or S3.
    """
    # get filepath to song data file
    # os.path.join not working for some reason...
    # song_data = os.path.join(input_data, '/song_data/*/*/*/*.json')
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data -- takes about 3 minutes on a 3-node 5m.xl cluster
    start = time.time()
    print('reading song data...')
    df = spark.read.json(song_data)
    end = time.time()
    print('took', int(end-start), 'seconds')

    start = time.time()
    # extract columns to create songs table
    songs_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(songs_cols).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    print('writing songs table...')
    # songs_table.head().write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs'))
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs')
    end = time.time()
    print('took', int(end-start), 's to write songs table')

    start = time.time()
    # extract columns to create artists table
    artists_cols = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = df.select(artists_cols).dropDuplicates()

    # write artists table to parquet files
    print('writing artists table...')
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')
    end = time.time()
    print('took', int(end-start), 's to write artists table')


def process_log_data(spark, input_data, output_data):
    """
    Loads log JSON files and creates users, time, and songplays tables
    for star schema DB as parquet files in HDFS or S3.
    """
    # get filepath to log data file
    # on s3, paths are log_data/year/month/date.json
    # in workspace, path is logdata/date.json
    # s3 path
    log_data = input_data + 'log_data/*/*/*.json'
    # workspace path
    # log_data = input_data + '/log_data/*.json'

    # read log data file
    start = time.time()
    print('reading logs data...')
    df = spark.read.json(log_data)
    end = time.time()
    print('took', int(end-start), 'seconds')

    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    start = time.time()
    # extract columns for users table
    users_cols = ['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = df.select(users_cols).dropDuplicates()

    # write users table to parquet files
    print('writing users table...')
    users_table.write.mode('overwrite').parquet(output_data + '/users')
    end = time.time()
    print('took', int(end-start), 's to write users table')

    # create datetime column from original timestamp column
    # timestamp is is ms
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('start_time', get_datetime('ts'))

    # extract columns to create time table

    time_table = df.select("start_time").dropDuplicates() \
                            .withColumn("hour", hour(col("start_time"))) \
                            .withColumn("day", dayofmonth(col("start_time"))) \
                            .withColumn("week", weekofyear(col("start_time"))) \
                            .withColumn("month", month(col("start_time"))) \
                            .withColumn("year", year(col("start_time"))) \
                            .withColumn("weekday", date_format(col("start_time"), 'E'))

    start = time.time()
    # write time table to parquet files partitioned by year and month
    print('writing time table...')
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time')
    end = time.time()

    print('took', int(end-start), 's to write time table')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'songs/*/*/*')

    # read in artist data
    artists_df = spark.read.parquet(output_data + 'artists/*')

    # extract columns from joined song and log datasets to create songplays table
    # columns desired: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    # we are joining the songs and artists data with logs to get the song_id and artist_id from the songs data
    # need to join on song title and artist name

    # first join songs and logs dfs on song title
    songs_logs_df = df.join(songs_df, (df.song == songs_df.title))
    # next join that df with artists on artist name
    artists_songs_logs_df = songs_logs_df.join(artists_df, (songs_logs_df.artist == artists_df.artist_name))


    songplay_cols = ['start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    # calculate year and month from start_time -- probably faster than a join on the time table
    songplays_table = artists_songs_logs_df.select(songplay_cols) \
                        .withColumn('songplay_id', monotonically_increasing_id()) \
                        .withColumn("month", month(col("start_time"))) \
                        .withColumn("year", year(col("start_time")))

    # write songplays table to parquet files partitioned by year and month
    start = time.time()
    print('writing songplays table...')
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays')
    end = time.time()
    print('took', int(end-start), 's to write songplays table')


def main():
    """
    Creates spark session, loads song and log data, and creates parquet tables
    with star schema DB.
    """
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
    # output_data = "hdfs:///dwh/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
