import configparser
from datetime import datetime
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

is_aws_access_keys_set = 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' in os.environ
is_aws_profile_set = 'AWS_PROFILE' in os.environ

song_schema = StructType([
    StructField('num_songs', IntegerType()),
    StructField('artist_id', StringType()),
    StructField('artist_latitude', DoubleType()),
    StructField('artist_longitude', DoubleType()),
    StructField('artist_location', StringType()),
    StructField('artist_name', StringType()),
    StructField('song_id', StringType()),
    StructField('title', StringType()),
    StructField('duration', DoubleType()),
    StructField('year', IntegerType()),
])

event_schema = StructType([
    StructField('artist', StringType()),
    StructField('auth', StringType()),
    StructField('firstName', StringType()),
    StructField('gender', StringType()),
    StructField('itemInSession', IntegerType()),
    StructField('lastName', StringType()),
    StructField('length', DoubleType()),
    StructField('level', StringType()),
    StructField('location', StringType()),
    StructField('method', StringType()),
    StructField('page', StringType()),
    StructField('registration', StringType()),
    StructField('sessionId', StringType()),
    StructField('song', StringType()),
    StructField('status', IntegerType()),
    StructField('ts', DoubleType()),
    StructField('userAgent', StringType()),
    StructField('userId', StringType()),
])

if (not is_aws_access_keys_set and not is_aws_profile_set):
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def save_parquet(df, save_dir, partition_columns):
    df.write.mode('overwrite').partitionBy(*partition_columns).parquet(save_dir)


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*/*/*/*.json')

    # read song data file
    df = spark.read.option('recursiveFileLookup', 'true').json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select(
        col('song_id'),
        col('title'),
        col('artist_id'),
        col('year'),
        col('duration')
    ).dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_dir = os.path.join(output_data, 'songs', 'songs.parquet')
    save_parquet(songs_table, songs_dir, ['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select(
        col('artist_id'),
        col('artist_name').alias('name'),
        col('artist_location').alias('location'),
        col('artist_latitude').alias('latitude'),
        col('artist_longitude').alias('longitude')
    ).dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_dir = os.path.join(output_data, 'artists', 'artists.parquet')
    save_parquet(artists_table, artists_dir, [])

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data')

    # read log data file
    df = spark.read.json(log_data, schema=event_schema)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        col('gender').alias('gender'),
        col('level').alias('level')
    ).dropDuplicates(['user_id'])

    # write users table to parquet files
    users_dir = os.path.join(output_data, 'users', 'users.parquet')
    save_parquet(users_table, users_dir, [])

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts / 1000), DateType())
    df = df.withColumn('date', get_datetime('ts'))

    # extract columns to create time table
    time_table = df.select(
        col('ts').alias('start_time'),
        hour(df.date).alias('hour'),
        dayofmonth(df.date).alias('day'),
        weekofyear(df.date).alias('week'),
        month(df.date).alias('month'),
        year(df.date).alias('year'),
        dayofweek(df.date).alias('weekday')
    ).dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    time_dir = os.path.join(output_data, 'time', 'time.parquet')
    save_parquet(time_table, time_dir, [])

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data', '*/*/*/*.json')
    song_df = spark.read.option('recursiveFileLookup', 'true').json(song_data, schema=song_schema)
    song_event_df = song_df.join(df, (df.artist == song_df.artist_name))
    song_event_df = song_event_df.withColumn("id", monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = song_event_df.select(
        col('id').alias('songplay_id'),
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        'artist_id',
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
    )

    songplays_table = songplays_table.withColumn('year', year(get_timestamp('start_time')))
    songplays_table = songplays_table.withColumn('month', month(get_timestamp('start_time')))

    # write songplays table to parquet files partitioned by year and month
    songplay_dir = os.path.join(output_data, 'songplay', 'songplay.parquet')
    save_parquet(songplays_table, songplay_dir, ['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = config['S3']['INPUT_FOLDER'] if config['S3']['INPUT_FOLDER'] else './data'
    output_data = config['S3']['OUTPUT_FOLDER'] if config['S3']['OUTPUT_FOLDER'] else './analytics'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
