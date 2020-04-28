import configparser
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, LongType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import TimestampType
# StructType, StructField, StringType, IntegerType,
# FloatType, TimestampType, LongType, DoubleType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Creates and returns the SparkSession object """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data, mode="overwrite"):
    """ Transfers song data from OLTP format to star schema format.

    This function transfers the songs and artists tables.

    Keyword arguments:
    spark -- SparkSession object
    input_data -- file address for OLTP logs on Udacity S3 bucket
    output_data -- file address for Star Schema location on personal S3 bucket
    mode -- overwrite or append option passed to write.parquet
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    print("reading song logs from {}".format(song_data))
    df = spark.read.json(song_data)

    # extract columns to create songs table
    song_fields = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(song_fields)
    songs_table = songs_table.withColumn('year', F.col('year').cast(IntegerType()))
    songs_table = songs_table.withColumn('duration', F.col('duration').cast(DoubleType()))

    # write songs table to parquet files partitioned by year and artist
    song_path = output_data + 'star_schema/song_table'
    print("Writing Song Table to {}".format(song_path))
    songs_table.write \
        .mode(mode) \
        .partitionBy('year', 'artist_id') \
        .parquet(song_path)

    # extract columns to create artists table
    artist_fields = [
        'artist_id', 'artist_name',
        'artist_location', 'artist_latitude', 'artist_longitude'
    ]
    artists_table = df.select(artist_fields)
    artists_table = artists_table.withColumnRenamed(
        'artist_name', 'name'
    )
    artists_table = artists_table.withColumnRenamed(
        'artist_location', 'location'
    )
    artists_table = artists_table.withColumn(
        'latitude',
        F.col('artist_latitude').cast(DoubleType())
    )
    artists_table = artists_table.withColumn(
        'longitude',
        F.col('artist_longitude').cast(DoubleType())
    )
    artist_col_names = [
        'artist_id', 'name', 'location', 'latitude', 'longitude'
    ]
    artists_table = artists_table.select(artist_col_names)

    # write artists table to parquet files
    artists_path = output_data + 'star_schema/artist_table'
    print("Writing Artist Table to {}".format(artists_path))
    artists_table.write \
        .mode(mode) \
        .partitionBy('artist_id') \
        .parquet(artists_path)


def process_log_data(spark, input_data, output_data, mode="overwrite"):
    """ Transfers event log data from OLTP format to star schema format.

    This function transfers the users, time, and songplay tables.

    Keyword arguments:
    spark -- SparkSession object
    input_data -- file address for OLTP logs on Udacity S3 bucket
    output_data -- file address for Star Schema location on personal S3 bucket
    mode -- overwrite or append option passed to write.parquet
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"
    # read log data file
    print("reading event logs from {}".format(log_data))
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(F.col('page') == 'NextSong')
    df = df.withColumn(
        'userId', F.col('userId').cast(LongType())
    )
    df = df.withColumn(
        'registration',
        (F.round(F.col('registration')/1000)).cast(TimestampType())
    )
    df = df.withColumn(
        'ts',
        (F.round(F.col('ts')/1000)).cast(TimestampType())
    )

    # extract columns for users table
    users_table = df.selectExpr(
        'userId AS user_id',
        'firstName AS first_name',
        'lastName AS last_name',
        'gender AS gender') \
        .dropDuplicates(['user_id'])

    # write users table to parquet files
    users_path = output_data + 'star_schema/user_table'
    print("Writing Users Table to {}".format(users_path))
    users_table.write \
        .mode(mode) \
        .partitionBy('user_id') \
        .parquet(users_path)

    # extract columns to create time table
    time_table = df.selectExpr('ts AS start_time') \
        .dropDuplicates() \
        .orderBy('start_time', ascending=True) \
        .withColumn('hour', F.hour('start_time')) \
        .withColumn('day', F.dayofmonth('start_time')) \
        .withColumn('week', F.weekofyear('start_time')) \
        .withColumn('month', F.month('start_time')) \
        .withColumn('year', F.year('start_time')) \
        .withColumn('weekday', F.dayofweek('start_time'))

    # write time table to parquet files partitioned by year and month
    times_path = output_data + 'star_schema/time_table'
    print("Writing Time Table to {}".format(times_path))
    time_table.write \
        .mode(mode) \
        .partitionBy('year', 'month', 'day') \
        .parquet(times_path)

    # read in song data to use for songplays table)
    songs_path = output_data + 'star_schema/song_table'
    print("Reading Songs table for join query from {}".format(songs_path))
    song_table = spark.read.parquet(songs_path)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.selectExpr(
        'ts as start_time',
        'userId as user_id',
        'level',
        'song',  # join to song_id from songs_df
        # artist_id # join from songs df
        'sessionId as session_id',
        'location',
        'userAgent as user_agent'
    )
    songplays_table = songplays_table \
        .withColumn('songplay_id', F.monotonically_increasing_id()) \
        .withColumn('songplay_year', F.year('start_time')) \
        .withColumn('month', F.month('start_time'))
    songplays_table = songplays_table.join(
        song_table,
        song_table.title == songplays_table.song, how='left'
    ).select([
        'songplay_id', 'start_time', 'songplay_year', 'month',
        'user_id', 'level', 'song_id', 'artist_id',
        'session_id', 'location', 'user_agent'
    ]).withColumnRenamed('songplay_year', 'year')

    # write songplays table to parquet files partitioned by year and month
    songplays_path = output_data + 'star_schema/songplay_table'
    print("Writing Songplays Table to {}".format(songplays_path))
    songplays_table.write \
        .mode(mode) \
        .partitionBy('year', 'month') \
        .parquet(songplays_path)


def main():
    """ Main method moves data from json files to datalake. """
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    input_data = "./data/"
    output_data = "./data/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
