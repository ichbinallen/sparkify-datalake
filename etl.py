import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data, mode="overwrite"):
   # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    song_fields = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(song_fields)
    songs_table = songs_table.withColumn('year', F.col('year').cast(IntegerType()))
    songs_table = songs_table.withColumn('duration', F.col('duration').cast(DoubleType())) 
   
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
        .mode(mode) \
        .partitionBy('year', 'artist_id') \
        .parquet(output_data + '/star_schema/song_table')

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
    artists_table.write \
        .mode(mode) \
        .partitionBy('artist_id') \
        .parquet(output_data + '/star_schema/artist_table')


def process_log_data(spark, input_data, output_data, mode="overwrite"):
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(F.col('page')=='NextSong')
    df = df.withColumn(
        'userId', col('userId').cast(LongType())
    )
    df = df.withColumn(
        'registration', 
        (F.round(col('registration')/1000)).cast(TimestampType())
    )
    df = df.withColumn(
        'ts', 
        (F.round(col('ts')/1000)).cast(TimestampType())
    )

    # extract columns for users table    
    users_table = df.selectExpr(
        'userId AS user_id',
        'firstName AS first_name',
        'lastName AS last_name',
        'gender AS gender') \
        .dropDuplicates(['user_id'])

    # write users table to parquet files
    users_table.write \
        .mode(mode) \
        .partitionBy('user_id') \
        .parquet(output_data + '/star_schema/user_table')
    
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
    time_table.write \
        .mode(mode) \
        .partitionBy('year', 'month', 'day') \
        .parquet(output_data + '/star_schema/time_table')


    # read in song data to use for songplays table)
    song_table = spark.read.parquet(output_data + '/star_schema/song_table')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.selectExpr(
        'ts as start_time',
        'userId as user_id',
        'level',
        'song', # join to song_id from songs_df
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
        song_table.title==songplays_table.song, how='left'
    ).select([
        'songplay_id','start_time', 'songplay_year', 'month',
        'user_id', 'level', 'song_id', 'artist_id',
        'session_id', 'location', 'user_agent'
    ]).withColumnRenamed('songplay_year', 'year')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
        .mode(mode) \
        .partitionBy('year', 'month') \
        .parquet(output_data + '/star_schema/songplay_table')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
