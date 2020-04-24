import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
   # get filepath to song data file
    song_data = input_data + "/data/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    song_fields = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(song_fields)
    songs_table = songs_table.withColumn('year', F.col('year').cast(IntegerType()))
    songs_table = songs_table.withColumn('duration', F.col('duration').cast(DoubleType())) 
   
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
        .mode('append') \
        .parquet(output_data + '/data/star_schema/song_table')

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
        .mode('append') \
        .parquet(output_data + '/data/star_schema/artist_table')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
