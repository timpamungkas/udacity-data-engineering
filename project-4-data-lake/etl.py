import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,
                                   date_format, dayofweek, max, monotonically_increasing_id)
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, 
                               IntegerType, TimestampType)
import pandas as pd


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """Create default spark session"""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data into analytic.
    
    Flows:
    1. Read song data from udacity S3
    2. Define song schema and artist table for analytic.
    3. Writes the artist table into parquet files on pre-defined S3bucket
    
    Arguments:
        spark: Spark session
        input_data: S3 bucket that contains song data
        output_data: S3 bucket to write output parquet files
    """

    # get filepath to song data file
    song_data = input_data + "song_data/A/A/B/*.json"
    
    # read song data file
    song_data_schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", StringType(), True),
        StructField("artist_longitude", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), False),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", DoubleType(), False),
        StructField("year", IntegerType(), False)
    ])
    
    print("Start read s3 song data")    
    df = spark.read.json(song_data, schema=song_data_schema)
    
    # extract columns to create songs table
    print("Start create songs table")
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    print("Start write songs table to parquet")
    songs_table.write.parquet(output_data + "songs_table.parquet", mode="overwrite", partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    # needs to distinct artist (can duplicate)
    print("Start create artists table")
    artists_table = df.select("artist_id", "artist_name", "artist_location", 
                              "artist_latitude", "artist_longitude").distinct()
    
    # write artists table to parquet files
    print("Start write artists table to parquet")
    artists_table.write.parquet(output_data + "artists_table.parquet", mode="overwrite")

def process_log_data(spark, input_data, output_data):
    """
    Process log data into analytic.
    
    Flows:
    1. Read log data from udacity S3
    2. Define log schema and tables for analytic (songplay, user, time).
    3. Writes the artist table into parquet files on pre-defined S3bucket
    
    Arguments:
        spark: Spark session
        input_data: S3 bucket that contains log data
        output_data: S3 bucket to write output parquet files
    """

    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    log_data_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), False),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), False),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), False),
        StructField("location", StringType(), True),
        StructField("method", StringType(), False),
        StructField("page", StringType(), False),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), False),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), False),
        StructField("ts", DoubleType(), False),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    print("Start read s3 log data")    
    df = spark.read.json(log_data, schema=log_data_schema)
    
    # filter by actions for song plays
    print("Start filter for song plays")    
    df = df.filter(col("page") == "NextSong")

    # extract columns for users table
    print("Start extract columns for users table")    
    users_table = df.filter((col("userID") != "") & (col("userID").isNotNull())).select(
        "userID", "firstName", "lastName", "gender", "level")
    
    # write users table to parquet files
    print("Start write users table to parquet files")    
    users_table.write.parquet(output_data + "users_table.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    print("Start create timestamp column from original timestamp column")    
    get_timestamp = udf(
        lambda x: x/1000,
        DoubleType()
    )
    df = df.withColumn("start_timestamp", get_timestamp("ts")) 
    
    # create datetime column from original timestamp column
    print("Start create datetime column from original timestamp column")
    get_datetime = udf(
        lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),
        TimestampType()
    )
    df = df.withColumn("start_datetime", get_datetime("ts")) 
    
    # extract columns to create time table
    print("Start extract columns to create time table")
    time_table = df.withColumn(
        "hour", hour("start_datetime")).withColumn(
        "day", dayofmonth("start_datetime")).withColumn(
        "week", weekofyear("start_datetime")).withColumn(
        "month", month("start_datetime")).withColumn(
        "year", year("start_datetime")).withColumn(
        "weekday", dayofweek("start_datetime")).select(
        "start_datetime", "hour", "day", "week", "month", "year", "weekday").distinct()
    
    # write time table to parquet files partitioned by year and month
    print("Start write time table to parquet files partitioned by year and month")
    time_table.write.parquet(output_data + "time_table.parquet", mode="overwrite", partitionBy=["year", "month"])

    # read in song data to use for songplays table
    print("Start read in song data to use for songplays table")
    song_df = spark.read.parquet(output_data + "songs_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    print("Start extract columns from joined song and log datasets to create songplays table ")
    artists_table = spark.read.parquet(output_data + "artists_table.parquet")
    songs = song_df.join(artists_table, "artist_id", "full").select(
        "song_id", "title", "artist_id", "name", "duration")
    
    songplays_table = df.join(songs, [df.song == songs.title, df.artist == songs.name, df.length == songs.duration], "left")
    
    songplays_table = songplays_table.join(time_table, "start_datetime", "left").select(
        "start_datetime", "userId", "level", "song_id", "artist_id", "sessionId",
        "location", "userAgent", "year", "month").withColumn("songplay_id", monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays_table.parquet", mode="overwrite", partitionBy=["year", "month"])

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-project-4-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()