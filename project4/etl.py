import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


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


def process_song_data(spark, input_data, output_data):
    print('begin process song data')
    # get filepath to song data file
    song_data = input_data + 'song-data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "duration", "year"]).dropDuplicates()
    
    print('save song table')
    # write songs table to parquet files partitioned by year and artist
    songs_table.repartition("year", "artist_id").write.mode("append").partitionBy("year", "artist_id").parquet(output_data+'songs_table')
    
    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", 'artist_latitude' ,'artist_longitude']).dropDuplicates()
    
    print('save artist table')
    # write artists table to parquet files
    artists_table.write.save(output_data+'artists_table', format='parquet', mode='append')
    print('done')


def process_log_data(spark, input_data, output_data):
    print("begin process log data")
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", 'lastName', 'location', 'gender']).dropDuplicates()
    
    print('save user table')
    # write users table to parquet files
    users_table.write.save(output_data+'users_table', format='parquet', mode='append')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000.0 )
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("datet", get_datetime("timestamp"))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour(df.datet)) \
    .withColumn("year", year(df.datet)) \
    .withColumn("day", dayofmonth(df.datet)) \
    .withColumn("week", weekofyear(df.datet)) \
    .withColumn("month", month(df.datet)) \
    .withColumn("weekday", dayofweek(df.datet)) \
    .withColumnRenamed('ts', 'start_time') \
    .select(['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']) \
    .dropDuplicates()
    
    print('save time table')
    # write time table to parquet files partitioned by year and month
    time_table.repartition("year", "month").write.mode("append").partitionBy("year", "month").parquet(output_data+'time_table')
    
    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song-data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, [df.song == song_df.title, df.length == song_df.duration, df.artist == song_df.artist_name]) \
    .select(df.ts, df.userId, df.level, song_df.song_id, song_df.artist_id, df.sessionId, df.location, df.userAgent, df.datet) \
    .withColumn("year", year(df.datet)) \
    .withColumn("month", month(df.datet))

    print('save songplays table')
    # write songplays table to parquet files partitioned by year and month
    songplays_table.repartition("year", "month").write.mode("append").partitionBy("year", "month").parquet(output_data+'songplays_table')
    print('done.')


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "./data/"
    output_data = "./temp/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
