import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    '''Reading Data From S3 and Creating Songs and Artist Tables. 
       Saving Tables in their own folders as parquet files on S3 '''
  
    # Loading Data
    song_data = 's3a://udacity-dend/song_data/*/*/*/*.json'
    df = spark.read.json(song_data)

    # Songs Table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'Songs Table'))

    # Artist Table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').distinct()
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'Artists Table'))

def process_log_data(spark, input_data, output_data):
    
    '''Reading Data From S3 and Creating SongPlays, Users and Timestamp tables
        Saving Tables in S3 to Separate Folderes as Parquet Files'''
    
    #Loading Data
    log_data = 's3a://udacity-dend/log-data/*/*/*.json'
    df = spark.read.json(log_data)
    
    # Filtering Data By Page Is Equal To Next Song
    df = df.where(df.page == 'NextSong')

    # Artist Table  
    artists_table = df.select('userId', 'firstName','lastName', 'gender', 'level').distinct()
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, "Users Table"))

    # Creating Timestamp Column
    get_timestamp = udf(lambda epoch: datetime.fromtimestamp(epoch/1000.0))
    df = df.withColumn('timestamp', get_timestamp('ts'))
 
    # TimeStamp Table
    time_table = df.selectExpr('timestamp as start_time',
                              'hour(timestamp) as hour',
                              'dayofmonth(timestamp) as day',
                              'weekofyear(timestamp) as week',
                              'month(timestamp) as month',
                              'year(timestamp) as year',
                              'dayofweek(timestamp) as weekday').distinct().orderBy('timestamp')

    time_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data, 'TimeStamp Table'))

    # Loading Data
    song_df = spark.read.json('s3a://udacity-dend/song_data/*/*/*/*.json')

    # SongPlays Table 
    cond=[df.song == song_df.title, df.artist == song_df.artist_id, df.length == song_df.duration]
    songplays_table = df.join(song_df, cond, 'left_outer').select(col("ts").alias("start_time"), col("userId").alias("user_id"), df.level, song_df.song_id, song_df.artist_id, df.itemInSession, df.location, df.userAgent).distinct()

    dt = col("start_time").cast("timestamp")
    fname = [(year, "year"), (month, "month"), (dayofmonth, "day")]
    exprs = [col("*")] + [f(dt).alias(name) for f, name in fname]
    
    songplays_table.select(*exprs).write.mode('overwrite').partitionBy(*(name for _, name in fname)).parquet(os.path.join(output_data, 'SongPlays Table'))


    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sf-dl-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
