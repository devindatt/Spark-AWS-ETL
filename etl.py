import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import  to_timestamp, from_unixtime, unix_timestamp

def create_env():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_path = input_data + "/song-data/*/*/*/"
    print(song_path)
    
    # read song data file
    print('####debugging: process_song_data, before reading in song data file')
    df_song = spark.read.json(song_path)
    df_song.createOrReplaceTempView("song_data")

    '''
     The df_song schema as a reference:
     |-- artist_id: string (nullable = true)
     |-- artist_latitude: double (nullable = true)
     |-- artist_location: string (nullable = true)
     |-- artist_longitude: double (nullable = true)
     |-- artist_name: string (nullable = true)
     |-- duration: double (nullable = true)
     |-- num_songs: long (nullable = true)
     |-- song_id: string (nullable = true)
     |-- title: string (nullable = true)
     |-- year: long (nullable = true)
    '''
    print('####debugging: process_song_data, after reading in song data file')

    
    #----------------------------------------------------------------------------    
    # extract the following columns to create songs table using spark SQL
    # get fields: song_id, title, artist_id, year, duration from:

    print('####debugging: process_song_data, before sql query of song_table')

    songs_table = spark.sql("""
    SELECT DISTINCT
        song_id, 
        title, 
        artist_id, 
        year, 
        duration 
    FROM song_data """)

    print('####debugging: process_song_data, after sql query of song_table')


    # write songs table to csv files
    out_path = "s3a://dend-bucket-ddatt/sparkify_songs_table_small.csv"
    songs_table.write.save(out_path, format = 'csv', header = True)

    # write songs table to parquet files partitioned by year and artist
    print('####debugging: process_song_data, before writing parquet song_table ')
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data,"songs"), "overwrite")
    print('####debugging: process_song_data, after writing parquet song_table ')

    
    #----------------------------------------------------------------------------
    #  extract the following columns to create artists table using spark SQL
    # get fields: artist_id, name, location, lattitude, longitude

    print('####debugging: process_song_data, before sql query of artists_table')

    artists_table = spark.sql("""
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude, 
        artist_longitude 
    FROM song_data """)

    print('####debugging: process_song_data, after sql query of artists_table')

    
    # write songs table to csv files
    out_path = "s3a://dend-bucket-ddatt/sparkify_artists_table_small.csv"
    artists_table.write.save(out_path, format = 'csv', header = True)
        
    # write artists table to parquet files partitioned by artist_id
    print('####debugging: process_song_data, before writing parquet song_table ')
    artists_table.write.partitionBy("artist_id").parquet(os.path.join(output_data,"artists"), "overwrite")
    print('####debugging: process_song_data, after writing parquet song_table ')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_path = input_data + "/log-data/*.json"
    print(log_path)

    # read log data file

    print('####debugging: process_log_data, before reading in log data file')
    
    df_log = spark.read.json(log_path)
    df_log.createOrReplaceTempView("log_data")
    
    print('####debugging: process_log_data, after reading in log data file')


    '''    
    log_data.location,
    log_data.lastName,
    log_data.method,
    log_data.status,
    log_data.length,
    log_data.sessionId,
    log_data.page,
    log_data.song,
    log_data.gender,
    log_data.userId,
    log_data.userAgent,
    log_data.registration,
    log_data.firstName,
    log_data.ts,
    log_data.itemInSession,
    log_data.artist,
    log_data.auth,
    log_data.level
    '''
    

    #----------------------------------------------------------------------------
    # filter by actions for song plays
    # get all df_log files associated when page='NextSong'

    print('####debugging: process_log_data, before sql query of df where page==NextSong ')
    
#    df.filter(df['page'] = 'NextSong')
    df = spark.sql("""
    SELECT DISTINCT * 
    FROM log_data
    WHERE page = 'NextSong' """)

    print('####debugging: process_log_data, after sql query of df ')


    #Output log data as a csv to confirm the schema    
    print('####debugging: process_log_data, before writing csv logdata_byNextSong ')
    out_path = "s3a://dend-bucket-ddatt/df_byNextSong.csv"
    df.write.save(out_path, format = 'csv', header = True)
    print('####debugging: process_log_data, after writing csv logdata_byNextSong ')

    
    
    df.printSchema()

    '''
    The df_log schema as a reference:
    |-- artist: string (nullable = true)
    |-- auth: string (nullable = true)
    |-- firstName: string (nullable = true)
    |-- gender: string (nullable = true)
    |-- itemInSession: long (nullable = true) 
    |-- lastName: string (nullable = true)
    |-- length: double (nullable = true)
    |-- level: string (nullable = true)
    |-- location: string (nullable = true)
    |-- method: string (nullable = true)
    |-- page: string (nullable = true)
    |-- registration: double (nullable = true)
    |-- sessionId: long (nullable = true)
    |-- song: string (nullable = true)
    |-- status: long (nullable = true)
    |-- ts: long (nullable = true)
    |-- userAgent: string (nullable = true)
    |-- userId: string (nullable = true)
    '''

    
    #----------------------------------------------------------------------------
    # extract columns for users table
    # get fields: user_id, first_name, last_name, gender, level
    
    print('####debugging: process_log_data, before sql query of users_table ')
    users_table = spark.sql("""
    SELECT DISTINCT 
        userid, 
        firstName,
        lastName, 
        gender, 
        level 
    FROM log_data """)
    print('####debugging: process_log_data, after sql query of users_table ')

    users_table.printSchema()


    # write users table to csv files
    print('####debugging: process_log_data, before writing csv users_table ')    
    out_path = "s3a://dend-bucket-ddatt/users_table_small.csv"
    users_table.write.save(out_path, format = 'csv', header = True)
    print('####debugging: process_log_data, after writing csv users_table ')    

        
    # write users table to parquet files
    print('####debugging: process_log_data, before writing parquet users_table ')
    users_table.write.partitionBy("userid").parquet(os.path.join(output_data,"users"), "overwrite")
    print('####debugging: process_log_data, after writing parquet users_table ')


    #----------------------------------------------------------------------------
    # create timestamp column from original timestamp column
#    get_timestamp = udf(lambda x : datetime.datetime.fromtimestamp(x / 1000.0).date)
#    get_timestamp = udf(lambda x : datetime.datetime(x/1000).strftime('YYYY-mm-dd %H:%M:%S'))
#    df = log_data.withColumn("ts" , get_timestamp(log_data.ts))
#    df = df.withColumn("timestamp" , get_timestamp(df.ts))

    print('####debugging: process_log_data, before timestamp extraction and log_data table creation ')

    df = df.withColumn("timestamp", from_unixtime(col("ts")/1000))
    df.createOrReplaceTempView("log_data")

    
    # extract columns to create time table
    # get fields: start_time, hour, day, week, month, year, weekday

    print('####debugging: process_log_data, before sql query of time_table ')
    time_table = spark.sql("""
    SELECT DISTINCT 
        ts as start_time,
        timestamp,
        cast(date_format(timestamp, "HH") as INTEGER) as hour,
        cast(date_format(timestamp, "dd") as INTEGER) as day,
        weekofyear(timestamp) as week, 
        month(timestamp) as month,
        year(timestamp) as year,
        dayofweek(timestamp) as weekday
    FROM log_data  """)
    print('####debugging: process_log_data, after sql query of time_table ')


    time_table.printSchema()
    
    # write time table to csv files
    print('####debugging: process_log_data, before writing csv time_table ')    
    out_path = "s3a://dend-bucket-ddatt/time_table_small.csv"
    time_table.write.save(out_path, format = 'csv', header = True)
    print('####debugging: process_log_data, after writing csv time_table ')


    # write time table to parquet files partitioned by year and month
    print('####debugging: process_log_data, before writing parquet time_table ')
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"time"), "overwrite")
    print('####debugging: process_log_data, after writing parquet time_table ')



    #----------------------------------------------------------------------------

    # extract columns from joined song and log datasets to create songplays table 
    # get fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    print('####debugging: process_log_data, before sql query of songplays_table ')
    songplays_table = spark.sql("""
    SELECT DISTINCT 
        ts as start_time,
        month(timestamp) as month,
        year(timestamp) as year,
        log_data.userId as user_id,
        log_data.level,
        song_data.song_id,
        song_data.artist_id,
        log_data.sessionId as session_id,
        log_data.location,
        log_data.userAgent as user_agent
    FROM log_data  
    JOIN song_data ON 
        log_data.artist = song_data.artist_name
        and log_data.song = song_data.title
        and log_data.length = song_data.duration
    """)
    print('####debugging: process_log_data, after sql query of songplays_table ')

    songplays_table.printSchema()
    
    '''
    The df_log schema as a reference:
    |-- artist: string (nullable = true)
    |-- auth: string (nullable = true)
    |-- firstName: string (nullable = true)
    |-- gender: string (nullable = true)
    |-- itemInSession: long (nullable = true) 
    |-- lastName: string (nullable = true)
    |-- length: double (nullable = true)
    |-- level: string (nullable = true)
    |-- location: string (nullable = true)
    |-- method: string (nullable = true)
    |-- page: string (nullable = true)
    |-- registration: double (nullable = true)
    |-- sessionId: long (nullable = true)
    |-- song: string (nullable = true)
    |-- status: long (nullable = true)
    |-- ts: long (nullable = true)
    |-- userAgent: string (nullable = true)
    |-- userId: string (nullable = true)

    
     The df_song schema as a reference:
     |-- artist_id: string (nullable = true)
     |-- artist_latitude: double (nullable = true)
     |-- artist_location: string (nullable = true)
     |-- artist_longitude: double (nullable = true)
     |-- artist_name: string (nullable = true)
     |-- duration: double (nullable = true)
     |-- num_songs: long (nullable = true)
     |-- song_id: string (nullable = true)
     |-- title: string (nullable = true)
     |-- year: long (nullable = true)
    '''    

    
    # write songplays table to csv files
    print('####debugging: process_log_data, before writing csv songplays_table ')    
    out_path = "s3a://dend-bucket-ddatt/songplays_table.csv"
    songplays_table.write.save(out_path, format = 'csv', header = True)
    print('####debugging: process_log_data, after writing csv songplays_table ')        

    
    # write songplays table to parquet files partitioned by year and month
    print('####debugging: process_log_data, before writing parquet songplays_table ')
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"songplays"), "overwrite")
    print('####debugging: process_log_data, after writing parquet songplays_table ')


    songplays_table.printSchema()

    #----------------------------------------------------------------------------



def main():
    create_env()
    spark = create_spark_session()
#    input_data = "s3a://udacity-dend/"        #full data set
    input_data = "s3a://dend-bucket-ddatt/"    #sample data set   
    output_data ="s3a://dend-bucket-ddatt/"
    
    print('####debugging: in main, before process_song_data')
    process_song_data(spark, input_data, output_data) 
    
    print('####debugging: in main, before process_log_data')
    process_log_data(spark, input_data, output_data)

    print('####debugging: in main, end of all  processes')
    
if __name__ == "__main__":
    main()