# Project: Data Lake and Spark ETL on AWS

![N|Solid](https://github.com/devindatt/aws-spark-etl/blob/master/images/emr_python_spark_image.png)



## Introduction

Our task as a Data Engineer for the music company Sparkify is to design and code an ETL pipeline that extracts music data from an AWS S3 bucket, processes them using Spark, and loads the data back into S3 as a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. We'll test our database and ETL pipeline by running queries given by the Sparkify analytics team and compare your results with their expected results.

##### Steps To Complete:
1) Create a star schema optimized for queries on song play analysis
2) Load data from the S3 bucket into spark data frames
3) Execute Spark SQL statements that create the analytics tables
4) Write them back to S3 bucket in parquet format

Parquet is a columnar format that is supported by many other data processing systems. Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data. When writing Parquet files, all columns are automatically converted to be nullable for compatibility reasons.

###### Files & Links Used:

| File | Description |
| ------ | ------ |
| song_data | Song JSON data in S3 bucket 'udacity-dend'|
| log_data | Log JSON data in S3 bucket 'udacity-dend'|
| dwh.cfg | Configuration file for AWS resource access |
| sql_queries.py | Python script create, copy and insert SQL tables  |
| dl.cfg | File containing your AWS credentials|
| etl.py | Python script reads data from S3, processes that data using Spark, and writes them back to S3  |

### Amazon Web Services

To create the cluster with your credentials place your AWS key and secret into dwh.cfg in the following structure:
```
ADD YOUR CENDENTIALS:
AWS_KEY=<your aws key>
AWS_SECRET=<your aws secret>
```
These were the source and destination files and cloud storage resources used in this implementation: 
```
SOURCE & DESTINATIONS:
Input_data = s3a://udacity-dend/
Output_data = s3a://dend-bucket-ddatt/
```
```
SOURCE FILES:
Song data: s3a://udacity-dend/song_data
Log data: s3a://udacity-dend/log_data
```


### Running the ETL Process

Running the ETL process is divided in three parts:
1. Load raw files from the AWS resources
2. Use Spark SQL to create tables
3. Write back tables into cloud storage


For our data warehouse, one fact table and four dimension tables need to be created.

#### Fact Table: songplays
```
songplays_table = spark.sql("""
    SELECT DISTINCT 
        start_time,
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
    
songplays_table.write.partitionBy("start_time").parquet(os.path.join(output_data,"songplays"), "overwrite")    
```

#### Dimension Table: Users
```
users_table = spark.sql("""
    SELECT 
        userid, 
        firstName,
        lastName, 
        gender, 
        level 
    FROM log_data """)
    
users_table.write.partitionBy("userid").parquet(os.path.join(output_data,"users"), "overwrite")
```

##### Dimension Table: Songs
```
songs_table = spark.sql("""
    SELECT song_id,
        title, 
        artist_id, 
        year, 
        duration 
    FROM song_data """)

songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data,"songs"), "overwrite")
```
##### Dimension Table: Artists
```
artists_table = spark.sql("""
    SELECT artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude 
    FROM song_data """)
    
artists_table.write.partitionBy("artist_id").parquet(os.path.join(output_data,"artists"), "overwrite")    
```
##### Dimension Table: Time
```
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
    
time_table.write.partitionBy("timestamp").parquet(os.path.join(output_data,"time"), "overwrite")    
```

## Useful Analysis Output:
ETL Spark console output run:
![N|Solid](https://github.com/devindatt/aws-spark-etl/blob/master/images/qt_etl_console_output.gif)

ETL Notebook on AWS EMR Cluster output run:
![N|Solid](https://github.com/devindatt/aws-spark-etl/blob/master/images/emr_notebook_summary2.png)

AWS files after files writen back to the AWS bucket:
![N|Solid](https://github.com/devindatt/aws-spark-etl/blob/master/images/aws_writing_directory.png)



## Resources
- Spark [Parquet Documentation](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- The Log dataset are files in JSON format generated by [Event Simulator](https://github.com/Interana/eventsim)

