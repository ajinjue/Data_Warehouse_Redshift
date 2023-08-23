# Data_Warehouse_with_Redshift

## Project Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ELT pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for our analytics team to continue finding insights into what songs our users are listening to.
![image](https://github.com/ajinjue/Data_Warehouse_Redshift/assets/100845693/fcc71d48-24bb-48ad-b2e9-d54bf4980d6f)

## Project Datasets
I worked with 3 datasets that reside in S3. Here are the S3 links for each:

1. Song data: s3://udacity-dend/song_data
2. Log data: s3://udacity-dend/log_data    
3. This third file s3://udacity-dend/log_json_path.json contains the meta information that is required by AWS to correctly load s3://udacity-dend/log_data

### Song Data
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset:
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

### Log Data
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset I worked with are partitioned by year and month. For example, here are file paths to two files in this dataset:
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

## Project Files
The project includes 7 files:
1. create_table.py: This is where I created my fact and dimension tables for the star schema in Redshift.
2. etl.py: This is where I loaded data from S3 into staging tables on Redshift and then process that data into our analytics tables on Redshift.
3. sql_queries.py: This is where I defined my SQL statements, which was imported into the two other files above.
4. README.md: This is where I provided discussion on my process and decisions for this ELT pipeline.
5. cluster_creation.ipynb: This is a jupyter notebook file where I showed how to create, connect to and delete a Redshift cluster programmatically.
6. executions_file.ipynb This is also a jupyter notebook file where I executed commands to run the files above.
7. dwh.cfg: This is where the parameters of the cluster were stored.

## Database Schema

### Staging Tables
#### i. Log data:
artist (text), auth (varchar), firstName (text), gender (char(1)), itemInSession (int), lastName (text), length (numeric), level (char(4)), location (text), method (char(3)), page (varchar), registration (bigint), session_id (int), song (text), status (smallint), ts (bigint), userAgent (text), user_id (int)
#### ii. Song data:
num_songs (int), artist_id (varchar), artist_latitude (double precision), artist_longitude (double precision), artist_location (text), artist_name (text), song_id (varchar), title (text), duration (numeric), year (int)

### Analytics Tables
#### Fact Table
**Songplays**:- records in event (or log) data associated with song plays i.e records with page "NextSong":
songplay_id (Identity(0,1) PK), start_time (timestamp), user_id (int), level (char(4)), song_id (varchar), artist_id (varchar), location (text), user_agent (text)

#### Dimension Tables
**Users**:- users in the app:
user_id (int), firstname (text), last_name (text), gender (char(1)), level (char(4))

**Songs**:- songs in the music database:
song_id (varchar), title (text), artist_id (varchar), year (int), duration (numeric)

**Artists**:- artists in the music database
artist_id (varchar), name (text), location (text), lattitude (double precision), longitude (double precision)

**Time**:- timestamps of records in Songplays broken down into specific units
start_time (timestamp), hour (smallint), day (smallint), week (smallint), month (text), year (int), weekday (text)
![image](https://github.com/ajinjue/Data_Warehouse_Redshift/assets/100845693/7a52b4d6-21b1-4872-b58e-09e1e8dbf565)


## ELT Pipeline and how to run scripts
1. Create Amazon Redshift Cluster and connect to it
2. Drop all existing staging and analytics tables
3. Create staging and analytics tables
4. Copy or load data from S3 to staging tables on Amazon Redshift cluster
5. Insert data from staging tables into analytics tables

In the executions_file.ipynb file, executing:
    **%run create_tables.py**,  <br/> and then
    **%run etl.py** got the job done.

The overall architecture below shows the flow of data:
![image](https://github.com/ajinjue/Data_Warehouse_Redshift/assets/100845693/53489176-9ff5-4ff5-b31e-ea40da709007)









![image](https://github.com/ajinjue/Data_Warehouse_Redshift/assets/100845693/cd73bfce-e5f5-40da-b259-46b413d82f0f)









