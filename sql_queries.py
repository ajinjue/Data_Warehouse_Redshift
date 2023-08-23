import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

print("Successful connection to the Cluster\n")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS log_data CASCADE;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_data CASCADE;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS log_data ( 
                                    artist TEXT,
                                    auth VARCHAR(MAX),
                                    firstName TEXT,
                                    gender CHAR(1),
                                    itemInSession int,
                                    lastName TEXT,
                                    length NUMERIC,
                                    level CHAR(4),
                                    location TEXT,
                                    method CHAR(3),
                                    page VARCHAR(MAX),
                                    registration bigint,
                                    session_id int,
                                    song TEXT,
                                    status smallint,
                                    ts bigint,
                                    userAgent TEXT,
                                    user_id int
                                    ); """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS song_data ( 
                                    num_songs int,
                                    artist_id VARCHAR(MAX),
                                    artist_latitude DOUBLE PRECISION,
                                    artist_longitude DOUBLE PRECISION,
                                    artist_location TEXT,
                                    artist_name TEXT,
                                    song_id VARCHAR(MAX),
                                    title TEXT,
                                    duration NUMERIC,
                                    year int
                                    ); """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                    user_id int GENERATED BY DEFAULT AS IDENTITY(0,1) PRIMARY KEY,
                                    first_name TEXT NOT NULL,
                                    last_name TEXT NOT NULL,
                                    gender CHAR(1) NOT NULL,
                                    level CHAR(4) NOT NULL
                                    ); """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                    song_id VARCHAR(MAX) PRIMARY KEY,
                                    title TEXT NOT NULL,
                                    artist_id VARCHAR(20) NOT NULL,
                                    year int NOT NULL,
                                    duration NUMERIC NOT NULL
                                    ); """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                    artist_id VARCHAR(MAX) PRIMARY KEY,
                                    name TEXT NOT NULL,
                                    location TEXT,
                                    latitude DOUBLE PRECISION,
                                    longitude DOUBLE PRECISION
                                    ); """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                    start_time TIMESTAMP PRIMARY KEY,
                                    hour smallint NOT NULL,
                                    day smallint NOT NULL,
                                    week smallint NOT NULL,
                                    month CHAR(3) NOT NULL,
                                    year int NOT NULL,
                                    weekday CHAR(3) NOT NULL
                                    ); """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                    songplay_id int IDENTITY(0,1) PRIMARY KEY,
                                    start_time bigint NOT NULL,
                                    user_id int NOT NULL,
                                    level CHAR(4) NOT NULL,
                                    song_id VARCHAR(MAX) NOT NULL,
                                    artist_id VARCHAR(MAX) NOT NULL,
                                    session_id int NOT NULL,
                                    location TEXT,
                                    user_agent TEXT
                                    ); """) 


# STAGING TABLES

DWH_ROLE_ARN = "arn:aws:iam::144316422473:role/dwhRole_akwa"

staging_songs_copy = ("""COPY song_data FROM 's3://udacity-dend/song_data/A'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS json 'auto ignorecase'
""").format(DWH_ROLE_ARN)

staging_events_copy = ("""COPY log_data FROM 's3://udacity-dend/log_data/2018/11/2018-11-'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    timeformat as 'epochmillisecs'
    FORMAT AS json 's3://udacity-dend/log_json_path.json'
""").format(DWH_ROLE_ARN)

# FINAL/ANALYTICS TABLES


songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                                SELECT ts, user_id, level, song_id, artist_id, session_id, artist_location, userAgent
                                FROM log_data l
                                INNER JOIN song_data s
                                ON s.title = l.song AND s.artist_name = l.artist
                                WHERE page = 'NextSong'; """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                                SELECT DISTINCT user_id, firstName, lastName, gender, level
                                FROM log_data
                                WHERE user_id IS NOT NULL; """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                                SELECT DISTINCT song_id, title, artist_id, year, duration
                                FROM song_data
                                WHERE song_id IS NOT NULL; """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                                SELECT 
                                    DISTINCT start_time, 
                                    DATE_PART('hour', start_time) AS hour,
                                    DATE_PART('day', start_time) AS day,
                                    DATE_PART('week', start_time) AS week,
                                    TO_CHAR(start_time, 'Mon') AS month,
                                    DATE_PART('year', start_time) AS year,
                                    TO_CHAR(start_time, 'Dy') AS weekday
                                FROM 
                                    (
                                        SELECT 
                                            TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' AS start_time
                                        FROM  songplays
                                        WHERE start_time IS NOT NULL
                                     ); """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                                SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                                FROM song_data
                                WHERE artist_id IS NOT NULL; """)


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, time_table_insert, artist_table_insert]

