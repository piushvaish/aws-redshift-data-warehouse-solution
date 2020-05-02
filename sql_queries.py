import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES
# staging tables
staging_logs_table_drop = "DROP TABLE IF EXISTS staging_logs"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
#tables

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# get data from logs
staging_logs_table_create= ("""

                        CREATE TABLE IF NOT EXISTS staging_logs(
                        artist VARCHAR,
                        auth VARCHAR,
                        first_name VARCHAR,
                        gender CHAR(1),
                        item_session INTEGER,
                        last_name VARCHAR,
                        length DECIMAL,
                        level VARCHAR,
                        location VARCHAR,
                        method VARCHAR,
                        page VARCHAR,
                        registration DECIMAL,
                        session_id INTEGER,
                        song VARCHAR,
                        status INTEGER,
                        ts BIGINT,
                        user_agent VARCHAR,
                        user_id INTEGER
                        )
                            
""")

staging_songs_table_create = ("""

                        CREATE  TABLE IF NOT EXISTS staging_songs(
                        num_songs INTEGER,
                        artist_id VARCHAR,
                        artist_latitude DECIMAL,
                        artist_longitude DECIMAL,
                        artist_location VARCHAR,
                        artist_name VARCHAR,
                        song_id VARCHAR,
                        title VARCHAR,
                        duration DECIMAL,
                        year INTEGER
                        )
""")

# Create tables (with a distribution strategy) in the dist schema
songplay_table_create = ("""

                        CREATE TABLE IF NOT EXISTS songplay(
                        songplay_id INT IDENTITY(1,1) PRIMARY KEY sortkey distkey,
                        start_time TIMESTAMP,
                        user_id INTEGER,
                        level VARCHAR,
                        song_id VARCHAR,
                        artist_id VARCHAR,
                        session_id INTEGER,
                        location VARCHAR,
                        user_agent VARCHAR
                        
                        )
""")

user_table_create = ("""
                        CREATE TABLE IF NOT EXISTS users(
                        user_id INTEGER PRIMARY KEY NOT NULL sortkey,
                        first_name VARCHAR NOT NULL,
                        last_name VARCHAR NOT NULL,
                        gender CHAR(1),
                        level VARCHAR
                        )
""")

song_table_create = ("""

                        CREATE TABLE IF NOT EXISTS songs(
                        song_id VARCHAR PRIMARY KEY NOT NULL sortkey,
                        title VARCHAR,
                        artist_id VARCHAR NOT NULL,
                        year INTEGER,
                        duration DECIMAL
                    )
""")

artist_table_create = ("""

                        CREATE TABLE IF NOT EXISTS artists(
                        artist_id VARCHAR PRIMARY KEY NOT NULL sortkey,
                        name VARCHAR,
                        location VARCHAR,
                        latitude DECIMAL,
                        longitude DECIMAL
                       )
""")

time_table_create = ("""

                        CREATE TABLE IF NOT EXISTS time(
                        start_time TIMESTAMP PRIMARY KEY NOT NULL sortkey,
                        hour INTEGER,
                        day INTEGER,
                        week INTEGER,
                        month INTEGER,
                        year INTEGER,
                        weekDay INTEGER
                    )
""")

# STAGING TABLES
staging_logs_copy = ("""
                          copy staging_logs 
                          from {}
                          iam_role {}
                          json {};
                       """).format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
                          copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto';
                      """).format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

## https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift

songplay_table_insert = ("""INSERT INTO songplay(
                            start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
                            )
                            SELECT  
                            timestamp 'epoch' + l.ts/1000 * interval '1 second' as start_time,
                            l.user_id, l.level, s.song_id, s.artist_id, l.session_id, l.location, l.user_agent
                            FROM staging_logs l, staging_songs s
                            LIMIT 100
                         
""")

user_table_insert = ("""INSERT INTO users(
                        user_id, first_name, last_name, gender, level
                        )
                        SELECT DISTINCT  
                        user_id, first_name, last_name, gender, level
                        FROM staging_logs
                        WHERE user_id IS NOT NULL
                        LIMIT 100
""")

song_table_insert = ("""INSERT INTO songs(
                        song_id, title, artist_id, year, duration
                        )
                        SELECT song_id, title, artist_id, year, duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
                        LIMIT 100
""")

artist_table_insert = ("""INSERT INTO artists(
                          artist_id, name, location, latitude, longitude
                          )
                          SELECT DISTINCT 
                          artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL
                          LIMIT 100
""")
# https://docs.aws.amazon.com/redshift/latest/dg/r_Dateparts_for_datetime_functions.html
time_table_insert = ("""INSERT INTO time(
                        start_time, hour, day, week, month, year, weekDay
                        )
                        SELECT 
                        start_time, 
                        extract(h from start_time), 
                        extract(d from start_time),
                        extract(w from start_time), 
                        extract(mon from start_time),
                        extract(y from start_time), 
                        extract(dow from start_time)
                        FROM songplay
                        LIMIT 100
""")

# QUERY LISTS

create_table_queries = [staging_logs_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_logs_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_logs_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]