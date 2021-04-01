import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "drop table if exists users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR, 
    gender VARCHAR,
    itemInSession INTEGER, 
    lastName VARCHAR, 
    length REAL, 
    level VARCHAR,
    location VARCHAR,
    method VARCHAR, 
    page VARCHAR, 
    registration REAL, 
    sessionId INTEGER,
    song VARCHAR, 
    status INTEGER, 
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER, 
    artist_id VARCHAR,
    artist_latitude REAL, 
    artist_longitude REAL, 
    artist_location VARCHAR, 
    artist_name VARCHAR,
    song_id VARCHAR , 
    title VARCHAR, 
    duration REAL, 
    year INTEGER );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INTEGER IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL, 
    user_id INTEGER NOT NULL, 
    level VARCHAR NOT NULL, 
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL, 
    session_id INTEGER NOT NULL, 
    location VARCHAR, 
    user_agent VARCHAR );
""")

user_table_create = (""" 
    CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER NOT NULL PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL, 
    gender VARCHAR NOT NULL, 
    level VARCHAR NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
    song_id VARCHAR NOT NULL UNIQUE, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INTEGER NOT NULL, 
    duration REAL NOT NULL,
    PRIMARY KEY(song_id, artist_id));
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist(
    artist_id VARCHAR NOT NULL PRIMARY KEY, 
    name VARCHAR NOT NULL, 
    location VARCHAR,
    lattitude REAL,
    longitude REAL
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL, 
    week_day INTEGER NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_songs
FROM 's3://udacity-dend/song_data'
iam_role {}
json 'auto ignorecase';
""").format(config.get('IAM_ROLE','ARN'))

staging_songs_copy = ("""
COPY staging_events
FROM 's3://udacity-dend/log_data'
iam_role {}
json 'auto ignorecase';
""").format(config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
  INSERT INTO songplay (start_time, user_id, level, song_id,artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + se.ts * interval '0.001 seconds',
                        se.userId, se.level, ss.song_id, ss.artist_id,
                        se.sessionId, se.location, se.userAgent
    FROM staging_events AS se LEFT JOIN staging_songs AS ss ON se.song = ss.title
    WHERE se.page = 'NextSong' AND ss.song_id IS NOT NULL ;
""")

user_table_insert = ("""
    INSERT INTO users 
    (SELECT distinct se.userId, se.firstName, se.lastName, se.gender, se.level
    FROM staging_events AS se 
     WHERE userId IS NOT NULL
    )
""")

song_table_insert = ("""
    INSERT INTO song 
    (SELECT distinct ss.song_id, ss.title, ss.artist_id, ss.year,ss.duration
    FROM staging_songs AS ss WHERE ss.song_id IS NOT NULL)
""")

artist_table_insert = ("""
    INSERT INTO artist
    (SELECT distinct ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
    FROM staging_songs AS ss WHERE ss.artist_id IS NOT NULL)
""")

time_table_insert = ("""
    INSERT INTO time
    (SELECT timestamp 'epoch' + se.ts * interval '0.001 seconds',
            date_part(h, timestamp 'epoch' + se.ts * interval '0.001 seconds'),
            date_part(w, timestamp 'epoch' + se.ts * interval '0.001 seconds'),
            date_part(mon, timestamp 'epoch' + se.ts * interval '0.001 seconds'),
            date_part(y, timestamp 'epoch' + se.ts * interval '0.001 seconds'),
            date_part(dw, timestamp 'epoch' + se.ts * interval '0.001 seconds')
    FROM staging_events AS se
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
