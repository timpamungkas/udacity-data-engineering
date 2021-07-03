import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time_tables"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration numeric,
    sessionId bigint not null distkey sortkey,
    song varchar,
    status int,
    ts numeric,
    userAgent varchar,
    userId varchar
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
   artist_id varchar,
   artist_location varchar,
   artist_latitude numeric,
   artist_longitude numeric,
   artist_name varchar,
   duration numeric,
   num_songs int,
   song_id varchar,
   title varchar,
   year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
  songplay_id bigint identity(0,1), 
  start_time bigint not null,
  user_id varchar not null distkey sortkey, 
  level varchar not null, 
  song_id varchar,
  artist_id varchar, 
  session_id bigint not null, 
  location varchar not null, 
  user_agent varchar not null
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
  user_id varchar distkey sortkey,
  first_name varchar not null,
  last_name varchar, 
  gender varchar, 
  level varchar not null
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
  song_id varchar distkey sortkey, 
  title varchar, 
  artist_id varchar, 
  year int, 
  duration numeric
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
  artist_id varchar distkey sortkey, 
  name varchar, 
  location varchar, 
  latitude numeric, 
  longitude numeric
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
  time_id bigint identity(0,1) distkey sortkey,
  start_time timestamp,
  hour int, 
  day int, 
  week int, 
  month int, 
  year int, 
  weekday int
)
""")

# STAGING TABLES
staging_events_copy = ("""
    copy staging_events 
    from {}
    credentials 'aws_iam_role={}' 
    json {}
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs 
    from {} 
    credentials 'aws_iam_role={}' 
    json 'auto';
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','LOG_JSONPATH'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(
  start_time, 
  user_id, 
  level, 
  song_id, 
  artist_id, 
  session_id, 
  location, 
  user_agent)
SELECT DISTINCT 
  se.ts,
  se.userId,
  se.level,
  ss.song_id,
  ss.artist_id,
  se.sessionId,
  se.location,
  se.userAgent
FROM staging_events se
JOIN staging_songs ss
ON se.artist = ss.artist_name
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(
  user_id, 
  first_name, 
  last_name, 
  gender, 
  level)
SELECT DISTINCT 
  userId, 
  firstName, 
  lastName, 
  gender, 
  level 
FROM staging_events se
WHERE se.page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (
  song_id, 
  title, 
  artist_id, 
  year, 
  duration)
SELECT DISTINCT
  song_id,
  title,
  artist_id,
  year,
  duration
FROM staging_songs ss
""")

artist_table_insert = ("""
INSERT INTO artists (
  artist_id,
  name,
  location,
  latitude,
  longitude)
SELECT DISTINCT 
  artist_id, 
  artist_name,
  artist_location,
  artist_latitude,
  artist_longitude
FROM staging_songs ss
""")

time_table_insert = ("""
INSERT INTO time (
  start_time,
  hour,
  day,
  week,
  month,
  year,
  weekday)
SELECT DISTINCT 
  se.ts as start_time,
  EXTRACT(hour FROM start_time),
  EXTRACT(day FROM start_time),
  EXTRACT(week FROM start_time),
  EXTRACT(month FROM start_time),
  EXTRACT(year FROM start_time),
  EXTRACT(week FROM start_time)
FROM staging_events se
WHERE se.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]