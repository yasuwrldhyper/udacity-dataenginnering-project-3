import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSON_PATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
REGION = config.get('GENERAL', 'REGION')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist             VARCHAR,
    auth               VARCHAR,
    firstName          VARCHAR,
    gender             VARCHAR,
    itemInSession      INTEGER,
    lastName           VARCHAR,
    length             FLOAT,
    level              VARCHAR,
    location           VARCHAR,
    method             VARCHAR,
    page               VARCHAR,
    registration       TIMESTAMP,
    sessionId          INTEGER,
    song               VARCHAR,
    status             INTEGER,
    ts                 TIMESTAMP,
    userAgent          VARCHAR,
    userId             VARCHAR
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1) NOT NULL,
    start_time  int NOT NULL sortkey,
    user_id     VARCHAR   NOT NULL,
    level       VARCHAR,
    song_id     VARCHAR distkey,
    artist_id   VARCHAR,
    session_id  int,
    location    VARCHAR,
    user_agent  VARCHAR,
    PRIMARY KEY (songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     VARCHAR NOT NULL sortkey,
    first_name  VARCHAR NOT NULL,
    last_name   VARCHAR NOT NULL,
    gender      VARCHAR,
    level       VARCHAR,
    PRIMARY KEY (user_id)
) diststyle all
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR NOT NULL sortkey distkey,
    title       VARCHAR NOT NULL,
    artist_id   VARCHAR NOT NULL,
    year        int,
    duration    NUMERIC NOT NULL,
    PRIMARY KEY (song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id       VARCHAR,
    name            VARCHAR NOT NULL,
    location        VARCHAR,
    latitude        FLOAT,
    longitude       FLOAT,
    PRIMARY KEY (artist_id)
) diststyle all
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  int NOT NULL,
    hour        int NOT NULL,
    day         int NOT NULL,
    week        int NOT NULL,
    month       int NOT NULL,
    year        int NOT NULL,
    weekday     int NOT NULL,
    PRIMARY KEY (start_time)
) diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    json {}
    credentials 'aws_iam_role={}'
    compupdate off region {}
    timeformat as 'epochmillisecs'
    truncatecolumns blanksasnull emptyasnull
""").format(S3_LOG_DATA, S3_LOG_JSON_PATH, DWH_ROLE_ARN, REGION)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    json 'auto'
    credentials 'aws_iam_role={}'
    compupdate off region {}
    truncatecolumns blanksasnull emptyasnull
""").format(S3_SONG_DATA, DWH_ROLE_ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
 SELECT
  TO_CHAR(se.ts, 'yyyyMMDDHH24')::integer as start_time,
  se.userId    as user_id,
  se.level,
  ss.song_id,
  ss.artist_id,
  se.sessionId as session_id,
  se.location,
  se.userAgent as user_agent
FROM staging_events se
LEFT OUTER JOIN staging_songs ss
ON  se.artist = ss.artist_name
AND se.song = ss.title
AND se.length = ss.duration
AND se.page = 'NextSong'
WHERE
 user_id    is not null AND
 start_time is not null
""")

user_table_insert = ("""
INSERT INTO users (
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
FROM staging_events
WHERE page = 'NextSong'
AND   userId NOT IN (SELECT DISTINCT user_id from users)
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
 SELECT DISTINCT
  song_id,
  title,
  artist_id,
  year,
  duration
FROM  staging_songs
WHERE song_id NOT IN (SELECT DISTINCT song_id from songs)
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
 SELECT DISTINCT
  artist_id,
  artist_name      as name,
  artist_location  as location,
  artist_latitude  as latitude,
  artist_longitude as latitude
FROM staging_songs
WHERE artist_id NOT IN (SELECT DISTINCT artist_id from artists)
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
 SELECT
  DISTINCT TO_CHAR(ts, 'yyyyMMDDHH24')::integer as start_time,
  EXTRACT(hour    FROM ts)      as hour,
  EXTRACT(day     FROM ts)      as day,
  EXTRACT(week    FROM ts)      as week,
  EXTRACT(month   FROM ts)      as month,
  EXTRACT(year    FROM ts)      as year,
  EXTRACT(weekday FROM ts)      as weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, staging_songs_table_create,
    songplay_table_create, user_table_create, song_table_create,
    artist_table_create, time_table_create
]
drop_table_queries = [
    staging_events_table_drop, staging_songs_table_drop,
    songplay_table_drop, user_table_drop, song_table_drop,
    artist_table_drop, time_table_drop]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert, user_table_insert,
    song_table_insert, artist_table_insert, time_table_insert]
