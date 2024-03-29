import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagingevents"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingsongs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS stagingevents (
    event_id    BIGINT IDENTITY(0,1)    PRIMARY KEY,
    artist      VARCHAR(255)            NULL,
    auth        VARCHAR(25)             NULL,
    first_name   VARCHAR(255)           NULL,
    gender      VARCHAR(1)              NULL,
    item_in_session INTEGER             NULL,
    last_name    VARCHAR(255)           NULL,
    length      FLOAT                   NULL,
    level       VARCHAR(15)             NULL,
    location    VARCHAR(255)            NULL,
    method      VARCHAR(5)              NULL,
    page        VARCHAR(25)             NULL,
    registration VARCHAR(255)           NULL,
    session_id   VARCHAR(30)            NOT NULL,
    song        VARCHAR(255)            NULL,
    status      INTEGER                 NULL,
    ts          BIGINT                  NOT NULL,
    user_agent   VARCHAR(255)           NULL,
    user_id      INTEGER                NULL);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stagingsongs (
    num_songs           INTEGER         NULL,
    artist_id           VARCHAR(50)     NOT NULL,
    artist_latitude     DECIMAL(10)     NULL,
    artist_longitude    DECIMAL(10)     NULL,
    artist_location     VARCHAR(255)    NULL,
    artist_name         VARCHAR(255)    NULL,
    song_id             VARCHAR(30)     PRIMARY KEY,
    title               VARCHAR(255)    NULL,
    duration            DECIMAL(10)     NULL,
    year                INTEGER         NULL);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id  BIGINT IDENTITY(0,1)  PRIMARY KEY,
    start_time   TIMESTAMP             NOT NULL,
    user_id      VARCHAR(30)           NOT NULL,
    level        VARCHAR(15)           NOT NULL,
    song_id      VARCHAR(30)           NOT NULL    SORTKEY DISTKEY,
    artist_id    VARCHAR(30)           NULL,
    session_id   VARCHAR(30)           NOT NULL,
    location     VARCHAR(255)          NOT NULL,
    user_agent   VARCHAR(255)          NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id      VARCHAR(30)          PRIMARY KEY    SORTKEY,
    first_name   VARCHAR(255)         NOT NULL,
    last_name    VARCHAR(255)         NOT NULL,
    gender       VARCHAR(1)           NULL,
    level        VARCHAR(15)          NOT NULL
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id      VARCHAR(30)          PRIMARY KEY    SORTKEY DISTKEY,
    title        VARCHAR(255)         NOT NULL,
    artist_id    VARCHAR(30)          NOT NULL,
    year         INTEGER              NOT NULL,
    duration     FLOAT                NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id    VARCHAR(30)          PRIMARY KEY    SORTKEY,
    name         VARCHAR(255)          NOT NULL,
    location     VARCHAR(255)         NULL,
    latitude     DECIMAL(10)          NULL,
    longitude    DECIMAL(10)          NULL
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP             PRIMARY KEY    SORTKEY,
    hour        SMALLINT              NOT NULL,
    day         SMALLINT              NOT NULL,
    week        SMALLINT              NOT NULL,
    month       SMALLINT              NOT NULL,
    year        SMALLINT              NOT NULL,
    weekday     SMALLINT              NOT NULL
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stagingevents from {} 
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json {}
    compupdate off statupdate off;
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH'),
)

staging_songs_copy = ("""
    copy stagingsongs from {} 
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto' 
    compupdate off statupdate off;
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
)

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
    user_agent)
SELECT DISTINCT
    timestamp 'epoch' + se.ts * interval '1 second' AS start_time,
    se.user_id AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.session_id AS session_id,
    se.location AS location,
    se.user_agent AS user_agent
FROM stagingevents AS se
JOIN stagingsongs AS ss
ON se.artist = ss.artist_name
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level)
SELECT  DISTINCT 
    se.user_id AS user_id,
    se.first_name AS first_name,
    se.last_name AS last_name,
    se.gender AS gender,
    se.level AS level
FROM stagingevents AS se
WHERE se.page = 'NextSong';""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT  DISTINCT 
    ss.song_id AS song_id,
    ss.title AS title,
    ss.artist_id AS artist_id,
    ss.year AS year,
    ss.duration AS duration
FROM stagingsongs AS ss;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
SELECT  DISTINCT 
    ss.artist_id AS artist_id,
    ss.artist_name AS name,
    ss.artist_location AS location,
    ss.artist_latitude AS latitude,
    ss.artist_longitude AS longitude
FROM stagingsongs AS ss;
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
SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'        AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(week FROM start_time) AS weekday
FROM stagingevents AS se
WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
