# Data Warehouse (Redshift)

## Project
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Here an ETL pipeline is built that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables, so that analytics team can continue finding insights in what songs their users are listening to.

## Data
The data is distributed over two datasets that reside in S3. Here are the S3 links for each:

Song data: `s3://udacity-dend/song_data`
Log data: `s3://udacity-dend/log_data`

Log data json path: `s3://udacity-dend/log_json_path.json`

## How to spin up resources
Run SpinupResources.ipynb notebook cells. Remember to set your aws key and secret key first in the `aws.env` file.

## How to set up the database
Run `python create_tables.py` to create the Redshift tables. Then run `python etl.py` to load the data into Redshift. Be aware that running the ETL can take few minutes.

## Database explanation
### Staging Tables
Staging tables are created for a fast copy from S3 to Redshift. Therefore, two staging tables are created (`stagingevents` and `stagingsongs`).

```python
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
    user_id      INTEGER                NULL
);

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
```

### Tables for analytics
For this project a star schema is created to optimize for on song play analysis. The benefits of using star schema is for instance simpler queries and good query performance. Both of which are preferrable here. The database consists of one fact table and four dimensional tables.

**Fact Table**
The `songplays` fact table is distributed by `song_id` and sorted by the same key. This is done to speed up aggregations of songs (the song table is quite big).

``` Python
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
```

**Dimensional Tables**
The `users` dimensional table is distributed by all and sorted by the `user_id`. `Diststyle all` is used because the users table is relatively small so all copies of the artist table can be stored on all nodes.

``` Python
CREATE TABLE IF NOT EXISTS users (
    user_id      VARCHAR(30)          PRIMARY KEY    SORTKEY,
    first_name   VARCHAR(255)         NOT NULL,
    last_name    VARCHAR(255)         NOT NULL,
    gender       VARCHAR(1)           NULL,
    level        VARCHAR(15)          NOT NULL
) diststyle all;
```

The `artist` dimensional table is distributed by all and sorted by the `artist_id`. `Diststyle all` is used because the users table is relatively small so all copies of the artist table can be stored on all nodes.

``` Python
CREATE TABLE IF NOT EXISTS artists (
    artist_id    VARCHAR(30)          PRIMARY KEY    SORTKEY,
    name         VARCHAR(255)          NOT NULL,
    location     VARCHAR(255)         NULL,
    latitude     DECIMAL(10)          NULL,
    longitude    DECIMAL(10)          NULL
) diststyle all;
```

The `time` dimensional table is distributed by all and sorted by the `artist_id`. `Diststyle all` is used because the users table is relatively small so all copies of the artist table can be stored on all nodes.

``` Python
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP             PRIMARY KEY    SORTKEY,
    hour        SMALLINT              NOT NULL,
    day         SMALLINT              NOT NULL,
    week        SMALLINT              NOT NULL,
    month       SMALLINT              NOT NULL,
    year        SMALLINT              NOT NULL,
    weekday     SMALLINT              NOT NULL
) diststyle all;
```


The `song` dimensional table has a DISTKEY on `song_id` and a sort key on the same song. his is done to speed up aggregations of songs, but the song table is too big to be stored on every node, therefore, it was decided to use DISTKEY instead on `song_id`.

``` Python
CREATE TABLE IF NOT EXISTS songs (
    song_id      VARCHAR(30)          PRIMARY KEY    SORTKEY DISTKEY,
    title        VARCHAR(255)         NOT NULL,
    artist_id    VARCHAR(30)          NOT NULL,
    year         INTEGER              NOT NULL,
    duration     FLOAT                NOT NULL
)
```