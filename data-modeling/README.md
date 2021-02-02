## Data Modeling (Postgres)

### Project
A PostgreSQL setup of a database representing songs and user activity from a made up startup called Sparkify. The goal of this project is to create a Postgres database with tables designed to optimize queries on song play analysis.

### Data
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

`song_data/A/B/C/TRABCEI128F424C983.json`
`song_data/A/A/B/TRAABJL12903CDCF1A.json`
And below is an example of what a single song file, `TRAABJL12903CDCF1A.json`, looks like.
```json
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud", 
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff", 
    "duration": 152.92036, 
    "year": 0}
```

The second dataset consists of log files in JSON format generated. The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

`log_data/2018/11/2018-11-12-events.json`
`log_data/2018/11/2018-11-13-events.json`
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![]("assets/log-data.png")

### Schema

![]("asset/schema.png")

To visualize the schema, [eralchemy](https://github.com/Alexis-benoist/eralchemy) was used. Which uses [Graphviz-dev](https://graphviz.org/download/) under the hood .

### How to Run

First create the database and all the tables with,

```bash
python create_tables.py
```

then run

```bash
python etl.py
```

to load the Sparkify data into the database.

### Repo structure
In this directory are the following files
1. `test.ipynb` displays the first few rows of each table to let you check your database.
2. `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts. (`python create_tables.py`)
3. `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. `etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook. (`python etl.py`)
5. `sql_queries.py` contains all your sql queries, and is imported into the last three files above. (`python sql_queries.py`)
6. `DrawDiagrams.ipynb` visualizes the database schema. (`python sql_queries.py`)


